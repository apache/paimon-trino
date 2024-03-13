/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.trino.catalog.TrinoCatalog;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.TupleDomainOrcPredicate;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.schema.SchemaEvolutionUtil.createIndexMapping;
import static org.apache.paimon.trino.ClassLoaderUtils.runWithContextClassLoader;

/** Trino {@link ConnectorPageSourceProvider}. */
public class TrinoPageSourceProvider implements ConnectorPageSourceProvider {

    private final TrinoFileSystemFactory fileSystemFactory;
    private final TrinoCatalog trinoCatalog;

    @Inject
    public TrinoPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory, TrinoMetadataFactory trinoMetadataFactory) {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.trinoCatalog =
                requireNonNull(trinoMetadataFactory, "trinoMetadataFactory is null")
                        .create()
                        .catalog();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        trinoCatalog.initSession(session);
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Table table = trinoTableHandle.tableWithDynamicOptions(trinoCatalog, session);
        return runWithContextClassLoader(
                () ->
                        createPageSource(
                                session,
                                table,
                                trinoTableHandle.getFilter(),
                                (TrinoSplit) split,
                                columns,
                                trinoTableHandle.getLimit()),
                TrinoPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            Table table,
            TupleDomain<TrinoColumnHandle> filter,
            TrinoSplit split,
            List<ColumnHandle> columns,
            OptionalLong limit) {
        RowType rowType = table.rowType();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        List<String> projectedFields =
                columns.stream()
                        .map(TrinoColumnHandle.class::cast)
                        .map(TrinoColumnHandle::getColumnName)
                        .toList();
        int[] columnIndex =
                // the column index, very important
                projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(optionalRawFiles)) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                SchemaManager schemaManager =
                        new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                List<Type> type =
                        columns.stream()
                                .map(s -> ((TrinoColumnHandle) s).getTrinoType())
                                .collect(Collectors.toList());
                try {
                    return new DirectTrinoPageSource(
                            optionalRawFiles.orElseThrow().stream()
                                    .map(
                                            rawFile ->
                                                    createDataPageSource(
                                                            rawFile.format(),
                                                            fileSystem.newInputFile(
                                                                    Location.of(rawFile.path())),
                                                            fileStoreTable.coreOptions(),
                                                            // map table column index to data column
                                                            // index, if column does not exist in
                                                            // data columns, set it to -1
                                                            // columns those set to -1 will generate
                                                            // a null vector in orc page
                                                            mapping(
                                                                    columnIndex,
                                                                    rowType.getFields(),
                                                                    schemaManager
                                                                            .schema(
                                                                                    rawFile
                                                                                            .schemaId())
                                                                            .fields()),
                                                            type,
                                                            orderDomains(projectedFields, filter)))
                                    .collect(
                                            Collector.of(
                                                    LinkedList::new,
                                                    List::add,
                                                    (left, right) -> {
                                                        left.addAll(right);
                                                        return left;
                                                    })));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                // old read way
                ReadBuilder read = table.newReadBuilder();
                new TrinoFilterConverter(rowType).convert(filter).ifPresent(read::withFilter);

                if (!fieldNames.equals(projectedFields)) {
                    read.withProjection(columnIndex);
                }

                return new TrinoPageSource(
                        read.newRead().executeFilter().createReader(paimonSplit), columns, limit);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // make domains(filters) to be ordered by projected fields' order.
    private List<Domain> orderDomains(
            List<String> projectedFields, TupleDomain<TrinoColumnHandle> filter) {
        Optional<Map<TrinoColumnHandle, Domain>> optionalFilter = filter.getDomains();
        Map<String, Domain> domainMap = new HashMap<>();
        optionalFilter.ifPresent(
                trinoColumnHandleDomainMap ->
                        trinoColumnHandleDomainMap.forEach(
                                (k, v) -> domainMap.put(k.getColumnName(), v)));

        return projectedFields.stream()
                .map(name -> domainMap.getOrDefault(name, null))
                .collect(Collectors.toList());
    }

    private boolean checkRawFile(Optional<List<RawFile>> optionalRawFiles) {
        return optionalRawFiles.isPresent() && checkFormat(optionalRawFiles.get());
    }

    // only support orc yet.
    // todo: support parquet and avro
    private boolean checkFormat(List<RawFile> rawFiles) {
        for (RawFile rawFile : rawFiles) {
            if (!rawFile.format().equals("orc")) {
                return false;
            }
        }
        return true;
    }

    // map the table schema columnsIndex to data schema columnsIndex
    private int[] mapping(
            int[] tableSchemaColumnIndex, List<DataField> tableFields, List<DataField> dataFields) {

        int[] mapping = createIndexMapping(tableFields, dataFields);
        if (mapping == null) {
            return tableSchemaColumnIndex;
        }
        int[] result = new int[tableSchemaColumnIndex.length];

        for (int i = 0; i < tableSchemaColumnIndex.length; i++) {
            int po = tableSchemaColumnIndex[i];
            result[i] = mapping[po];
        }
        return result;
    }

    private ConnectorPageSource createDataPageSource(
            String format,
            TrinoInputFile inputFile,
            // todo construct read option by core-options
            CoreOptions coreOptions,
            int[] columns,
            List<Type> types,
            List<Domain> domains) {
        switch (format) {
            case "orc":
                {
                    return createOrcDataPageSource(
                            inputFile,
                            new OrcReaderOptions()
                                    .withTinyStripeThreshold(
                                            DataSize.of(4, DataSize.Unit.KILOBYTE)),
                            columns,
                            types,
                            domains);
                }
            case "parquet":
                {
                    // todo
                    throw new RuntimeException("Unsupport file format: " + format);
                }
            case "avro":
                {
                    // todo
                    throw new RuntimeException("Unsupport file format: " + format);
                }
            default:
                {
                    throw new RuntimeException("Unsupport file format: " + format);
                }
        }
    }

    private ConnectorPageSource createOrcDataPageSource(
            TrinoInputFile inputFile,
            OrcReaderOptions options,
            int[] columns,
            List<Type> types,
            List<Domain> domains) {
        try {
            OrcDataSource orcDataSource = new TrinoOrcDataSource(inputFile, options);
            OrcReader reader =
                    OrcReader.createOrcReader(orcDataSource, options)
                            .orElseThrow(() -> new RuntimeException("ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder =
                    TupleDomainOrcPredicate.builder();
            List<OrcPageSource.ColumnAdaptation> columnAdaptations = new ArrayList<>();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.length);
            List<Type> fileReadTypes = new ArrayList<>(columns.length);

            for (int i = 0; i < columns.length; i++) {
                if (columns[i] >= 0) {
                    // column exists
                    columnAdaptations.add(
                            OrcPageSource.ColumnAdaptation.sourceColumn(fileReadColumns.size()));
                    fileReadColumns.add(fileColumns.get(columns[i]));
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(
                                fileColumns.get(columns[i]).getColumnId(), domains.get(i));
                    }
                } else {
                    columnAdaptations.add(OrcPageSource.ColumnAdaptation.nullColumn(types.get(i)));
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcRecordReader recordReader =
                    reader.createRecordReader(
                            fileReadColumns,
                            fileReadTypes,
                            predicateBuilder.build(),
                            DateTimeZone.UTC,
                            memoryUsage,
                            INITIAL_BATCH_SIZE,
                            RuntimeException::new);

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    Optional.empty(),
                    Optional.empty(),
                    memoryUsage,
                    new FileFormatDataSourceStats(),
                    reader.getCompressionKind());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
