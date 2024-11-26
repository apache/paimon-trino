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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IndexFile;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static java.util.Objects.requireNonNull;
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
                () -> {
                    Optional<TrinoColumnHandle> rowId =
                            columns.stream()
                                    .map(TrinoColumnHandle.class::cast)
                                    .filter(column -> column.isRowId())
                                    .findFirst();
                    if (rowId.isPresent()) {
                        List<ColumnHandle> dataColumns =
                                columns.stream()
                                        .map(TrinoColumnHandle.class::cast)
                                        .filter(column -> !column.isRowId())
                                        .collect(Collectors.toList());
                        Set<String> rowIdFileds =
                                ((io.trino.spi.type.RowType) rowId.get().getTrinoType())
                                        .getFields().stream()
                                                .map(io.trino.spi.type.RowType.Field::getName)
                                                .map(Optional::get)
                                                .collect(Collectors.toSet());

                        HashMap<String, Integer> fieldToIndex = new HashMap<>();
                        for (int i = 0; i < dataColumns.size(); i++) {
                            TrinoColumnHandle trinoColumnHandle =
                                    (TrinoColumnHandle) dataColumns.get(i);
                            if (rowIdFileds.contains(trinoColumnHandle.getColumnName())) {
                                fieldToIndex.put(trinoColumnHandle.getColumnName(), i);
                            }
                        }
                        return TrinoMergePageSourceWrapper.wrap(
                                createPageSource(
                                        session,
                                        table,
                                        trinoTableHandle.getFilter(),
                                        (TrinoSplit) split,
                                        dataColumns,
                                        trinoTableHandle.getLimit()),
                                fieldToIndex);
                    } else {
                        return createPageSource(
                                session,
                                table,
                                trinoTableHandle.getFilter(),
                                (TrinoSplit) split,
                                columns,
                                trinoTableHandle.getLimit());
                    }
                },
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
        List<String> fieldNames = rowType.getFieldNames();
        List<String> projectedFields =
                columns.stream()
                        .map(TrinoColumnHandle.class::cast)
                        .map(TrinoColumnHandle::getColumnName)
                        .toList();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        Optional<Predicate> paimonFilter = new TrinoFilterConverter(rowType).convert(filter);

        try {
            Split paimonSplit = split.decodeSplit();
            Optional<List<RawFile>> optionalRawFiles = paimonSplit.convertToRawFiles();
            if (checkRawFile(optionalRawFiles)) {
                FileStoreTable fileStoreTable = (FileStoreTable) table;
                boolean readIndex = fileStoreTable.coreOptions().fileIndexReadEnabled();

                Optional<List<DeletionFile>> deletionFiles = paimonSplit.deletionFiles();
                Optional<List<IndexFile>> indexFiles =
                        readIndex ? paimonSplit.indexFiles() : Optional.empty();
                SchemaManager schemaManager =
                        new SchemaManager(fileStoreTable.fileIO(), fileStoreTable.location());
                List<Type> type =
                        columns.stream()
                                .map(s -> ((TrinoColumnHandle) s).getTrinoType())
                                .collect(Collectors.toList());

                try {
                    List<RawFile> files = optionalRawFiles.orElseThrow();
                    LinkedList<ConnectorPageSource> sources = new LinkedList<>();

                    // if file index exists, do the filter.
                    for (int i = 0; i < files.size(); i++) {
                        RawFile rawFile = files.get(i);
                        if (indexFiles.isPresent()) {
                            IndexFile indexFile = indexFiles.get().get(i);
                            if (indexFile != null && paimonFilter.isPresent()) {
                                try (FileIndexPredicate fileIndexPredicate =
                                        new FileIndexPredicate(
                                                new Path(indexFile.path()),
                                                ((FileStoreTable) table).fileIO(),
                                                rowType)) {
                                    if (!fileIndexPredicate.evaluate(paimonFilter.get()).remain()) {
                                        continue;
                                    }
                                }
                            }
                        }
                        ConnectorPageSource source =
                                createDataPageSource(
                                        rawFile.format(),
                                        fileSystem.newInputFile(Location.of(rawFile.path())),
                                        fileStoreTable.coreOptions(),
                                        // map table column name to data column
                                        // name, if column does not exist in
                                        // data columns, set it to null
                                        // columns those set to null will generate
                                        // a null vector in orc page
                                        fileStoreTable.schema().id() == rawFile.schemaId()
                                                ? projectedFields
                                                : schemaEvolutionFieldNames(
                                                        projectedFields,
                                                        rowType.getFields(),
                                                        schemaManager
                                                                .schema(rawFile.schemaId())
                                                                .fields()),
                                        type,
                                        orderDomains(projectedFields, filter));

                        if (deletionFiles.isPresent()) {
                            source =
                                    TrinoPageSourceWrapper.wrap(
                                            source,
                                            Optional.ofNullable(deletionFiles.get().get(i))
                                                    .map(
                                                            deletionFile -> {
                                                                try {
                                                                    return DeletionVector.read(
                                                                            fileStoreTable.fileIO(),
                                                                            deletionFile);
                                                                } catch (IOException e) {
                                                                    throw new RuntimeException(e);
                                                                }
                                                            }));
                        }
                        sources.add(source);
                    }

                    return new DirectTrinoPageSource(sources);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                int[] columnIndex =
                        projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();

                // old read way
                ReadBuilder read = table.newReadBuilder();
                paimonFilter.ifPresent(read::withFilter);

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
        return optionalRawFiles.isPresent() && canUseTrinoPageSource(optionalRawFiles.get());
    }

    // only support orc yet.
    // TODO: support parquet and avro
    private boolean canUseTrinoPageSource(List<RawFile> rawFiles) {
        for (RawFile rawFile : rawFiles) {
            if (!rawFile.format().equals("orc")) {
                return false;
            }
        }
        return true;
    }

    // map the table schema column names to data schema column names
    private List<String> schemaEvolutionFieldNames(
            List<String> fieldNames, List<DataField> tableFields, List<DataField> dataFields) {

        Map<String, Integer> fieldNameToId = new HashMap<>();
        Map<Integer, String> idToFieldName = new HashMap<>();
        List<String> result = new ArrayList<>();

        tableFields.forEach(field -> fieldNameToId.put(field.name(), field.id()));
        dataFields.forEach(field -> idToFieldName.put(field.id(), field.name()));

        for (String fieldName : fieldNames) {
            Integer id = fieldNameToId.get(fieldName);
            result.add(idToFieldName.getOrDefault(id, null));
        }
        return result;
    }

    private ConnectorPageSource createDataPageSource(
            String format,
            TrinoInputFile inputFile,
            // TODO construct read option by core-options
            CoreOptions coreOptions,
            List<String> columns,
            List<Type> types,
            List<Domain> domains) {
        switch (format) {
            case "orc":
                {
                    return createOrcDataPageSource(
                            inputFile,
                            // TODO: pass options from catalog configuration
                            new OrcReaderOptions()
                                    // Default tiny stripe size 8 M is too big for paimon.
                                    // Cache stripe will cause more read (I want to read one column,
                                    // but not the whole stripe)
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
            List<String> columns,
            List<Type> types,
            List<Domain> domains) {
        try {
            OrcDataSource orcDataSource = new TrinoOrcDataSource(inputFile, options);
            OrcReader reader =
                    OrcReader.createOrcReader(orcDataSource, options)
                            .orElseThrow(() -> new RuntimeException("ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            Map<String, OrcColumn> fieldsMap = new HashMap<>();
            fileColumns.forEach(column -> fieldsMap.put(column.getColumnName(), column));
            TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder predicateBuilder =
                    TupleDomainOrcPredicate.builder();
            List<OrcPageSource.ColumnAdaptation> columnAdaptations = new ArrayList<>();
            List<OrcColumn> fileReadColumns = new ArrayList<>(columns.size());
            List<Type> fileReadTypes = new ArrayList<>(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) != null) {
                    // column exists
                    columnAdaptations.add(
                            OrcPageSource.ColumnAdaptation.sourceColumn(fileReadColumns.size()));
                    OrcColumn orcColumn = fieldsMap.get(columns.get(i));
                    if (orcColumn == null) {
                        throw new RuntimeException(
                                "Column " + columns.get(i) + " does not exist in orc file.");
                    }
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(types.get(i));
                    if (domains.get(i) != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domains.get(i));
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
