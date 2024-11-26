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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.trino.catalog.TrinoCatalog;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.StringUtils;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.paimon.table.BucketMode.FIXED;
import static org.apache.paimon.table.BucketMode.UNAWARE;
import static org.apache.paimon.trino.TrinoColumnHandle.TRINO_ROW_ID_NAME;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Trino {@link ConnectorMetadata}. */
public class TrinoMetadata implements ConnectorMetadata {
    private static final String TAG_PREFIX = "tag-";

    protected final TrinoCatalog catalog;

    public TrinoMetadata(TrinoCatalog catalog) {
        this.catalog = catalog;
    }

    public TrinoCatalog catalog() {
        return catalog;
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Table table = trinoTableHandle.table(catalog);
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException(table.getClass() + " is not supported");
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        switch (fileStoreTable.bucketMode()) {
            case FIXED:
                try {
                    return Optional.of(
                            new ConnectorTableLayout(
                                    new TrinoPartitioningHandle(
                                            InstantiationUtil.serializeObject(
                                                    fileStoreTable.schema()),
                                            FIXED),
                                    fileStoreTable.schema().bucketKeys(),
                                    false));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case DYNAMIC:
            case GLOBAL_DYNAMIC:
                if (table.primaryKeys().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Only primary-key table can support dynamic bucket.");
                }
                throw new IllegalArgumentException("Global dynamic bucket mode are not supported");
            case UNAWARE:
                try {
                    return Optional.of(
                            new ConnectorTableLayout(
                                    new TrinoPartitioningHandle(
                                            InstantiationUtil.serializeObject(
                                                    fileStoreTable.schema()),
                                            UNAWARE),
                                    fileStoreTable.schema().partitionKeys(),
                                    true));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new IllegalArgumentException("Unknown bucket mode");
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode) {
        createTable(session, tableMetadata, false);
        return getTableHandle(session, tableMetadata.getTable(), Collections.emptyMap());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        if (fragments.isEmpty()) {
            return Optional.empty();
        }
        return commit(session, (TrinoTableHandle) tableHandle, fragments);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode) {
        return (ConnectorInsertTableHandle) tableHandle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        return commit(session, (TrinoTableHandle) insertHandle, fragments);
    }

    private Optional<ConnectorOutputMetadata> commit(
            ConnectorSession session, TrinoTableHandle insertHandle, Collection<Slice> fragments) {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<CommitMessage> commitMessages =
                fragments.stream()
                        .map(
                                slice -> {
                                    try {
                                        return serializer.deserialize(
                                                serializer.getVersion(), slice.getBytes());
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(toList());

        if (commitMessages.isEmpty()) {
            return Optional.empty();
        }

        TrinoTableHandle table = insertHandle;
        BatchWriteBuilder batchWriteBuilder =
                table.tableWithDynamicOptions(catalog, session).newBatchWriteBuilder();
        if (TrinoSessionProperties.enableInsertOverwrite(session)) {
            batchWriteBuilder.withOverwrite();
        }
        batchWriteBuilder.newCommit().commit(commitMessages);
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    // todo support dynamic bucket table
    @Override
    public ColumnHandle getMergeRowIdColumnHandle(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Table table = trinoTableHandle.table(catalog);
        try {
            if (table.getClass()
                    == Class.forName("org.apache.paimon.table.AppendOnlyFileStoreTable")) {
                throw new IllegalArgumentException("Append-only table does not support upsert");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Set<String> pkSet = table.primaryKeys().stream().collect(Collectors.toSet());
        DataField[] row =
                table.rowType().getFields().stream()
                        .filter(dataField -> pkSet.contains(dataField.name()))
                        .toArray(DataField[]::new);
        return TrinoColumnHandle.of(TRINO_ROW_ID_NAME, DataTypes.ROW(row));
    }

    // todo support dynamic bucket table
    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Table table = trinoTableHandle.table(catalog);
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException(table.getClass() + " is not supported");
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        switch (fileStoreTable.bucketMode()) {
            case FIXED:
                try {
                    return Optional.of(
                            new TrinoPartitioningHandle(
                                    InstantiationUtil.serializeObject(fileStoreTable.schema()),
                                    FIXED));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            case DYNAMIC:
            case GLOBAL_DYNAMIC:
                if (table.primaryKeys().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Only primary-key table can support dynamic bucket.");
                }
                throw new IllegalArgumentException("Global dynamic bucket mode are not supported");
            case UNAWARE:
                if (!table.primaryKeys().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Only append table can support unaware bucket.");
                }
                throw new IllegalArgumentException("Unaware bucket mode are not supported");
            default:
                throw new IllegalArgumentException("Unknown bucket mode");
        }
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode) {
        return new TrinoMergeTableHandle((TrinoTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        commit(session, (TrinoTableHandle) mergeTableHandle.getTableHandle(), fragments);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        catalog.initSession(session);
        return catalog.databaseExists(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        catalog.initSession(session);
        return catalog.listDatabases();
    }

    @Override
    public void createSchema(
            ConnectorSession session,
            String schemaName,
            Map<String, Object> properties,
            TrinoPrincipal owner) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");

        try {
            catalog.initSession(session);
            catalog.createDatabase(schemaName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(format("database already existed: '%s'", schemaName));
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        try {
            catalog.initSession(session);
            catalog.dropDatabase(schemaName, false, true);
        } catch (Catalog.DatabaseNotEmptyException e) {
            throw new RuntimeException(format("database is not empty: '%s'", schemaName));
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(format("database not exists: '%s'", schemaName));
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL:
                    {
                        if (!(versionType instanceof TimestampWithTimeZoneType)) {
                            throw new TrinoException(
                                    NOT_SUPPORTED,
                                    "Unsupported type for table version: "
                                            + versionType.getDisplayName());
                        }
                        TimestampWithTimeZoneType timeZonedVersionType =
                                (TimestampWithTimeZoneType) versionType;
                        long epochMillis =
                                timeZonedVersionType.isShort()
                                        ? unpackMillisUtc((long) version.getVersion())
                                        : ((LongTimestampWithTimeZone) version.getVersion())
                                                .getEpochMillis();
                        dynamicOptions.put(
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                String.valueOf(epochMillis));
                        break;
                    }
                case TARGET_ID:
                    {
                        String tagOrVersion;
                        if (versionType instanceof VarcharType) {
                            tagOrVersion =
                                    BinaryString.fromBytes(
                                                    ((Slice) version.getVersion()).getBytes())
                                            .toString();
                        } else {
                            tagOrVersion = version.getVersion().toString();
                        }

                        // if value is not number, set tag option
                        boolean isNumber = StringUtils.isNumeric(tagOrVersion);
                        if (!isNumber) {
                            dynamicOptions.put(CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                        } else {
                            try {
                                catalog.initSession(session);
                                String path =
                                        catalog.getTable(
                                                        new Identifier(
                                                                tableName.getSchemaName(),
                                                                tableName.getTableName()))
                                                .options()
                                                .get("path");

                                if (catalog.fileIO()
                                        .exists(
                                                new Path(
                                                        path
                                                                + "/tag/"
                                                                + TAG_PREFIX
                                                                + tagOrVersion))) {
                                    dynamicOptions.put(
                                            CoreOptions.SCAN_TAG_NAME.key(), tagOrVersion);
                                } else {
                                    dynamicOptions.put(
                                            CoreOptions.SCAN_SNAPSHOT_ID.key(), tagOrVersion);
                                }
                            } catch (IOException | Catalog.TableNotExistException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        break;
                    }
            }
        }
        return getTableHandle(session, tableName, dynamicOptions);
    }

    @Override
    public TrinoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return getTableHandle(session, tableName, Collections.emptyMap());
    }

    @Override
    public ConnectorTableProperties getTableProperties(
            ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    public TrinoTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Map<String, String> dynamicOptions) {
        catalog.initSession(session);
        return catalog.tableExists(
                        Identifier.create(tableName.getSchemaName(), tableName.getTableName()))
                ? new TrinoTableHandle(
                        tableName.getSchemaName(), tableName.getTableName(), dynamicOptions)
                : null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        catalog.initSession(session);
        return ((TrinoTableHandle) tableHandle).tableMetadata(catalog);
    }

    @Override
    public void setTableProperties(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        Map<String, String> options =
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> (String) e.getValue().get()));
        options.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        // TODO: remove options, SET PROPERTIES x = DEFAULT
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        catalog.initSession(session);
        List<SchemaTableName> tables = new ArrayList<>();
        schemaName
                .map(Collections::singletonList)
                .orElseGet(catalog::listDatabases)
                .forEach(schema -> tables.addAll(listTables(schema)));
        return tables;
    }

    private List<SchemaTableName> listTables(String schema) {
        try {
            return catalog.listTables(schema).stream()
                    .map(table -> new SchemaTableName(schema, table))
                    .collect(toList());
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            boolean ignoreExisting) {
        SchemaTableName table = tableMetadata.getTable();
        Identifier identifier = Identifier.create(table.getSchemaName(), table.getTableName());

        try {
            catalog.initSession(session);
            catalog.createTable(identifier, prepareSchema(tableMetadata), false);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(format("database not exists: '%s'", table.getSchemaName()));
        } catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(format("table already existed: '%s'", table.getTableName()));
        }
    }

    private Schema prepareSchema(ConnectorTableMetadata tableMetadata) {
        Map<String, Object> properties = new HashMap<>(tableMetadata.getProperties());
        Schema.Builder builder =
                Schema.newBuilder()
                        .primaryKey(TrinoTableOptions.getPrimaryKeys(properties))
                        .partitionKeys(TrinoTableOptions.getPartitionedKeys(properties));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(
                    column.getName(),
                    TrinoTypeUtils.toPaimonType(column.getType()),
                    column.getComment());
        }

        TrinoTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    @Override
    public void renameTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SchemaTableName newTableName) {
        TrinoTableHandle oldTableHandle = (TrinoTableHandle) tableHandle;
        try {
            catalog.initSession(session);
            catalog.renameTable(
                    new Identifier(oldTableHandle.getSchemaName(), oldTableHandle.getTableName()),
                    new Identifier(newTableName.getSchemaName(), newTableName.getTableName()),
                    false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", oldTableHandle.getTableName()));
        } catch (Catalog.TableAlreadyExistException e) {
            throw new RuntimeException(
                    format("table already existed: '%s'", newTableName.getTableName()));
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        try {
            catalog.initSession(session);
            catalog.dropTable(
                    new Identifier(
                            trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName()),
                    false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", trinoTableHandle.getTableName()));
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        TrinoTableHandle table = (TrinoTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(catalog)) {
            handleMap.put(column.getName(), table.columnHandle(catalog, column.getName()));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((TrinoColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> tableNames;
        if (prefix.getTable().isPresent()) {
            tableNames = Collections.singletonList(prefix.toSchemaTableName());
        } else {
            tableNames = listTables(session, prefix.getSchema());
        }

        return tableNames.stream()
                .collect(
                        toMap(
                                Function.identity(),
                                table -> getTableHandle(session, table).columnMetadatas(catalog)));
    }

    @Override
    public void addColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.addColumn(
                        column.getName(), TrinoTypeUtils.toPaimonType(column.getType())));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }

    @Override
    public void renameColumn(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle source,
            String target) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName());
        TrinoColumnHandle trinoColumnHandle = (TrinoColumnHandle) source;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(trinoColumnHandle.getColumnName(), target));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }

    @Override
    public void dropColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName());
        TrinoColumnHandle trinoColumnHandle = (TrinoColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(trinoColumnHandle.getColumnName()));
        try {
            catalog.initSession(session);
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        catalog.initSession(session);
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) handle;
        Optional<TrinoFilterExtractor.TrinoFilter> extract =
                TrinoFilterExtractor.extract(catalog, trinoTableHandle, constraint);
        if (extract.isPresent()) {
            TrinoFilterExtractor.TrinoFilter trinoFilter = extract.get();
            return Optional.of(
                    new ConstraintApplicationResult<>(
                            trinoTableHandle.copy(trinoFilter.getFilter()),
                            trinoFilter.getRemainFilter(),
                            false));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) handle;
        List<ColumnHandle> newColumns = new ArrayList<>(assignments.values());

        if (trinoTableHandle.getProjectedColumns().isPresent()
                && containSameElements(newColumns, trinoTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        assignments.forEach(
                (name, column) ->
                        assignmentList.add(
                                new Assignment(
                                        name,
                                        column,
                                        ((TrinoColumnHandle) column).getTrinoType())));

        return Optional.of(
                new ProjectionApplicationResult<>(
                        trinoTableHandle.copy(Optional.of(newColumns)),
                        projections,
                        assignmentList,
                        false));
    }

    private static boolean containSameElements(
            List<? extends ColumnHandle> first, List<? extends ColumnHandle> second) {
        return new HashSet<>(first).equals(new HashSet<>(second));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit) {
        TrinoTableHandle table = (TrinoTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        if (!table.getFilter().isAll()) {
            Table paimonTable = table.table(catalog);
            LinkedHashMap<TrinoColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
            LinkedHashMap<TrinoColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
            new TrinoFilterConverter(paimonTable.rowType())
                    .convert(table.getFilter(), acceptedDomains, unsupportedDomains);
            Set<String> acceptedFields =
                    acceptedDomains.keySet().stream()
                            .map(TrinoColumnHandle::getColumnName)
                            .collect(Collectors.toSet());
            if (unsupportedDomains.size() > 0
                    || !paimonTable.partitionKeys().containsAll(acceptedFields)) {
                return Optional.empty();
            }
        }

        table = table.copy(OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }
}
