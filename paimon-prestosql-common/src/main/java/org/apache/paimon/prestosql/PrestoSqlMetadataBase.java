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

package org.apache.paimon.prestosql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.StringUtils;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult.Assignment;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/** PrestoSql {@link ConnectorMetadata}. */
public abstract class PrestoSqlMetadataBase implements ConnectorMetadata {

    private final Catalog catalog;
    private final TypeManager typeManager;

    public PrestoSqlMetadataBase(Options catalogOptions, TypeManager typeManager) {
        try {
            SecurityContext.install(catalogOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.catalog =
                ClassLoaderUtils.runWithContextClassLoader(
                        () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions)),
                        getClass().getClassLoader());
        this.typeManager = typeManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return listSchemaNames();
    }

    private List<String> listSchemaNames() {
        return catalog.listDatabases();
    }

    @Override
    public void createSchema(
            ConnectorSession session,
            String schemaName,
            Map<String, Object> properties,
            PrestoPrincipal owner) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");

        try {
            catalog.createDatabase(schemaName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(format("database already existed: '%s'", schemaName));
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(schemaName),
                "schemaName cannot be null or empty");
        try {
            catalog.dropDatabase(schemaName, false, true);
        } catch (Catalog.DatabaseNotEmptyException e) {
            throw new RuntimeException(format("database is not empty: '%s'", schemaName));
        } catch (Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(format("database not exists: '%s'", schemaName));
        }
    }

    @Override
    public PrestoSqlTableHandle getTableHandle(
            ConnectorSession session, SchemaTableName tableName) {
        return getTableHandle(tableName);
    }

    @Override
    public ConnectorTableProperties getTableProperties(
            ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    public PrestoSqlTableHandle getTableHandle(SchemaTableName tableName) {
        Identifier tablePath = new Identifier(tableName.getSchemaName(), tableName.getTableName());
        byte[] serializedTable;
        try {
            serializedTable = InstantiationUtil.serializeObject(catalog.getTable(tablePath));
        } catch (Catalog.TableNotExistException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new PrestoSqlTableHandle(
                tableName.getSchemaName(), tableName.getTableName(), serializedTable);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        return ((PrestoSqlTableHandle) tableHandle).tableMetadata(typeManager);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
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
                        .primaryKey(PrestoSqlTableOptions.getPrimaryKeys(properties))
                        .partitionKeys(PrestoSqlTableOptions.getPartitionedKeys(properties));

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            builder.column(
                    column.getName(),
                    PrestoSqlTypeUtils.toPaimonType(column.getType()),
                    column.getComment());
        }

        PrestoSqlTableOptionUtils.buildOptions(builder, properties);

        return builder.build();
    }

    @Override
    public void renameTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SchemaTableName newTableName) {
        PrestoSqlTableHandle oldTableHandle = (PrestoSqlTableHandle) tableHandle;
        try {
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
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) tableHandle;
        try {
            catalog.dropTable(
                    new Identifier(
                            prestosqlTableHandle.getSchemaName(),
                            prestosqlTableHandle.getTableName()),
                    false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", prestosqlTableHandle.getTableName()));
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        PrestoSqlTableHandle table = (PrestoSqlTableHandle) tableHandle;
        Map<String, ColumnHandle> handleMap = new HashMap<>();
        for (ColumnMetadata column : table.columnMetadatas(typeManager)) {
            handleMap.put(column.getName(), table.columnHandle(column.getName(), typeManager));
        }
        return handleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((PrestoSqlColumnHandle) columnHandle).getColumnMetadata();
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
                                table ->
                                        getTableHandle(session, table)
                                                .columnMetadatas(typeManager)));
    }

    @Override
    public void addColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(
                        prestosqlTableHandle.getSchemaName(), prestosqlTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.addColumn(
                        column.getName(), PrestoSqlTypeUtils.toPaimonType(column.getType())));
        try {
            catalog.alterTable(identifier, changes, false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", prestosqlTableHandle.getTableName()));
        }
    }

    @Override
    public void renameColumn(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle source,
            String target) {
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(
                        prestosqlTableHandle.getSchemaName(), prestosqlTableHandle.getTableName());
        PrestoSqlColumnHandle prestosqlColumnHandle = (PrestoSqlColumnHandle) source;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.renameColumn(prestosqlColumnHandle.getColumnName(), target));
        try {
            catalog.alterTable(identifier, changes, false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", prestosqlTableHandle.getTableName()));
        }
    }

    @Override
    public void dropColumn(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(
                        prestosqlTableHandle.getSchemaName(), prestosqlTableHandle.getTableName());
        PrestoSqlColumnHandle prestosqlColumnHandle = (PrestoSqlColumnHandle) column;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.dropColumn(prestosqlColumnHandle.getColumnName()));
        try {
            catalog.alterTable(identifier, changes, false);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(
                    format("table not exists: '%s'", prestosqlTableHandle.getTableName()));
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) handle;
        TupleDomain<PrestoSqlColumnHandle> oldFilter = prestosqlTableHandle.getFilter();
        TupleDomain<PrestoSqlColumnHandle> newFilter =
                constraint
                        .getSummary()
                        .transform(PrestoSqlColumnHandle.class::cast)
                        .intersect(oldFilter);
        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        return Optional.of(
                new ConstraintApplicationResult<>(
                        prestosqlTableHandle.copy(newFilter), constraint.getSummary()));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments) {
        PrestoSqlTableHandle prestosqlTableHandle = (PrestoSqlTableHandle) handle;
        List<ColumnHandle> newColumns = new ArrayList<>(assignments.values());

        if (prestosqlTableHandle.getProjectedColumns().isPresent()
                && containSameElements(
                        newColumns, prestosqlTableHandle.getProjectedColumns().get())) {
            return Optional.empty();
        }

        List<Assignment> assignmentList = new ArrayList<>();
        assignments.forEach(
                (name, column) ->
                        assignmentList.add(
                                new Assignment(
                                        name,
                                        column,
                                        ((PrestoSqlColumnHandle) column).getPrestoSqlType())));

        return Optional.of(
                new ProjectionApplicationResult<>(
                        prestosqlTableHandle.copy(Optional.of(newColumns)),
                        projections,
                        assignmentList));
    }

    private static boolean containSameElements(
            List<? extends ColumnHandle> first, List<? extends ColumnHandle> second) {
        return new HashSet<>(first).equals(new HashSet<>(second));
    }
}
