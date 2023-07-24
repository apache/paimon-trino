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

import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;

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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Trino {@link ConnectorMetadata}. */
public abstract class TrinoMetadataBase implements ConnectorMetadata {

    private final Catalog catalog;

    public TrinoMetadataBase(Options catalogOptions) {
        try {
            SecurityContext.install(catalogOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
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
            TrinoPrincipal owner) {
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
    public TrinoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return getTableHandle(tableName);
    }

    @Override
    public ConnectorTableProperties getTableProperties(
            ConnectorSession session, ConnectorTableHandle table) {
        return new ConnectorTableProperties();
    }

    public TrinoTableHandle getTableHandle(SchemaTableName tableName) {
        Identifier tablePath = new Identifier(tableName.getSchemaName(), tableName.getTableName());
        byte[] serializedTable;
        try {
            serializedTable = InstantiationUtil.serializeObject(catalog.getTable(tablePath));
        } catch (Catalog.TableNotExistException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new TrinoTableHandle(
                tableName.getSchemaName(), tableName.getTableName(), serializedTable);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        return ((TrinoTableHandle) tableHandle).tableMetadata();
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
        for (ColumnMetadata column : table.columnMetadatas()) {
            handleMap.put(column.getName(), table.columnHandle(column.getName()));
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
                                table -> getTableHandle(session, table).columnMetadatas()));
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
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) handle;
        TupleDomain<TrinoColumnHandle> oldFilter = trinoTableHandle.getFilter();
        TupleDomain<TrinoColumnHandle> newFilter =
                constraint
                        .getSummary()
                        .transformKeys(TrinoColumnHandle.class::cast)
                        .intersect(oldFilter);
        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        return Optional.of(
                new ConstraintApplicationResult<>(
                        trinoTableHandle.copy(newFilter), constraint.getSummary(), false));
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
}
