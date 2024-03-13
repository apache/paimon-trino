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
import org.apache.paimon.table.Table;
import org.apache.paimon.trino.catalog.TrinoCatalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** Trino {@link ConnectorTableHandle}. */
public class TrinoTableHandle implements ConnectorTableHandle {

    protected final String schemaName;
    protected final String tableName;
    protected final TupleDomain<TrinoColumnHandle> filter;
    protected final Optional<List<ColumnHandle>> projectedColumns;
    protected final OptionalLong limit;
    protected final Map<String, String> dynamicOptions;
    protected TrinoFileSystem trinoFileSystem;

    private transient Table table;

    public TrinoTableHandle(
            String schemaName, String tableName, Map<String, String> dynamicOptions) {
        this(
                schemaName,
                tableName,
                dynamicOptions,
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty());
    }

    @JsonCreator
    public TrinoTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("dynamicOptions") Map<String, String> dynamicOptions,
            @JsonProperty("filter") TupleDomain<TrinoColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.dynamicOptions = dynamicOptions;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
        this.limit = limit;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @JsonProperty
    public TupleDomain<TrinoColumnHandle> getFilter() {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns() {
        return projectedColumns;
    }

    public OptionalLong getLimit() {
        return limit;
    }

    public Table tableWithDynamicOptions(TrinoCatalog catalog, ConnectorSession session) {
        Table paimonTable = table(catalog);

        // see TrinoConnector.getSessionProperties
        Map<String, String> dynamicOptions = new HashMap<>();
        Long scanTimestampMills = TrinoSessionProperties.getScanTimestampMillis(session);
        if (scanTimestampMills != null) {
            dynamicOptions.put(
                    CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), scanTimestampMills.toString());
        }
        Long scanSnapshotId = TrinoSessionProperties.getScanSnapshotId(session);
        if (scanSnapshotId != null) {
            dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), scanSnapshotId.toString());
        }

        return dynamicOptions.size() > 0 ? paimonTable.copy(dynamicOptions) : paimonTable;
    }

    public Table table(TrinoCatalog catalog) {
        if (table != null) {
            return table;
        }
        try {
            table = catalog.getTable(Identifier.create(schemaName, tableName)).copy(dynamicOptions);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    public ConnectorTableMetadata tableMetadata(TrinoCatalog catalog) {
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(catalog),
                Collections.emptyMap(),
                Optional.empty());
    }

    public List<ColumnMetadata> columnMetadatas(TrinoCatalog catalog) {
        return table(catalog).rowType().getFields().stream()
                .map(
                        column ->
                                ColumnMetadata.builder()
                                        .setName(column.name())
                                        .setType(TrinoTypeUtils.fromPaimonType(column.type()))
                                        .setNullable(column.type().isNullable())
                                        .setComment(Optional.ofNullable(column.description()))
                                        .build())
                .collect(Collectors.toList());
    }

    public TrinoColumnHandle columnHandle(TrinoCatalog catalog, String field) {
        Table paimonTable = table(catalog);
        List<String> fieldNames = FieldNameUtils.fieldNames(paimonTable.rowType());
        int index = fieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Cannot find field %s in schema %s", field, fieldNames));
        }
        return TrinoColumnHandle.of(field, paimonTable.rowType().getTypeAt(index));
    }

    public void setTrinoFileSystem(TrinoFileSystem fileSystem) {
        this.trinoFileSystem = fileSystem;
    }

    public TrinoTableHandle copy(TupleDomain<TrinoColumnHandle> filter) {
        return new TrinoTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public TrinoTableHandle copy(Optional<List<ColumnHandle>> projectedColumns) {
        return new TrinoTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    public TrinoTableHandle copy(OptionalLong limit) {
        return new TrinoTableHandle(
                schemaName, tableName, dynamicOptions, filter, projectedColumns, limit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrinoTableHandle that = (TrinoTableHandle) o;
        return Objects.equals(dynamicOptions, that.dynamicOptions)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName, filter, projectedColumns, dynamicOptions);
    }
}
