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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.InstantiationUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** PrestoSql {@link ConnectorTableHandle}. */
public final class PrestoSqlTableHandle implements ConnectorTableHandle {

    public static final String SCAN_TIMESTAMP = "scan_timestamp_millis";
    public static final String SCAN_SNAPSHOT = "scan_snapshot_id";

    private final String schemaName;
    private final String tableName;
    private final byte[] serializedTable;
    private final TupleDomain<PrestoSqlColumnHandle> filter;
    private final Optional<List<ColumnHandle>> projectedColumns;

    private Table lazyTable;

    public PrestoSqlTableHandle(String schemaName, String tableName, byte[] serializedTable) {
        this(schemaName, tableName, serializedTable, TupleDomain.all(), Optional.empty());
    }

    @JsonCreator
    public PrestoSqlTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("serializedTable") byte[] serializedTable,
            @JsonProperty("filter") TupleDomain<PrestoSqlColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.serializedTable = serializedTable;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
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
    public byte[] getSerializedTable() {
        return serializedTable;
    }

    @JsonProperty
    public TupleDomain<PrestoSqlColumnHandle> getFilter() {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns() {
        return projectedColumns;
    }

    public PrestoSqlTableHandle copy(TupleDomain<PrestoSqlColumnHandle> filter) {
        return new PrestoSqlTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns);
    }

    public PrestoSqlTableHandle copy(Optional<List<ColumnHandle>> projectedColumns) {
        return new PrestoSqlTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns);
    }

    public Table tableWithDynamicOptions(ConnectorSession session) {
        // see prestosqlConnector.getSessionProperties
        Map<String, String> dynamicOptions = new HashMap<>();
        Long scanTimestampMills = session.getProperty(SCAN_TIMESTAMP, Long.class);
        if (scanTimestampMills != null) {
            dynamicOptions.put(
                    CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), scanTimestampMills.toString());
        }
        Long scanSnapshotId = session.getProperty(SCAN_SNAPSHOT, Long.class);
        if (scanSnapshotId != null) {
            dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), scanSnapshotId.toString());
        }

        return dynamicOptions.size() > 0 ? table().copy(dynamicOptions) : table();
    }

    public Table table() {
        if (lazyTable == null) {
            try {
                lazyTable =
                        InstantiationUtil.deserializeObject(
                                serializedTable, this.getClass().getClassLoader());
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return lazyTable;
    }

    public ConnectorTableMetadata tableMetadata(TypeManager typeManager) {
        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                columnMetadatas(typeManager),
                Collections.emptyMap(),
                Optional.empty());
    }

    public List<ColumnMetadata> columnMetadatas(TypeManager typeManager) {
        return table().rowType().getFields().stream()
                .map(
                        column ->
                                ColumnMetadata.builder()
                                        .setName(column.name())
                                        .setType(
                                                PrestoSqlTypeUtils.fromPaimonType(
                                                        column.type(), typeManager))
                                        .setNullable(column.type().isNullable())
                                        .setComment(Optional.ofNullable(column.description()))
                                        .build())
                .collect(Collectors.toList());
    }

    public PrestoSqlColumnHandle columnHandle(String field, TypeManager typeManager) {
        List<String> fieldNames = FieldNameUtils.fieldNames(table().rowType());
        int index = fieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Cannot find field %s in schema %s", field, fieldNames));
        }
        return PrestoSqlColumnHandle.of(field, table().rowType().getTypeAt(index), typeManager);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrestoSqlTableHandle that = (PrestoSqlTableHandle) o;
        return Arrays.equals(serializedTable, that.serializedTable)
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaName, tableName, filter, projectedColumns, Arrays.hashCode(serializedTable));
    }
}
