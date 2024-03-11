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
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.InstantiationUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** Trino {@link ConnectorTableHandle}. */
public abstract class TrinoTableHandleBase implements ConnectorTableHandle {

    protected final String schemaName;
    protected final String tableName;
    protected final byte[] serializedTable;
    protected final TupleDomain<TrinoColumnHandle> filter;
    protected final Optional<List<ColumnHandle>> projectedColumns;
    protected final OptionalLong limit;

    protected Table lazyTable;

    public TrinoTableHandleBase(String schemaName, String tableName, byte[] serializedTable) {
        this(
                schemaName,
                tableName,
                serializedTable,
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty());
    }

    @JsonCreator
    public TrinoTableHandleBase(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("serializedTable") byte[] serializedTable,
            @JsonProperty("filter") TupleDomain<TrinoColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.serializedTable = serializedTable;
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
    public byte[] getSerializedTable() {
        return serializedTable;
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

    public Table tableWithDynamicOptions(ConnectorSession session) {
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

    public ConnectorTableMetadata tableMetadata() {
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(),
                Collections.emptyMap(),
                Optional.empty());
    }

    public List<ColumnMetadata> columnMetadatas() {
        return table().rowType().getFields().stream()
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

    public TrinoColumnHandle columnHandle(String field) {
        List<String> fieldNames = FieldNameUtils.fieldNames(table().rowType());
        int index = fieldNames.indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Cannot find field %s in schema %s", field, fieldNames));
        }
        return TrinoColumnHandle.of(field, table().rowType().getTypeAt(index));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrinoTableHandleBase that = (TrinoTableHandleBase) o;
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
