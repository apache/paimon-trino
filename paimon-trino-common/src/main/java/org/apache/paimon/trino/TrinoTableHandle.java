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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Trino {@link ConnectorTableHandle}. */
public final class TrinoTableHandle extends TrinoTableHandleBase {

    public TrinoTableHandle(String schemaName, String tableName, byte[] serializedTable) {
        this(
                schemaName,
                tableName,
                serializedTable,
                TupleDomain.all(),
                Optional.empty(),
                OptionalLong.empty());
    }

    @JsonCreator
    public TrinoTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("serializedTable") byte[] serializedTable,
            @JsonProperty("filter") TupleDomain<TrinoColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit) {
        super(schemaName, tableName, serializedTable, filter, projectedColumns, limit);
    }

    public TrinoTableHandle copy(TupleDomain<TrinoColumnHandle> filter) {
        return new TrinoTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns, limit);
    }

    public TrinoTableHandle copy(Optional<List<ColumnHandle>> projectedColumns) {
        return new TrinoTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns, limit);
    }

    public TrinoTableHandle copy(OptionalLong limit) {
        return new TrinoTableHandle(
                schemaName, tableName, serializedTable, filter, projectedColumns, limit);
    }
}
