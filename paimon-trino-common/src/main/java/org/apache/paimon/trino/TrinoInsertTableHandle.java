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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

/** Trino {@link TrinoInsertTableHandle}. */
public class TrinoInsertTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle {
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    @JsonCreator
    public TrinoInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
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
    public List<String> getColumnNames() {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public String toString() {
        return "paimon:" + schemaName + "." + tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrinoInsertTableHandle that = (TrinoInsertTableHandle) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(columnNames, that.columnNames)
                && Objects.equals(columnTypes, that.columnTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName, columnNames, columnTypes);
    }
}
