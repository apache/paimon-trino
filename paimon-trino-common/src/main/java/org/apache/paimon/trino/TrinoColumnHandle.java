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

import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/** Trino {@link ColumnHandle}. */
public final class TrinoColumnHandle implements ColumnHandle {
    private final String columnName;
    private final String typeString;
    private final Type trinoType;

    @JsonCreator
    public TrinoColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("typeString") String typeString,
            @JsonProperty("trinoType") Type trinoType) {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.typeString = requireNonNull(typeString, "columnType is null");
        this.trinoType = requireNonNull(trinoType, "columnType is null");
    }

    public static TrinoColumnHandle of(String columnName, DataType columnType) {
        return new TrinoColumnHandle(
                columnName,
                JsonSerdeUtil.toJson(columnType),
                TrinoTypeUtils.fromFlinkType(columnType));
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public String getTypeString() {
        return typeString;
    }

    @JsonProperty
    public Type getTrinoType() {
        return trinoType;
    }

    public DataType logicalType() {
        return JsonSerdeUtil.fromJson(typeString, DataType.class);
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, trinoType);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        TrinoColumnHandle other = (TrinoColumnHandle) obj;
        return columnName.equals(other.columnName);
    }

    @Override
    public String toString() {
        return "{"
                + "columnName='"
                + columnName
                + '\''
                + ", typeString='"
                + typeString
                + '\''
                + ", trinoType="
                + trinoType
                + '}';
    }
}
