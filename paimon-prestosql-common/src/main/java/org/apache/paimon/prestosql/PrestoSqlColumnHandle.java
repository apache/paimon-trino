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

import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

/** PrestoSql {@link ColumnHandle}. */
public final class PrestoSqlColumnHandle implements ColumnHandle {
    private final String columnName;
    private final String typeString;
    private final Type prestoSqlType;

    @JsonCreator
    public PrestoSqlColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("typeString") String typeString,
            @JsonProperty("prestoSqlType") Type prestoSqlType) {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.typeString = requireNonNull(typeString, "columnType is null");
        this.prestoSqlType = requireNonNull(prestoSqlType, "columnType is null");
    }

    public static PrestoSqlColumnHandle of(
            String columnName, DataType columnType, TypeManager typeManager) {
        return new PrestoSqlColumnHandle(
                columnName,
                JsonSerdeUtil.toJson(columnType),
                PrestoSqlTypeUtils.fromPaimonType(columnType, typeManager));
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
    public Type getPrestoSqlType() {
        return prestoSqlType;
    }

    public DataType logicalType() {
        return JsonSerdeUtil.fromJson(typeString, DataType.class);
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, prestoSqlType);
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

        PrestoSqlColumnHandle other = (PrestoSqlColumnHandle) obj;
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
                + ", prestoSqlType="
                + prestoSqlType
                + '}';
    }
}
