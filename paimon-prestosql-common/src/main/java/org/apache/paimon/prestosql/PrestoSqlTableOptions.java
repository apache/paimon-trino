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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.ArrayType;

import java.util.List;
import java.util.Map;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

/** PrestoSql table options. */
public class PrestoSqlTableOptions {

    public static final String PRIMARY_KEY_IDENTIFIER = "primary_key";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    public PrestoSqlTableOptions() {
        ImmutableList.Builder<PropertyMetadata<?>> builder = ImmutableList.builder();
        List<PrestoSqlTableOptionUtils.OptionInfo> optionInfos =
                PrestoSqlTableOptionUtils.getOptionInfos();
        optionInfos.forEach(
                item -> {
                    if (item.isEnum) {
                        builder.add(
                                enumProperty(
                                        item.prestosqlOptionKey,
                                        "option",
                                        item.clazz,
                                        null,
                                        false));
                    } else {
                        builder.add(stringProperty(item.prestosqlOptionKey, "option", null, false));
                    }
                });

        builder.add(
                new PropertyMetadata<>(
                        PRIMARY_KEY_IDENTIFIER,
                        "Primary keys for the table.",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value));

        builder.add(
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition keys for the table.",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value));

        tableProperties = builder.build();
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKeys(Map<String, Object> tableProperties) {
        List<String> primaryKeys = (List<String>) tableProperties.get(PRIMARY_KEY_IDENTIFIER);
        return primaryKeys == null ? ImmutableList.of() : ImmutableList.copyOf(primaryKeys);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedKeys(Map<String, Object> tableProperties) {
        List<String> partitionedKeys = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedKeys);
    }
}
