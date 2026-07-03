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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.types.DataField;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;

import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/** Utilities to convert between Trino and Paimon view definitions. */
public final class TrinoViewUtils {

    private static final String TRINO_DIALECT = "trino";
    private static final String OPTION_OWNER = "trino.view.owner";
    private static final String OPTION_RUN_AS_INVOKER = "trino.view.run_as_invoker";

    private TrinoViewUtils() {}

    public static View toPaimonView(
            Identifier identifier, ConnectorViewDefinition definition, TypeManager typeManager) {
        requireNonNull(identifier, "identifier is null");
        requireNonNull(definition, "definition is null");
        requireNonNull(typeManager, "typeManager is null");

        List<ConnectorViewDefinition.ViewColumn> columns = definition.getColumns();
        List<DataField> fields =
                IntStream.range(0, columns.size())
                        .mapToObj(
                                index -> {
                                    ConnectorViewDefinition.ViewColumn column = columns.get(index);
                                    Type trinoType = typeManager.getType(column.getType());
                                    if (trinoType == null) {
                                        trinoType =
                                                typeManager.fromSqlType(column.getType().getId());
                                    }
                                    return new DataField(
                                            index,
                                            column.getName(),
                                            TrinoTypeUtils.toPaimonType(trinoType),
                                            column.getComment().orElse(null));
                                })
                        .collect(toList());

        String originalSql = definition.getOriginalSql();
        Map<String, String> dialects = new HashMap<>();
        dialects.put(TRINO_DIALECT, originalSql);

        Map<String, String> options = new HashMap<>();
        definition.getOwner().ifPresent(owner -> options.put(OPTION_OWNER, owner));
        options.put(OPTION_RUN_AS_INVOKER, String.valueOf(definition.isRunAsInvoker()));

        return new ViewImpl(
                identifier,
                fields,
                originalSql,
                dialects,
                definition.getComment().orElse(null),
                options);
    }

    public static ConnectorViewDefinition toConnectorViewDefinition(
            View view, TypeManager typeManager) {
        requireNonNull(view, "view is null");
        requireNonNull(typeManager, "typeManager is null");

        List<ConnectorViewDefinition.ViewColumn> columns =
                view.rowType().getFields().stream()
                        .map(
                                field ->
                                        new ConnectorViewDefinition.ViewColumn(
                                                field.name(),
                                                TrinoTypeUtils.fromPaimonType(field.type())
                                                        .getTypeId(),
                                                Optional.ofNullable(field.description())))
                        .collect(toList());

        String originalSql = view.query(TRINO_DIALECT);

        boolean runAsInvoker =
                Boolean.parseBoolean(
                        view.options()
                                .getOrDefault(OPTION_RUN_AS_INVOKER, Boolean.TRUE.toString()));
        Optional<String> owner = Optional.ofNullable(view.options().get(OPTION_OWNER));
        if (runAsInvoker) {
            owner = Optional.empty();
        }

        return new ConnectorViewDefinition(
                originalSql,
                Optional.empty(),
                Optional.empty(),
                columns,
                view.comment(),
                owner,
                runAsInvoker,
                List.of());
    }
}
