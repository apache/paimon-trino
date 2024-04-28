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
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.trino.catalog.TrinoCatalog;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;

/** Extract filter from trino. */
public class TrinoFilterExtractor {
    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";

    /** Extract filter from trino , include ExpressionFilter. */
    public static Optional<TrinoFilter> extract(
            TrinoCatalog catalog, TrinoTableHandle trinoTableHandle, Constraint constraint) {

        TupleDomain<TrinoColumnHandle> oldFilter = trinoTableHandle.getFilter();
        TupleDomain<TrinoColumnHandle> newFilter =
                constraint
                        .getSummary()
                        .transformKeys(TrinoColumnHandle.class::cast)
                        .intersect(oldFilter);

        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        Map<TrinoColumnHandle, Domain> trinoColumnHandleForExpressionFilter =
                extractTrinoColumnHandleForExpressionFilter(constraint);

        LinkedHashMap<TrinoColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<TrinoColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        new TrinoFilterConverter(trinoTableHandle.table(catalog).rowType())
                .convert(newFilter, acceptedDomains, unsupportedDomains);

        List<String> partitionKeys = trinoTableHandle.table(catalog).partitionKeys();
        LinkedHashMap<TrinoColumnHandle, Domain> unenforcedDomains = new LinkedHashMap<>();
        acceptedDomains.forEach(
                (columnHandle, domain) -> {
                    if (!partitionKeys.contains(columnHandle.getColumnName())) {
                        unenforcedDomains.put(columnHandle, domain);
                    }
                });

        acceptedDomains.putAll(trinoColumnHandleForExpressionFilter);

        @SuppressWarnings({"unchecked", "rawtypes"})
        TupleDomain<ColumnHandle> remain =
                (TupleDomain)
                        TupleDomain.withColumnDomains(unsupportedDomains)
                                .intersect(TupleDomain.withColumnDomains(unenforcedDomains));

        return Optional.of(new TrinoFilter(TupleDomain.withColumnDomains(acceptedDomains), remain));
    }

    /**
     * Extract Expression filter from trino Constraint. Extract Trino Expression filter ( e.g.
     * element_at(jsonmap, 'a') = '1' ) to TrinoColumnHandle.
     */
    public static Map<TrinoColumnHandle, Domain> extractTrinoColumnHandleForExpressionFilter(
            Constraint constraint) {
        Map<TrinoColumnHandle, Domain> expressionPredicates = Collections.emptyMap();

        if (constraint.getExpression() instanceof Call) {
            Call expression = (Call) constraint.getExpression();
            Map<String, ColumnHandle> assignments = constraint.getAssignments();

            if (expression.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                expressionPredicates = handleElementAtArguments(assignments, expression, false);
            } else if (expression.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                expressionPredicates = handleElementAtArguments(assignments, expression, true);
            } else if (expression.getFunctionName().equals(AND_FUNCTION_NAME)) {
                expressionPredicates = handleAndArguments(assignments, expression);
            }
        }
        return expressionPredicates;
    }

    /** Using paimon, trino only supports element_at function to extract values from map type. */
    private static Map<TrinoColumnHandle, Domain> handleElementAtArguments(
            Map<String, ColumnHandle> assignments, Call expression, boolean isIn) {
        Map<TrinoColumnHandle, Domain> expressionPredicates = Maps.newHashMap();

        Call elementAtExpression = (Call) expression.getArguments().get(0);

        if (!elementAtExpression
                .getFunctionName()
                .getName()
                .equals(TRINO_MAP_ELEMENT_AT_FUNCTION_NAME)) {
            return expressionPredicates;
        }

        Variable columnExpression = (Variable) elementAtExpression.getArguments().get(0);
        Constant columnKey = (Constant) elementAtExpression.getArguments().get(1);

        TrinoColumnHandle trinoColumnHandle =
                (TrinoColumnHandle) assignments.get(columnExpression.getName());
        Type trinoType = trinoColumnHandle.getTrinoType();
        if (trinoType instanceof MapType) {
            String columnName = trinoColumnHandle.getColumnName();
            String key = ((Slice) columnKey.getValue()).toStringUtf8();
            Constant elementAtValue = (Constant) expression.getArguments().get(1);
            expressionPredicates.put(
                    TrinoColumnHandle.of(
                            toMapKey(columnName, key), TrinoTypeUtils.toPaimonType(trinoType)),
                    Domain.create(
                            SortedRangeSet.copyOf(
                                    isIn
                                            ? ((ArrayType) elementAtValue.getType())
                                                    .getElementType()
                                            : elementAtValue.getType(),
                                    isIn
                                            ? elementAtValue.getChildren().stream()
                                                    .map(
                                                            arguemnt ->
                                                                    Range.equal(
                                                                            arguemnt.getType(),
                                                                            ((Constant) arguemnt)
                                                                                    .getValue()))
                                                    .collect(Collectors.toList())
                                            : ImmutableList.of(
                                                    Range.equal(
                                                            elementAtValue.getType(),
                                                            elementAtValue.getValue()))),
                            false));
        }
        return expressionPredicates;
    }

    /** Generate map key name ,e.g. map[key]. */
    public static String toMapKey(String mapColumnName, String keyName) {
        return mapColumnName + "[" + keyName + "]";
    }

    /** Expression filter support the case of AND and IN. */
    private static Map<TrinoColumnHandle, Domain> handleAndArguments(
            Map<String, ColumnHandle> assignments, Call expression) {
        Map<TrinoColumnHandle, Domain> expressionPredicates = new HashMap<>();

        expression.getArguments().stream()
                .map(argument -> (Call) argument)
                .forEach(
                        argument -> {
                            if (argument.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                                expressionPredicates.putAll(
                                        handleElementAtArguments(assignments, argument, false));
                            } else if (argument.getFunctionName()
                                    .equals(IN_PREDICATE_FUNCTION_NAME)) {
                                expressionPredicates.putAll(
                                        handleElementAtArguments(assignments, argument, true));
                            }
                        });

        return expressionPredicates;
    }

    /** TrinoFilter for paimon trinoMetadata applyFilter. */
    public static class TrinoFilter {

        private final TupleDomain<TrinoColumnHandle> filter;
        private final TupleDomain<ColumnHandle> remainFilter;

        public TrinoFilter(
                TupleDomain<TrinoColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter) {
            this.filter = filter;
            this.remainFilter = remainFilter;
        }

        public TupleDomain<TrinoColumnHandle> getFilter() {
            return filter;
        }

        public TupleDomain<ColumnHandle> getRemainFilter() {
            return remainFilter;
        }
    }
}
