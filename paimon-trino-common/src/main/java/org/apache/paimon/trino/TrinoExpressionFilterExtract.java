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
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;

/** Extract Trino Expression filter ( e.g. element_at(jsonmap, 'a') = '1' ) to TrinoColumnHandle. */
public class TrinoExpressionFilterExtract {
    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";

    /** Extract Expression filter from trino Constraint. */
    public static TupleDomain<TrinoColumnHandle> getTrinoColumnHandleForExpressionFilter(
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
        return TupleDomain.withColumnDomains(expressionPredicates);
    }

    /** Using paimon, trino only supports element_at function to extract values from map type . */
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

    /** Generate map key name ,e.g. map[key] . */
    public static String toMapKey(String mapColumnName, String keyName) {
        return mapColumnName + "[" + keyName + "]";
    }

    /** Expression filter support the case of AND and IN . */
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
}
