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

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.types.DataTypes;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.paimon.trino.TrinoFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** The test of TestTrinoFilterExtractor. */
public class TestTrinoFilterExtractor {
    @Test
    public void testExtractTrinoColumnHandleForExpressionFilter() {
        TupleDomain<ColumnHandle> summary = TupleDomain.all();
        Type mapType = TESTING_TYPE_MANAGER.fromSqlType("map<varchar,varchar>");
        String columnName = "map";
        String mapKeyName = "key";
        String constantValue = "value";
        Slice value = Slices.utf8Slice(constantValue);
        Call elemetAtFuntion =
                new Call(
                        BOOLEAN,
                        new FunctionName(TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                        List.of(
                                new Variable(columnName, mapType),
                                new Constant(
                                        io.airlift.slice.Slices.utf8Slice(mapKeyName),
                                        VarcharType.createUnboundedVarcharType())));
        ConnectorExpression expression =
                new Call(
                        BOOLEAN,
                        StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        List.of(
                                elemetAtFuntion,
                                new Constant(value, VarcharType.createUnboundedVarcharType())));
        Map<String, ColumnHandle> assignments = Maps.newHashMap();
        assignments.put(
                columnName,
                TrinoColumnHandle.of(
                        columnName, DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        Constraint constraint = new Constraint(summary, expression, assignments);
        Map<TrinoColumnHandle, Domain> domainMap =
                TrinoFilterExtractor.extractTrinoColumnHandleForExpressionFilter(constraint);
        assertThat(domainMap.entrySet().size()).isEqualTo(1);
        Map.Entry<TrinoColumnHandle, Domain> next = domainMap.entrySet().iterator().next();
        assertThat(next.getKey().getColumnName())
                .isEqualTo(TrinoFilterExtractor.toMapKey(columnName, mapKeyName));
        assertThat(
                        next.getValue()
                                .getValues()
                                .getRanges()
                                .getOrderedRanges()
                                .get(0)
                                .getLowBoundedValue())
                .isEqualTo(value);
    }
}
