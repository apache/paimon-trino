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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.NullableValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Trino {@link ConnectorSplitManager}. */
public abstract class TrinoSplitManagerBase implements ConnectorSplitManager {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoSplitManagerBase.class);

    protected ConnectorSplitSource getSplits(
            ConnectorTableHandle connectorTableHandle,
            ConnectorSession session,
            Constraint constraint) {
        // TODO dynamicFilter?

        TrinoTableHandle tableHandle = (TrinoTableHandle) connectorTableHandle;
        Table table = tableHandle.tableWithDynamicOptions(session);
        ReadBuilder readBuilder = table.newReadBuilder();
        new TrinoFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(readBuilder::withFilter);
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));
        List<Split> splits = readBuilder.newScan().plan().splits();

        // Filter partition with trino function, suck as length(partition_column) > 10;
        RowType partitionType = TypeUtils.project(table.rowType(), table.partitionKeys());
        List<TrinoColumnHandle> partitionColumnHandles =
                table.partitionKeys().stream()
                        .map(tableHandle::columnHandle)
                        .collect(Collectors.toList());
        splits = filterByPartition(constraint, partitionColumnHandles, partitionType, splits);

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().getAsLong();
        double minimumSplitWeight = TrinoSessionProperties.getMinimumSplitWeight(session);
        return new TrinoSplitSource(
                splits.stream()
                        .map(
                                split ->
                                        TrinoSplit.fromSplit(
                                                split,
                                                Math.min(
                                                        Math.max(
                                                                (double) split.rowCount()
                                                                        / maxRowCount,
                                                                minimumSplitWeight),
                                                        1.0)))
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    static List<Split> filterByPartition(
            Constraint constraint,
            List<TrinoColumnHandle> parititonColumnHandles,
            RowType partitionType,
            List<Split> splits) {
        if (!(constraint == null
                || parititonColumnHandles.isEmpty()
                || constraint.predicate().isEmpty()
                || Sets.intersection(
                                constraint.getPredicateColumns().orElseThrow(),
                                new HashSet<>(parititonColumnHandles))
                        .isEmpty())) {
            RowDataToObjectArrayConverter rowDataToObjectArrayConverter =
                    new RowDataToObjectArrayConverter(partitionType);
            return splits.stream()
                    .filter(
                            split -> {
                                if (!(split instanceof DataSplit)) {
                                    return true;
                                }
                                BinaryRow partition = ((DataSplit) split).partition();
                                Map<ColumnHandle, NullableValue> bindings = new HashMap<>();
                                Object[] partitionObject =
                                        rowDataToObjectArrayConverter.convert(partition);
                                for (int i = 0; i < parititonColumnHandles.size(); i++) {
                                    TrinoColumnHandle trinoColumnHandle =
                                            parititonColumnHandles.get(i);
                                    try {
                                        bindings.put(
                                                trinoColumnHandle,
                                                NullableValue.of(
                                                        trinoColumnHandle.getTrinoType(),
                                                        TrinoTypeUtils.convertPaimonValueToTrino(
                                                                trinoColumnHandle.logicalType(),
                                                                partitionObject[i])));
                                    } catch (UnsupportedOperationException e) {
                                        LOG.warn(
                                                "Unsupported predicate, maybe the type of column is not supported yet.",
                                                e);
                                        return true;
                                    }
                                }
                                return constraint.predicate().get().test(bindings);
                            })
                    .collect(Collectors.toList());
        }
        return splits;
    }
}
