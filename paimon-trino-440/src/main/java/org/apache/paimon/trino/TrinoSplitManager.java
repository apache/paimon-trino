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

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.trino.catalog.TrinoCatalog;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** Trino {@link ConnectorSplitManager}. */
public class TrinoSplitManager implements ConnectorSplitManager {

    private final TrinoCatalog trinoCatalog;

    @Inject
    public TrinoSplitManager(TrinoMetadataFactory trinoMetadataFactory) {
        this.trinoCatalog =
                requireNonNull(trinoMetadataFactory, "trinoMetadataFactory is null")
                        .create()
                        .catalog();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        return getSplits(table, session);
    }

    protected ConnectorSplitSource getSplits(
            ConnectorTableHandle connectorTableHandle, ConnectorSession session) {
        // TODO dynamicFilter?
        // TODO what is constraint?

        TrinoTableHandle tableHandle = (TrinoTableHandle) connectorTableHandle;
        Table table = tableHandle.tableWithDynamicOptions(trinoCatalog, session);
        ReadBuilder readBuilder = table.newReadBuilder();
        new TrinoFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(readBuilder::withFilter);
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));
        List<Split> splits = readBuilder.newScan().plan().splits();

        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
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
                        .collect(Collectors.toList()),
                ((TrinoTableHandle) connectorTableHandle).getLimit());
    }
}
