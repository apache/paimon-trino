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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/** Trino {@link Connector}. */
public abstract class TrinoConnectorBase implements Connector {
    private final TrinoMetadataBase trinoMetadata;
    private final TrinoSplitManagerBase trinoSplitManager;
    private final TrinoPageSourceProvider trinoPageSourceProvider;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Set<ConnectorTableFunction> tableFunctions;

    public TrinoConnectorBase(
            TrinoMetadataBase trinoMetadata,
            TrinoSplitManagerBase trinoSplitManager,
            TrinoPageSourceProvider trinoPageSourceProvider,
            TrinoTableOptions trinoTableOptions,
            TrinoSessionProperties trinoSessionProperties,
            Set<ConnectorTableFunction> tableFunctions) {
        this.trinoMetadata = requireNonNull(trinoMetadata, "jmxMetadata is null");
        this.trinoSplitManager = requireNonNull(trinoSplitManager, "jmxSplitManager is null");
        this.trinoPageSourceProvider =
                requireNonNull(trinoPageSourceProvider, "jmxRecordSetProvider is null");
        this.tableProperties = trinoTableOptions.getTableProperties();
        sessionProperties = trinoSessionProperties.getSessionProperties();
        this.tableFunctions =
                ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
    }

    protected ConnectorTransactionHandle beginTransactionBase(
            IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return TrinoTransactionHandle.INSTANCE;
    }

    protected TrinoMetadataBase getMetadataBase(ConnectorTransactionHandle transactionHandle) {
        return trinoMetadata;
    }

    @Override
    public TrinoSplitManagerBase getSplitManager() {
        return trinoSplitManager;
    }

    @Override
    public TrinoPageSourceProvider getPageSourceProvider() {
        return trinoPageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return tableFunctions;
    }
}
