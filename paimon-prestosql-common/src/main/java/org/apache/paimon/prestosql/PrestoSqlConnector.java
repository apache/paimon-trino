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

import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.prestosql.PrestoSqlTableHandle.SCAN_SNAPSHOT;
import static org.apache.paimon.prestosql.PrestoSqlTableHandle.SCAN_TIMESTAMP;

/** PrestoSql {@link Connector}. */
public class PrestoSqlConnector implements Connector {
    private final PrestoSqlMetadataBase prestosqlMetadata;
    private final PrestoSqlSplitManagerBase prestosqlSplitManager;
    private final PrestoSqlPageSourceProvider prestosqlPageSourceProvider;
    private final List<PropertyMetadata<?>> tableProperties;

    public PrestoSqlConnector(
            PrestoSqlMetadataBase trinoMetadata,
            PrestoSqlSplitManagerBase trinoSplitManager,
            PrestoSqlPageSourceProvider trinoPageSourceProvider) {
        this.prestosqlMetadata = requireNonNull(trinoMetadata, "jmxMetadata is null");
        this.prestosqlSplitManager = requireNonNull(trinoSplitManager, "jmxSplitManager is null");
        this.prestosqlPageSourceProvider =
                requireNonNull(trinoPageSourceProvider, "jmxRecordSetProvider is null");
        tableProperties =
                new PrestoSqlTableOptions()
                        .getTableProperties().stream().collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return PrestoSqlTransactionHandle.INSTANCE;
    }

    @Override
    public PrestoSqlMetadataBase getMetadata(ConnectorTransactionHandle transactionHandle) {
        return prestosqlMetadata;
    }

    @Override
    public PrestoSqlSplitManagerBase getSplitManager() {
        return prestosqlSplitManager;
    }

    @Override
    public PrestoSqlPageSourceProvider getPageSourceProvider() {
        return prestosqlPageSourceProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return Arrays.asList(
                PropertyMetadata.longProperty(
                        SCAN_TIMESTAMP, SCAN_TIMESTAMP_MILLIS.description().toString(), null, true),
                PropertyMetadata.longProperty(
                        SCAN_SNAPSHOT, SCAN_SNAPSHOT_ID.description().toString(), null, true));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }
}
