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

import org.apache.paimon.trino.catalog.TrinoCatalog;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

/** Trino {@link ConnectorPageSinkProvider}. */
public class TrinoPageSinkProvider implements ConnectorPageSinkProvider {
    private final TrinoCatalog catalog;

    public TrinoPageSinkProvider(TrinoCatalog catalog) {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId) {
        requireNonNull(outputTableHandle, "outputTableHandle is null");
        TrinoInsertTableHandle handle = (TrinoInsertTableHandle) outputTableHandle;
        return new TrinoPageSink(
                catalog, handle.getColumnTypes(), handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId) {
        requireNonNull(insertTableHandle, "insertTableHandle is null");
        TrinoInsertTableHandle handle = (TrinoInsertTableHandle) insertTableHandle;
        catalog.initSession(session);
        return new TrinoPageSink(
                catalog, handle.getColumnTypes(), handle.getSchemaName(), handle.getTableName());
    }
}
