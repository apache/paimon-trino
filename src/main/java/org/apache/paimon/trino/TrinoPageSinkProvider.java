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

import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.trino.catalog.TrinoCatalog;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.trino.ClassLoaderUtils.runWithContextClassLoader;

/** Trino {@link ConnectorPageSinkProvider}. */
public class TrinoPageSinkProvider implements ConnectorPageSinkProvider {

    private final TrinoCatalog trinoCatalog;

    @Inject
    public TrinoPageSinkProvider(TrinoMetadataFactory trinoMetadataFactory) {
        this.trinoCatalog =
                requireNonNull(trinoMetadataFactory, "trinoMetadataFactory is null")
                        .create()
                        .catalog();
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId) {
        return createPageSink((TrinoTableHandle) outputTableHandle, session);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId) {
        return createPageSink((TrinoTableHandle) insertTableHandle, session);
    }

    private ConnectorPageSink createPageSink(
            TrinoTableHandle tableHandle, ConnectorSession session) {
        trinoCatalog.initSession(session);
        Table table = tableHandle.tableWithDynamicOptions(trinoCatalog, session);
        validataBucketMode(table);

        return runWithContextClassLoader(
                () -> {
                    BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
                    if (TrinoSessionProperties.enableInsertOverwrite(session)) {
                        batchWriteBuilder.withOverwrite();
                    }
                    BatchTableWrite write = batchWriteBuilder.newWrite();
                    return new TrinoPageSink(write);
                },
                TrinoPageSinkProvider.class.getClassLoader());
    }

    private static void validataBucketMode(Table table) {
        BucketMode mode =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).bucketMode()
                        : BucketMode.HASH_FIXED;
        switch (mode) {
            case HASH_FIXED:
            case BUCKET_UNAWARE:
                break;
            default:
                throw new IllegalArgumentException("Unknown bucket mode: " + mode);
        }
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) mergeHandle.getTableHandle();
        Table table = trinoTableHandle.tableWithDynamicOptions(trinoCatalog, session);
        return new TrinoMergeSink(
                createPageSink(trinoTableHandle, session), table.rowType().getFields().size());
    }
}
