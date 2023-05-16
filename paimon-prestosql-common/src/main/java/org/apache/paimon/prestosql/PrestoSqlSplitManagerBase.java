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

import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.stream.Collectors;

/** PrestoSql {@link ConnectorSplitManager}. */
public abstract class PrestoSqlSplitManagerBase implements ConnectorSplitManager {

    protected ConnectorSplitSource getSplits(
            ConnectorTableHandle connectorTableHandle, ConnectorSession session) {
        // TODO dynamicFilter?
        // TODO what is constraint?

        PrestoSqlTableHandle tableHandle = (PrestoSqlTableHandle) connectorTableHandle;
        Table table = tableHandle.tableWithDynamicOptions(session);
        ReadBuilder readBuilder = table.newReadBuilder();
        new PrestoSqlFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(readBuilder::withFilter);
        List<Split> splits = readBuilder.newScan().plan().splits();
        return new PrestoSqlSplitSource(
                splits.stream().map(PrestoSqlSplit::fromSplit).collect(Collectors.toList()));
    }
}
