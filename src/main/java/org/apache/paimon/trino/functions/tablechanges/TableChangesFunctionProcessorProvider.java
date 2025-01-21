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

package org.apache.paimon.trino.functions.tablechanges;

import org.apache.paimon.trino.TrinoPageSourceProvider;
import org.apache.paimon.trino.TrinoSplit;
import org.apache.paimon.trino.TrinoTableHandle;

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionSplitProcessor;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

/** TableChangesFunctionProcessorProvider. */
public class TableChangesFunctionProcessorProvider implements TableFunctionProcessorProvider {

    private final TrinoPageSourceProvider icebergPageSourceProvider;

    @Inject
    public TableChangesFunctionProcessorProvider(
            TrinoPageSourceProvider icebergPageSourceProvider) {
        this.icebergPageSourceProvider = icebergPageSourceProvider;
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
        return new ClassLoaderSafeTableFunctionSplitProcessor(
                new TableChangesFunctionProcessor(
                        session,
                        (TrinoTableHandle) handle,
                        (TrinoSplit) split,
                        icebergPageSourceProvider),
                getClass().getClassLoader());
    }
}
