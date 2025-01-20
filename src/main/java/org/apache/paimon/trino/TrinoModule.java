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

import org.apache.paimon.options.Options;
import org.apache.paimon.trino.functions.TrinoFunctionProvider;
import org.apache.paimon.trino.functions.tablechanges.TableChangesFunctionProcessorProvider;
import org.apache.paimon.trino.functions.tablechanges.TableChangesFunctionProvider;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Map;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

/** Module for binding instance. */
public class TrinoModule implements Module {
    private Map<String, String> config;

    public TrinoModule(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(Options.class).toInstance(new Options(config));
        binder.bind(TrinoMetadataFactory.class).in(SINGLETON);
        binder.bind(TrinoSplitManager.class).in(SINGLETON);
        binder.bind(TrinoPageSourceProvider.class).in(SINGLETON);
        binder.bind(TrinoPageSinkProvider.class).in(SINGLETON);
        binder.bind(TrinoNodePartitioningProvider.class).in(SINGLETON);
        binder.bind(TrinoSessionProperties.class).in(SINGLETON);
        binder.bind(TrinoTableOptions.class).in(SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class)
                .addBinding()
                .toProvider(TableChangesFunctionProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(TrinoFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(TableChangesFunctionProcessorProvider.class).in(SINGLETON);
    }
}
