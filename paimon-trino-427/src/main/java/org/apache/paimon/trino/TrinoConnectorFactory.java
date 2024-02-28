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

import com.google.inject.Module;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.hdfs.HdfsModule;
import io.trino.hdfs.authentication.HdfsAuthenticationModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

/** Trino {@link ConnectorFactory}. */
public class TrinoConnectorFactory extends TrinoConnectorFactoryBase {

    @Override
    protected Module[] modules(
            String catalogName, Map<String, String> config, ConnectorContext context) {
        return new Module[] {
            new JsonModule(),
            new TrinoModule(config),
            new HdfsModule(),
            new HdfsAuthenticationModule(),
            new FileSystemModule(),
            binder -> {
                binder.bind(NodeVersion.class)
                        .toInstance(
                                new NodeVersion(
                                        context.getNodeManager().getCurrentNode().getVersion()));
                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry());
                binder.bind(Tracer.class).toInstance(context.getTracer());
                binder.bind(OrcReaderConfig.class).toInstance(new OrcReaderConfig());
            }
        };
    }
}
