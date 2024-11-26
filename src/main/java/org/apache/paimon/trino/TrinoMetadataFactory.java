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
import org.apache.paimon.trino.catalog.TrinoCatalog;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.ConfigurationUtils;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;

/** A factory to create {@link TrinoMetadata}. */
public class TrinoMetadataFactory {

    private final TrinoCatalog catalog;

    @Inject
    public TrinoMetadataFactory(
            Options options,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            HdfsConfig hdfsConfig,
            TrinoFileSystemFactory fileSystemFactory) {
        Configuration configuration = null;
        if (!hdfsConfig.getResourceConfigFiles().isEmpty()) {
            configuration = ConfigurationUtils.getInitialConfiguration();
            hdfsConfigurationInitializer.initializeConfiguration(configuration);
        }

        this.catalog = new TrinoCatalog(options, configuration, fileSystemFactory);
    }

    public TrinoMetadata create() {
        return new TrinoMetadata(catalog);
    }
}
