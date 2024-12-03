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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.trino.fileio.TrinoFileIOLoader;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

/** A factory to create {@link TrinoMetadata}. */
public class TrinoMetadataFactory {

    private final Options options;
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public TrinoMetadataFactory(Options options, TrinoFileSystemFactory fileSystemFactory) {
        this.options = options;
        this.trinoFileSystemFactory = fileSystemFactory;
    }

    public TrinoMetadata create(ConnectorIdentity identity) {
        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(identity);
        CatalogContext catalogContext =
                CatalogContext.create(options, new TrinoFileIOLoader(trinoFileSystem), null);
        try {
            SecurityContext.install(catalogContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);

        return new TrinoMetadata(catalog);
    }
}
