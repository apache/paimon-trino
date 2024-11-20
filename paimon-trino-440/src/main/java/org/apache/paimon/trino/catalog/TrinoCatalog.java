/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.paimon.trino.catalog;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.table.Table;
import org.apache.paimon.trino.ClassLoaderUtils;
import org.apache.paimon.trino.fileio.TrinoFileIOLoader;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Trino catalog, use it after set session. */
public class TrinoCatalog implements Catalog {

    private final Options options;

    private final Configuration configuration;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    private Catalog current;

    private volatile boolean inited = false;

    public TrinoCatalog(
            Options options,
            Configuration configuration,
            TrinoFileSystemFactory trinoFileSystemFactory) {
        this.options = options;
        this.configuration = configuration;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    public void initSession(ConnectorSession connectorSession) {
        if (!inited) {
            synchronized (this) {
                if (!inited) {
                    current =
                            ClassLoaderUtils.runWithContextClassLoader(
                                    () -> {
                                        TrinoFileSystem trinoFileSystem =
                                                trinoFileSystemFactory.create(connectorSession);
                                        CatalogContext catalogContext =
                                                CatalogContext.create(
                                                        options,
                                                        configuration,
                                                        new TrinoFileIOLoader(trinoFileSystem),
                                                        null);
                                        try {
                                            SecurityContext.install(catalogContext);
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                        return CatalogFactory.createCatalog(catalogContext);
                                    },
                                    this.getClass().getClassLoader());
                    inited = true;
                }
            }
        }
    }

    @Override
    public String warehouse() {
        if (!inited) {
            throw new RuntimeException("Not inited yet.");
        }
        return current.warehouse();
    }

    @Override
    public Map<String, String> options() {
        if (!inited) {
            throw new RuntimeException("Not inited yet.");
        }
        return current.options();
    }

    @Override
    public FileIO fileIO() {
        if (!inited) {
            throw new RuntimeException("Not inited yet.");
        }
        return current.fileIO();
    }

    @Override
    public Optional<CatalogLockFactory> lockFactory() {
        return current.lockFactory();
    }

    @Override
    public List<String> listDatabases() {
        return current.listDatabases();
    }

    @Override
    public boolean databaseExists(String s) {
        return current.databaseExists(s);
    }

    @Override
    public void createDatabase(String s, boolean b, Map<String, String> map)
            throws DatabaseAlreadyExistException {
        current.createDatabase(s, b, map);
    }

    @Override
    public Map<String, String> loadDatabaseProperties(String s) throws DatabaseNotExistException {
        return current.loadDatabaseProperties(s);
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        current.dropDatabase(s, b, b1);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return current.getTable(identifier);
    }

    @Override
    public List<String> listTables(String s) throws DatabaseNotExistException {
        return current.listTables(s);
    }

    @Override
    public void dropTable(Identifier identifier, boolean b) throws TableNotExistException {
        current.dropTable(identifier, b);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        current.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfExistsb)
            throws TableNotExistException, TableAlreadyExistException {
        current.renameTable(fromTable, toTable, ignoreIfExistsb);
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> list, boolean ignoreIfExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, list, ignoreIfExists);
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {
        current.dropPartition(identifier, partitions);
    }

    @Override
    public void close() throws Exception {
        if (current != null) {
            current.close();
        }
    }

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return current.lockContext();
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier) {
        return current.metastoreClientFactory(identifier);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        current.createDatabase(name, ignoreIfExists);
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        return current.tableExists(identifier);
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public boolean caseSensitive() {
        return current.caseSensitive();
    }
}
