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

package org.apache.paimon.trino.catalog;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.trino.ClassLoaderUtils;
import org.apache.paimon.trino.fileio.TrinoFileIOLoader;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

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
    public boolean caseSensitive() {
        return current.caseSensitive();
    }

    @Override
    public List<String> listDatabases() {
        return current.listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern) {
        return current.listDatabasesPaged(maxResults, pageToken, databaseNamePattern);
    }

    public void createDatabase(String s, boolean b, Map<String, String> map)
            throws DatabaseAlreadyExistException {
        current.createDatabase(s, b, map);
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        return current.getDatabase(name);
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        current.dropDatabase(s, b, b1);
    }

    @Override
    public void alterDatabase(String s, List<PropertyChange> list, boolean b)
            throws DatabaseNotExistException {
        current.alterDatabase(s, list, b);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return current.getTable(identifier);
    }

    @Override
    public Table getTableById(String tableId) throws TableIdNotExistException {
        return current.getTableById(tableId);
    }

    @Override
    public List<String> listTables(String s) throws DatabaseNotExistException {
        return current.listTables(s);
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        return current.listTablesPaged(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        return current.listTableDetailsPaged(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public List<Table> listTableDetails(String databaseName) throws DatabaseNotExistException {
        return current.listTableDetails(databaseName);
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
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        current.createView(identifier, view, ignoreIfExists);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        current.dropView(identifier, ignoreIfNotExists);
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        return current.getView(identifier);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        return current.listViews(databaseName);
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        current.renameView(fromView, toView, ignoreIfNotExists);
    }

    @Override
    public void alterView(Identifier view, List<ViewChange> viewChanges, boolean ignoreIfNotExists)
            throws ViewNotExistException, DialectAlreadyExistException, DialectNotExistException {
        current.alterView(view, viewChanges, ignoreIfNotExists);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return current.listPartitions(identifier);
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern)
            throws TableNotExistException {
        return current.listPartitionsPaged(identifier, maxResults, pageToken, partitionNamePattern);
    }

    @Override
    public List<Partition> listPartitionsByNames(
            Identifier identifier, List<Map<String, String>> partitionNames)
            throws TableNotExistException {
        return current.listPartitionsByNames(identifier, partitionNames);
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return current.supportsListObjectsPaged();
    }

    @Override
    public boolean supportsPartitionModification() {
        return current.supportsPartitionModification();
    }

    @Override
    public boolean supportsVersionManagement() {
        return current.supportsVersionManagement();
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws TableNotExistException {
        return current.commitSnapshot(identifier, tableUuid, snapshot, statistics);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        return current.loadSnapshot(identifier);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version)
            throws TableNotExistException {
        return current.loadSnapshot(identifier, version);
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException {
        return current.listSnapshotsPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant) throws TableNotExistException {
        current.rollbackTo(identifier, instant);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, Long snapshotId)
            throws TableNotExistException {
        current.rollbackTo(identifier, instant, snapshotId);
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        current.createBranch(identifier, branch, fromTag);
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        current.dropBranch(identifier, branch);
    }

    @Override
    public void renameBranch(Identifier identifier, String branch, String newBranch)
            throws BranchNotExistException, BranchAlreadyExistException {
        current.renameBranch(identifier, branch, newBranch);
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        current.fastForward(identifier, branch);
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        return current.listBranches(identifier);
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        return current.getTag(identifier, tagName);
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String branch,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        current.createTag(identifier, tagName, snapshotId, branch, ignoreIfExists);
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePattern)
            throws TableNotExistException {
        return current.listTagsPaged(identifier, maxResults, pageToken, tagNamePattern);
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        current.deleteTag(identifier, tagName);
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        current.createPartitions(identifier, partitions);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        current.dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {
        current.alterPartitions(identifier, partitions);
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
        return current.listFunctions(databaseName);
    }

    @Override
    public Function getFunction(Identifier identifier) throws FunctionNotExistException {
        return current.getFunction(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        current.createFunction(identifier, function, ignoreIfExists);
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        current.dropFunction(identifier, ignoreIfNotExists);
    }

    @Override
    public void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException,
                    DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        current.alterFunction(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public TableQueryAuthResult authTableQuery(Identifier identifier, @Nullable List<String> select)
            throws TableNotExistException {
        return current.authTableQuery(identifier, select);
    }

    @Override
    public Map<String, String> options() {
        return current.options();
    }

    @Override
    public CatalogLoader catalogLoader() {
        return current.catalogLoader();
    }

    public void close() throws Exception {
        if (current != null) {
            current.close();
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        current.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        current.alterTable(identifier, change, ignoreIfNotExists);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        current.markDonePartitions(identifier, partitions);
    }
}
