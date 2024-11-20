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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.testing.TestingSession.testSessionBuilder;

/** The query runner of trino. */
public class TrinoQueryRunner {

    private static final Logger LOG = Logger.get(TrinoQueryRunner.class);

    private static final String PAIMON_CATALOG = "paimon";

    private TrinoQueryRunner() {}

    public static DistributedQueryRunner createPrestoQueryRunner(
            Map<String, String> extraProperties) throws Exception {
        return createPrestoQueryRunner(extraProperties, ImmutableMap.of(), false);
    }

    public static DistributedQueryRunner createPrestoQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            boolean createTpchTables)
            throws Exception {

        Session session = testSessionBuilder().setCatalog(PAIMON_CATALOG).setSchema("tpch").build();

        DistributedQueryRunner queryRunner =
                DistributedQueryRunner.builder(session).setExtraProperties(extraProperties).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("paimon_data");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        queryRunner.installPlugin(new TrinoPlugin());

        Map<String, String> options =
                ImmutableMap.<String, String>builder()
                        .put("warehouse", catalogDir.toFile().toURI().toString())
                        .putAll(extraConnectorProperties)
                        .build();

        queryRunner.createCatalog(PAIMON_CATALOG, PAIMON_CATALOG, options);

        queryRunner.execute("CREATE SCHEMA tpch");

        // TODO
        /*if (createTpchTables) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());
        }*/

        return queryRunner;
    }

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = createPrestoQueryRunner(properties);
        } catch (Throwable t) {
            LOG.error(t);
            System.exit(1);
        }
        TimeUnit.MILLISECONDS.sleep(10);
        Logger log = Logger.get(TrinoQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
