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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for trino sink connector. */
public class TestInsertTrinoITCase extends AbstractTestQueryFramework {

    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                            .build();
            queryRunner.installPlugin(new TrinoPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testNormalInsert() {
        super.assertUpdate("CREATE SCHEMA default");

        String tableName = "test_table_" + TestTable.randomTableSuffix();
        String createTable =
                "CREATE TABLE "
                        + tableName
                        + "(\n"
                        + "    boolean_column boolean, \n"
                        + "    integer_column integer, \n"
                        + "    bigint_column bigint,\n"
                        + "    double_column double,\n"
                        + "    decimal_column decimal(5, 3), \n"
                        + "    char_column char(3), \n"
                        + "    bounded_varchar_column varchar(5), \n"
                        + "    unbounded_varchar_column varchar, \n"
                        + "    date_column date,\n"
                        + "    time_column time(3),\n"
                        + "    timestamp_column timestamp(3),\n"
                        + "    timestamp_tz_column timestamp(3) with time zone\n"
                        + ")\n";
        super.assertUpdate(createTable);

        // test number column
        super.assertUpdate(
                "INSERT INTO "
                        + tableName
                        + " "
                        + "(boolean_column, integer_column, bigint_column, double_column, decimal_column) "
                        + "VALUES (true, 1, 100, 4.5, 3.697)",
                1L);

        String result =
                sql(
                        "SELECT boolean_column, integer_column, bigint_column, double_column, decimal_column"
                                + " FROM "
                                + tableName
                                + " where integer_column = 1");
        assertThat(result.equals("[[true, 1, 100, 4.5, 3.697]]"));

        // test string not null column
        super.assertUpdate(
                "INSERT INTO "
                        + tableName
                        + " "
                        + "(integer_column, char_column, bounded_varchar_column, unbounded_varchar_column) "
                        + "VALUES (2, VARCHAR 'aa     ', VARCHAR 'aa     ', VARCHAR 'aa     ')",
                1L);

        result =
                sql(
                        "SELECT integer_column, char_column, bounded_varchar_column, unbounded_varchar_column"
                                + " FROM "
                                + tableName
                                + " where integer_column = 2");
        assertThat(result.equals("[[2, aa , aa   , aa     ]]"));

        // test string null column
        super.assertUpdate(
                "INSERT INTO "
                        + tableName
                        + " "
                        + "(integer_column, char_column, bounded_varchar_column, unbounded_varchar_column) "
                        + "VALUES (3, NULL, NULL, NULL)",
                1L);
        result =
                sql(
                        "SELECT integer_column, char_column, bounded_varchar_column, unbounded_varchar_column"
                                + " FROM "
                                + tableName
                                + " where integer_column = 3");
        assertThat(result.equals("[[3, null, null, null]]"));

        // test datetime column
        super.assertUpdate(
                "INSERT INTO "
                        + tableName
                        + " "
                        + "(integer_column, date_column, time_column, timestamp_column, timestamp_tz_column) "
                        + "VALUES (4, TIMESTAMP '2023-05-24 19:54:40',  TIME '23:59:59.123', "
                        + "TIMESTAMP '2023-05-24 19:54:40', TIMESTAMP '2023-05-24 19:54:40.321 UTC')",
                1L);
        result =
                sql(
                        "SELECT integer_column, date_column, time_column, timestamp_column, timestamp_tz_column"
                                + " FROM "
                                + tableName
                                + " where integer_column = 4");
        assertThat(
                result.equals(
                        "[[4, 2023-05-24, 23:59:59.123, 2023-05-24T19:54:40, 2023-05-24T19:54:40.321Z[UTC]]]"));

        // query fails
        super.assertQueryFails(
                "INSERT INTO " + tableName + "(integer_column) VALUES (3e9)",
                "Out of range for integer: 3.0E9");
        super.assertQueryFails(
                "INSERT INTO " + tableName + " (char_column) VALUES ('abcd')",
                "\\QCannot truncate non-space characters when casting from varchar(4) to char(3) on INSERT");
        super.assertQueryFails(
                "INSERT INTO " + tableName + " (bounded_varchar_column) VALUES ('abcdef')",
                "\\QCannot truncate non-space characters when casting from varchar(6) to varchar(5) on INSERT");

        super.assertUpdate("DROP TABLE " + tableName);
    }

    private String sql(String sql) {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }
}
