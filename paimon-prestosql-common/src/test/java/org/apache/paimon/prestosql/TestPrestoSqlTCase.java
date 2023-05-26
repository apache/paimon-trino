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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for prestosql connector. */
public class TestPrestoSqlTCase extends AbstractTestQueryFramework {

    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "pt", DataTypes.STRING()),
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType()),
                                    new DataField(3, "c", new BigIntType()),
                                    new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.singletonList("pt"),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath3);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRow.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRow.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i", new IntType()),
                                    new DataField(
                                            1,
                                            "map",
                                            new MapType(
                                                    new VarCharType(VarCharType.MAX_LENGTH),
                                                    new VarCharType(VarCharType.MAX_LENGTH)))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.singletonList("i"),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    new HashMap<BinaryString, BinaryString>() {
                                        {
                                            put(fromString("1"), fromString("2"));
                                        }
                                    })));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                            .build();
            queryRunner.installPlugin(new PrestoSqlPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new BigIntType()),
                                // test field name has upper case
                                new DataField(2, "aCa", new VarCharType()),
                                new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    @Test
    public void testComplexTypes() {
        assertThat(sql("SELECT * FROM paimon.default.t4")).isEqualTo("[[1, {1=2}]]");
    }

    @Test
    public void testProjection() {
        assertThat(sql("SELECT * FROM paimon.default.t1"))
                .isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testSystemTable() {
        assertThat(
                        sql(
                                "SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testFilter() {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 4"))
                .isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    public void testGroupByWithCast() {
        assertThat(
                        sql(
                                "SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    public void testShowCreateTable() {
        assertThat(sql("SHOW CREATE TABLE paimon.default.t3"))
                .isEqualTo(
                        "[[CREATE TABLE paimon.default.t3 (\n"
                                + "   pt varchar,\n"
                                + "   a integer,\n"
                                + "   b bigint,\n"
                                + "   c bigint,\n"
                                + "   d integer\n"
                                + ")]]");
    }

    @Test
    public void testCreateSchema() {
        sql("CREATE SCHEMA paimon.test");
        assertThat(sql("SHOW SCHEMAS FROM paimon"))
                .isEqualTo("[[default], [information_schema], [test]]");
        sql("DROP SCHEMA paimon.test");
    }

    @Test
    public void testDropSchema() {
        sql("CREATE SCHEMA paimon.tpch");
        sql("DROP SCHEMA paimon.tpch");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema]]");
    }

    @Test
    public void testCreateTable() {
        sql(
                "CREATE TABLE orders ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        assertThat(sql("SHOW TABLES FROM paimon.default"))
                .isEqualTo("[[orders], [t1], [t2], [t3], [t4]]");
        sql("DROP TABLE IF EXISTS paimon.default.orders");
    }

    @Test
    public void testRenameTable() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME TO t6");
        assertThat(sql("SHOW TABLES FROM paimon.default"))
                .isEqualTo("[[t1], [t2], [t3], [t4], [t6]]");
        sql("DROP TABLE IF EXISTS paimon.default.t6");
    }

    @Test
    public void testDropTable() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
        assertThat(sql("SHOW TABLES FROM paimon.default")).isEqualTo("[[t1], [t2], [t3], [t4]]");
    }

    @Test
    public void testAddColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 ADD COLUMN zip varchar");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [order_status, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ], [zip, varchar(2147483646), , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testRenameColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME COLUMN order_status to g");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [g, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropColumn() {
        sql(
                "CREATE TABLE t5 ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.t5 DROP COLUMN order_status");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    private String sql(String sql) {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }
}
