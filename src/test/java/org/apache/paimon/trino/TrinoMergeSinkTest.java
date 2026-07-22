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

import org.apache.paimon.types.RowKind;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoMergeSink}. */
public class TrinoMergeSinkTest {

    @Test
    void testTrino483MergePageLayout() {
        TestingPageSink pageSink = new TestingPageSink();
        TrinoMergeSink mergeSink = new TrinoMergeSink(pageSink, 2);

        Page input =
                new Page(
                        integerBlock(1, 2),
                        integerBlock(10, 20),
                        tinyintBlock(DELETE_OPERATION_NUMBER, INSERT_OPERATION_NUMBER),
                        integerBlock(17, 18),
                        bigintBlock(100, 200));

        mergeSink.storeMergedRows(input);

        Page delete = pageSink.pages.get(RowKind.DELETE);
        assertThat(delete.getChannelCount()).isEqualTo(2);
        assertThat(delete.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(delete.getBlock(0), 0)).isEqualTo(1);
        assertThat(INTEGER.getInt(delete.getBlock(1), 0)).isEqualTo(10);

        Page insert = pageSink.pages.get(RowKind.INSERT);
        assertThat(insert.getChannelCount()).isEqualTo(2);
        assertThat(insert.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(insert.getBlock(0), 0)).isEqualTo(2);
        assertThat(INTEGER.getInt(insert.getBlock(1), 0)).isEqualTo(20);
    }

    private static Block integerBlock(int... values) {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(values.length);
        for (int value : values) {
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block tinyintBlock(int... values) {
        BlockBuilder builder = TINYINT.createFixedSizeBlockBuilder(values.length);
        for (int value : values) {
            TINYINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block bigintBlock(long... values) {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static class TestingPageSink extends TrinoPageSink {

        private final Map<RowKind, Page> pages = new EnumMap<>(RowKind.class);

        private TestingPageSink() {
            super(null);
        }

        @Override
        public void writePage(Page page, RowKind rowKind) {
            pages.put(rowKind, page);
        }
    }
}
