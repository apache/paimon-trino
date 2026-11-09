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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.CloseableIterator;

import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoPageSource}. */
public class TrinoPageSourceTest {

    @Test
    void testGetNextSourcePage() {
        TrinoColumnHandle column =
                TrinoColumnHandle.of("id", org.apache.paimon.types.DataTypes.BIGINT());
        TrinoPageSource pageSource =
                new TrinoPageSource(
                        new InMemoryRecordReader(GenericRow.of(42L)),
                        List.of(column),
                        OptionalLong.empty());

        SourcePage page = pageSource.getNextSourcePage();

        assertThat(page).isNotNull();
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(42L);
    }

    private static class InMemoryRecordReader implements RecordReader<InternalRow> {

        private final CloseableIterator<InternalRow> iterator;

        private InMemoryRecordReader(InternalRow row) {
            this.iterator = CloseableIterator.adapterForIterator(List.of(row).iterator());
        }

        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (!iterator.hasNext()) {
                return null;
            }
            return new RecordIterator<InternalRow>() {
                @Override
                public InternalRow next() {
                    return iterator.hasNext() ? iterator.next() : null;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() throws IOException {
            try {
                iterator.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
