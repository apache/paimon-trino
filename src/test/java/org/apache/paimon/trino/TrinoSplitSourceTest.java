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

import org.apache.paimon.table.source.Split;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoSplitSource}. */
public class TrinoSplitSourceTest {

    @Test
    void testGetNextBatchWithDynamicFilterSnapshot() {
        TrinoSplit first = TrinoSplit.fromSplit(new TestingSplit(2), 1.0);
        TrinoSplit second = TrinoSplit.fromSplit(new TestingSplit(3), 1.0);
        TrinoSplitSource source =
                new TrinoSplitSource(List.of(first, second), OptionalLong.empty());

        List<ConnectorSplit> firstBatch =
                source.getNextBatch(1, DynamicFilterSnapshot.EMPTY).join();
        List<ConnectorSplit> secondBatch =
                source.getNextBatch(10, DynamicFilterSnapshot.EMPTY).join();

        assertThat(firstBatch).containsExactly(first);
        assertThat(secondBatch).containsExactly(second);
        assertThat(source.isFinished()).isTrue();
    }

    private static class TestingSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final long rowCount;

        private TestingSplit(long rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }
}
