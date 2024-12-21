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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.HashMap;

/** Trino {@link ConnectorPageSource}. */
public class TrinoMergePageSourceWrapper implements ConnectorPageSource {

    private final ConnectorPageSource pageSource;
    private final HashMap<String, Integer> fieldToIndex;

    public TrinoMergePageSourceWrapper(
            ConnectorPageSource pageSource, HashMap<String, Integer> fieldToIndex) {
        this.pageSource = pageSource;
        this.fieldToIndex = fieldToIndex;
    }

    public static TrinoMergePageSourceWrapper wrap(
            ConnectorPageSource pageSource, HashMap<String, Integer> fieldToIndex) {
        return new TrinoMergePageSourceWrapper(pageSource, fieldToIndex);
    }

    @Override
    public long getCompletedBytes() {
        return pageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos() {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        return pageSource.isFinished();
    }

    @Override
    public Page getNextPage() {
        Page originPage = pageSource.getNextPage();
        if (originPage == null) {
            return null;
        }
        int positionCount = originPage.getPositionCount();

        Block[] newBlocks = new Block[originPage.getChannelCount() + 1];
        Block[] rowIdBlocks = new Block[fieldToIndex.size()];
        for (int i = 0, idx = 0; i < originPage.getChannelCount(); i++) {
            Block block = originPage.getBlock(i);
            newBlocks[i] = block;
            if (fieldToIndex.containsValue(i)) {
                rowIdBlocks[idx] = block;
                idx++;
            }
        }
        newBlocks[originPage.getChannelCount()] =
                RowBlock.fromFieldBlocks(positionCount, rowIdBlocks);

        return new Page(positionCount, newBlocks);
    }

    @Override
    public long getMemoryUsage() {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close() throws IOException {
        pageSource.close();
    }
}
