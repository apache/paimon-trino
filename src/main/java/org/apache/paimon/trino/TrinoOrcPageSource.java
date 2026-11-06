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

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.metadata.CompressionKind;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcPageSource;

import java.util.Optional;

/**
 * {@link OrcPageSource} that also reports where the last returned page starts in the ORC file.
 *
 * <p>Deletion vectors address rows by their position in the data file. When the ORC reader skips
 * stripes or row groups (filter pushdown), the number of rows returned so far is no longer that
 * position; {@link OrcRecordReader#getFilePosition()} still is.
 */
public class TrinoOrcPageSource extends OrcPageSource {

    private final OrcRecordReader recordReader;

    public TrinoOrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            AggregatedMemoryContext memoryContext,
            FileFormatDataSourceStats stats,
            CompressionKind compressionKind) {
        super(
                recordReader,
                orcDataSource,
                Optional.empty(),
                Optional.empty(),
                memoryContext,
                stats,
                compressionKind);
        this.recordReader = recordReader;
    }

    /** Position in the file of the first row of the page last returned by this source. */
    public long getFilePosition() {
        return recordReader.getFilePosition();
    }
}
