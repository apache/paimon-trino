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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedList;

/** Trino {@link ConnectorPageSource}. */
public class DirectTrinoPageSource implements ConnectorPageSource {

    private ConnectorPageSource current;
    private final LinkedList<ConnectorPageSource> pageSourceQueue;

    public DirectTrinoPageSource(LinkedList<ConnectorPageSource> pageSourceQueue) {
        this.pageSourceQueue = pageSourceQueue;
        this.current = pageSourceQueue.poll();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return current == null ? 0 : current.getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        return current == null || (current.isFinished() && pageSourceQueue.isEmpty());
    }

    @Override
    public Page getNextPage() {
        try {
            if (current == null) {
                return null;
            }
            Page dataPage = current.getNextPage();
            if (dataPage == null) {
                advance();
                return getNextPage();
            }

            return dataPage;
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    private void advance() {
        if (current == null) {
            throw new RuntimeException("Current is null, should not invoke advance");
        }
        try {
            current.close();
        } catch (IOException e) {
            close();
            throw new RuntimeException("error happens while advance and close old page source.");
        }
        current = pageSourceQueue.poll();
    }

    @Override
    public void close() {
        try {
            if (current != null) {
                current.close();
            }
            for (ConnectorPageSource source : pageSourceQueue) {
                source.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return current == null ? null : current.toString();
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public Metrics getMetrics() {
        return current == null ? Metrics.EMPTY : current.getMetrics();
    }
}
