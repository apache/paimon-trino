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

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowKind;

import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Trino {@link BucketFunction}. */
public class UnawareTableShuffleFunction implements BucketFunction {
    private static final Logger LOG = LoggerFactory.getLogger(UnawareTableShuffleFunction.class);
    private final int workerCount;
    private final boolean hasPartitionKeys;
    private final Projection partitionProjection;

    public UnawareTableShuffleFunction(
            List<Type> partitionChannelTypes,
            TrinoPartitioningHandle partitioningHandle,
            int workerCount) {
        this.hasPartitionKeys = partitionChannelTypes.size() > 0;
        TableSchema schema = partitioningHandle.getOriginalSchema();
        this.partitionProjection =
                CodeGenUtils.newProjection(schema.logicalPartitionType(), schema.partitionKeys());
        this.workerCount = workerCount;
    }

    @Override
    public int getBucket(Page page, int position) {
        if (!hasPartitionKeys) {
            return 0;
        } else {
            TrinoRow trinoRow = new TrinoRow(page.getSingleValuePage(position), RowKind.INSERT);
            BinaryRow partition = partitionProjection.apply(trinoRow);
            return partition.hashCode() % workerCount;
        }
    }
}
