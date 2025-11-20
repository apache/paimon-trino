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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;
import org.apache.paimon.types.RowKind;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/** Trino {@link BucketFunction}. */
public class FixedBucketTableShuffleFunction implements BucketFunction {

    private final int workerCount;
    private final int bucketCount;
    private final boolean isRowId;
    private final ThreadLocal<Projection> projectionContext;
    private final TableSchema schema;
    private final List<String> bucketKeys; // 🔧 改为通用的 bucketKeys

    public FixedBucketTableShuffleFunction(
            List<Type> partitionChannelTypes,
            TrinoPartitioningHandle partitioningHandle,
            int workerCount) {

        this.schema = partitioningHandle.getOriginalSchema();

        // 🔧 关键修改：根据是否分区表选择不同的 keys
        List<String> partitionKeys = schema.partitionKeys();
        if (!partitionKeys.isEmpty()) {
            // 分区表：使用 partition keys
            this.bucketKeys = partitionKeys;
            this.projectionContext =
                    ThreadLocal.withInitial(
                            () ->
                                    CodeGenUtils.newProjection(
                                            schema.logicalPartitionType(), bucketKeys));
        } else {
            // 非分区表：使用 primary keys
            this.bucketKeys = schema.primaryKeys();
            this.projectionContext =
                    ThreadLocal.withInitial(
                            () -> CodeGenUtils.newProjection(schema.logicalRowType(), bucketKeys));
        }

        this.bucketCount = new CoreOptions(schema.options()).bucket();
        this.workerCount = workerCount;
        this.isRowId =
                partitionChannelTypes.size() == 1
                        && partitionChannelTypes.get(0) instanceof RowType;
    }

    @Override
    public int getBucket(Page page, int position) {
        Page processedPage = page;

        // 处理 RowBlock 的情况
        if (isRowId) {
            RowBlock rowBlock = (RowBlock) page.getBlock(0);
            try {
                Method method = RowBlock.class.getDeclaredMethod("getRawFieldBlocks");
                method.setAccessible(true);
                Block[] rawBlocks = (Block[]) method.invoke(rowBlock);
                processedPage = new Page(rowBlock.getPositionCount(), rawBlocks);
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException("Failed to extract raw field blocks from RowBlock", e);
            }
        }

        // 🔧 修改验证逻辑：验证 bucketKeys 数量
        int expectedBlockCount = bucketKeys.size();
        int actualBlockCount = processedPage.getChannelCount();

        if (actualBlockCount != expectedBlockCount) {
            throw new IllegalStateException(
                    String.format(
                            "Page block count mismatch: expected %d (bucket keys), but got %d. "
                                    + "Bucket keys: %s, Partition keys: %s, Primary keys: %s, Schema fields: %s",
                            expectedBlockCount,
                            actualBlockCount,
                            bucketKeys,
                            schema.partitionKeys(),
                            schema.primaryKeys(),
                            schema.fieldNames()));
        }

        // 使用 processedPage 创建 TrinoRow
        TrinoRow trinoRow =
                new TrinoRow(processedPage.getSingleValuePage(position), RowKind.INSERT);

        // 🔧 修改错误信息：显示 bucketKeys 相关信息
        BinaryRow pk;
        try {
            pk = projectionContext.get().apply(trinoRow);
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to extract bucket keys from row. "
                                    + "Row field count: %d, Bucket keys: %s, "
                                    + "Page block count: %d, Position: %d",
                            trinoRow.getFieldCount(),
                            bucketKeys,
                            processedPage.getChannelCount(),
                            position),
                    e);
        }

        int bucket =
                KeyAndBucketExtractor.bucket(
                        KeyAndBucketExtractor.bucketKeyHashCode(pk), bucketCount);
        return bucket % workerCount;
    }
}
