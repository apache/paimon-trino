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
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList.toImmutableList;

/** Trino {@link BucketFunction}. */
public class FixedBucketTableShuffleFunction implements BucketFunction {

    private final int bucketCount;
    private final List<Type> partitionChannelTypes;
    private final List<MethodHandle> hashCodeInvokers;
    private final boolean isRowId;

    public FixedBucketTableShuffleFunction(
            TypeOperators typeOperators, List<Type> partitionChannelTypes, int bucketCount) {
        requireNonNull(typeOperators, "typeOperators is null");
        this.bucketCount = bucketCount;
        if (partitionChannelTypes.size() == 1 && partitionChannelTypes.get(0) instanceof RowType) {
            this.partitionChannelTypes = partitionChannelTypes.get(0).getTypeParameters();
            isRowId = true;
        } else {
            isRowId = false;
            this.partitionChannelTypes = partitionChannelTypes;
        }
        hashCodeInvokers =
                this.partitionChannelTypes.stream()
                        .map(
                                type ->
                                        typeOperators.getHashCodeOperator(
                                                type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)))
                        .collect(toImmutableList());
    }

    @Override
    public int getBucket(Page page, int position) {
        if (isRowId) {
            RowBlock rowBlock = (RowBlock) page.getBlock(0);
            try {
                Method method = RowBlock.class.getDeclaredMethod("getRawFieldBlocks");
                method.setAccessible(true);
                page = new Page(rowBlock.getPositionCount(), (Block[]) method.invoke(rowBlock));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        long hash = 0;
        for (int i = 0; i < partitionChannelTypes.size(); i++) {
            Block block = page.getBlock(i);
            Object value = TypeUtils.readNativeValue(partitionChannelTypes.get(i), block, position);
            long valueHash = hashValue(hashCodeInvokers.get(i), value);
            hash = (31 * hash) + valueHash;
        }
        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    private static long hashValue(MethodHandle method, Object value) {
        if (value == null) {
            return NULL_HASH_CODE;
        }
        try {
            return (long) method.invoke(value);
        } catch (Throwable throwable) {
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new RuntimeException(throwable);
        }
    }
}
