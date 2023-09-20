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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.utils.InternalRowUtils;

import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.ArrayValueBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.MapValueBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/** Trino {@link ConnectorPageSource}. */
public class TrinoPageSource extends TrinoPageSourceBase {

    public TrinoPageSource(RecordReader<InternalRow> reader, List<ColumnHandle> projectedColumns) {
        super(reader, projectedColumns);
    }

    protected void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value) {
        if (type instanceof ArrayType) {
            ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) output;
            try {
                arrayBlockBuilder.buildEntry(
                        (ArrayValueBuilder<Throwable>)
                                elementBuilder -> {
                                    InternalArray arrayData = (InternalArray) value;
                                    DataType elementType =
                                            DataTypeChecks.getNestedTypes(logicalType).get(0);
                                    for (int i = 0; i < arrayData.size(); i++) {
                                        appendTo(
                                                type.getTypeParameters().get(0),
                                                elementType,
                                                InternalRowUtils.get(arrayData, i, elementType),
                                                elementBuilder);
                                    }
                                });
            } catch (Throwable e) {
                e.printStackTrace();
            }
            return;
        }
        if (type instanceof RowType) {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) output;
            try {
                rowBlockBuilder.buildEntry(
                        (RowValueBuilder<Throwable>)
                                fieldBuilders -> {
                                    InternalRow rowData = (InternalRow) value;
                                    for (int index = 0;
                                            index < type.getTypeParameters().size();
                                            index++) {
                                        Type fieldType = type.getTypeParameters().get(index);
                                        DataType fieldLogicalType =
                                                ((org.apache.paimon.types.RowType) logicalType)
                                                        .getTypeAt(index);
                                        appendTo(
                                                fieldType,
                                                fieldLogicalType,
                                                InternalRowUtils.get(
                                                        rowData, index, fieldLogicalType),
                                                fieldBuilders.get(index));
                                    }
                                });
            } catch (Throwable e) {
                e.printStackTrace();
            }
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType = ((org.apache.paimon.types.MapType) logicalType).getKeyType();
            DataType valueType = ((org.apache.paimon.types.MapType) logicalType).getValueType();
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) output;
            try {
                mapBlockBuilder.buildEntry(
                        (MapValueBuilder<Throwable>)
                                (keyBuilder, valueBuilder) -> {
                                    for (int i = 0; i < keyArray.size(); i++) {
                                        appendTo(
                                                type.getTypeParameters().get(0),
                                                keyType,
                                                InternalRowUtils.get(keyArray, i, keyType),
                                                keyBuilder);
                                        appendTo(
                                                type.getTypeParameters().get(1),
                                                valueType,
                                                InternalRowUtils.get(valueArray, i, valueType),
                                                valueBuilder);
                                    }
                                });
            } catch (Throwable e) {
                e.printStackTrace();
            }
            return;
        }
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return super.getCompletedPositions();
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public CompletableFuture<?> isBlocked() {
        return super.isBlocked();
    }

    @Override
    public Metrics getMetrics() {
        return super.getMetrics();
    }
}
