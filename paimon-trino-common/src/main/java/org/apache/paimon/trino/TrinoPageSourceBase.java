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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.utils.InternalRowUtils;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Trino {@link ConnectorPageSource}. */
public abstract class TrinoPageSourceBase implements ConnectorPageSource {

    private final RecordReader<InternalRow> reader;
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    private boolean isFinished = false;

    public TrinoPageSourceBase(
            RecordReader<InternalRow> reader, List<ColumnHandle> projectedColumns) {
        this.reader = reader;
        this.columnTypes = new ArrayList<>();
        this.logicalTypes = new ArrayList<>();
        for (ColumnHandle handle : projectedColumns) {
            TrinoColumnHandle trinoColumnHandle = (TrinoColumnHandle) handle;
            columnTypes.add(trinoColumnHandle.getTrinoType());
            logicalTypes.add(trinoColumnHandle.logicalType());
        }

        this.pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public Page getNextPage() {
        return ClassLoaderUtils.runWithContextClassLoader(
                () -> {
                    try {
                        return nextPage();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                TrinoPageSourceBase.class.getClassLoader());
    }

    private Page nextPage() throws IOException {
        RecordIterator<InternalRow> batch = reader.readBatch();
        if (batch == null) {
            isFinished = true;
            return null;
        }
        InternalRow row;
        while ((row = batch.next()) != null) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendTo(
                        columnTypes.get(i),
                        logicalTypes.get(i),
                        InternalRowUtils.get(row, i, logicalTypes.get(i)),
                        output);
            }
        }
        batch.releaseBatch();
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    protected void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        } else if (javaType == long.class) {
            if (type.equals(BIGINT)
                    || type.equals(INTEGER)
                    || type.equals(TINYINT)
                    || type.equals(SMALLINT)
                    || type.equals(DATE)) {
                type.writeLong(output, ((Number) value).longValue());
            } else if (type.equals(REAL)) {
                type.writeLong(output, Float.floatToIntBits((Float) value));
            } else if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            } else if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_SECONDS)) {
                type.writeLong(
                        output,
                        ((Timestamp) value).getMillisecond() * MICROSECONDS_PER_MILLISECOND);
            } else if (type.equals(TIME_MICROS)) {
                type.writeLong(output, (int) value * MICROSECONDS_PER_MILLISECOND);
            } else {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        } else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        } else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        } else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        } else if (javaType == LongTimestampWithTimeZone.class) {
            checkArgument(type.equals(TIMESTAMP_TZ_MILLIS));
            Timestamp timestamp = (org.apache.paimon.data.Timestamp) value;
            type.writeObject(
                    output, fromEpochMillisAndFraction(timestamp.getMillisecond(), 0, UTC_KEY));
        } else if (javaType == Block.class) {
            writeBlock(output, type, logicalType, value);
        } else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value) {
        if (type instanceof VarcharType || type instanceof io.trino.spi.type.CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        } else if (type instanceof VarbinaryType) {
            type.writeSlice(output, wrappedBuffer((byte[]) value));
        } else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value) {
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        } else {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    protected void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value) {
        if (type instanceof ArrayType) {
            BlockBuilder builder = output.beginBlockEntry();

            InternalArray arrayData = (InternalArray) value;
            DataType elementType = DataTypeChecks.getNestedTypes(logicalType).get(0);
            for (int i = 0; i < arrayData.size(); i++) {
                appendTo(
                        type.getTypeParameters().get(0),
                        elementType,
                        InternalRowUtils.get(arrayData, i, elementType),
                        builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType) {
            InternalRow rowData = (InternalRow) value;
            BlockBuilder builder = output.beginBlockEntry();
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                Type fieldType = type.getTypeParameters().get(index);
                DataType fieldLogicalType =
                        ((org.apache.paimon.types.RowType) logicalType).getTypeAt(index);
                appendTo(
                        fieldType,
                        fieldLogicalType,
                        InternalRowUtils.get(rowData, index, fieldLogicalType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType = ((org.apache.paimon.types.MapType) logicalType).getKeyType();
            DataType valueType = ((org.apache.paimon.types.MapType) logicalType).getValueType();
            BlockBuilder builder = output.beginBlockEntry();
            for (int i = 0; i < keyArray.size(); i++) {
                appendTo(
                        type.getTypeParameters().get(0),
                        keyType,
                        InternalRowUtils.get(keyArray, i, keyType),
                        builder);
                appendTo(
                        type.getTypeParameters().get(1),
                        valueType,
                        InternalRowUtils.get(valueArray, i, valueType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }
}
