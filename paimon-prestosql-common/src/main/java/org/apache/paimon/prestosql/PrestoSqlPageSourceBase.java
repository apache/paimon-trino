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

package org.apache.paimon.prestosql;

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
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.String.format;

/** PrestoSql {@link ConnectorPageSource}. */
public abstract class PrestoSqlPageSourceBase implements ConnectorPageSource {

    private final RecordReader<InternalRow> reader;
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    private boolean isFinished = false;

    public PrestoSqlPageSourceBase(
            RecordReader<InternalRow> reader, List<ColumnHandle> projectedColumns) {
        this.reader = reader;
        this.columnTypes = new ArrayList<>();
        this.logicalTypes = new ArrayList<>();
        for (ColumnHandle handle : projectedColumns) {
            PrestoSqlColumnHandle prestosqlColumnHandle = (PrestoSqlColumnHandle) handle;
            columnTypes.add(prestosqlColumnHandle.getPrestoSqlType());
            logicalTypes.add(prestosqlColumnHandle.logicalType());
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
                PrestoSqlPageSourceBase.class.getClassLoader());
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

    private void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        } else if (javaType == long.class) {
            if (type.equals(BIGINT)) {
                type.writeLong(output, ((Number) value).longValue());
            } else if (type.equals(INTEGER)) {
                type.writeLong(output, ((Number) value).intValue());
            } else if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            } else if (type.equals(DATE)) {
                type.writeLong(output, (int) value);
            } else if (type.equals(TIMESTAMP)) {
                type.writeLong(output, ((Timestamp) value).getMillisecond());
            } else if (type.equals(TIME)) {
                type.writeLong(output, (int) value);
            } else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                type.writeLong(
                        output,
                        packDateTimeWithZone(((Timestamp) value).getMillisecond(), UTC_KEY));
            } else {
                throw new PrestoSqlException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        } else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        } else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        } else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        } else if (javaType == Block.class) {
            writeBlock(output, type, logicalType, value);
        } else {
            throw new PrestoSqlException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value) {
        if (type instanceof VarcharType || type instanceof io.prestosql.spi.type.CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        } else if (type instanceof VarbinaryType) {
            type.writeSlice(output, wrappedBuffer((byte[]) value));
        } else {
            throw new PrestoSqlException(
                    GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value) {
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        } else {
            throw new PrestoSqlException(
                    GENERIC_INTERNAL_ERROR,
                    "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value) {
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
        throw new PrestoSqlException(
                GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }
}
