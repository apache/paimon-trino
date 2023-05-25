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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.InstantiationUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/** Trino {@link ConnectorPageSink}. */
public class TrinoPageSinkBase implements ConnectorPageSink {

    private final List<Type> columnTypes;
    private final Table table;
    private final BatchWriteBuilder writeBuilder;
    private final BatchTableWrite write;
    private List<CommitMessage> messageList = new ArrayList<>();

    public TrinoPageSinkBase(List<Type> columnTypes, byte[] serializedTable) {
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        try {
            table =
                    InstantiationUtil.deserializeObject(
                            serializedTable, this.getClass().getClassLoader());
            writeBuilder = table.newBatchWriteBuilder();
            write = writeBuilder.newWrite();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        writePage(page, write);
        try {
            messageList.addAll(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    private void writePage(Page chunk, BatchTableWrite write) {
        for (int position = 0; position < chunk.getPositionCount(); position++) {
            GenericRow record = new GenericRow(chunk.getChannelCount());
            for (int channel = 0; channel < chunk.getChannelCount(); channel++) {
                Block block = chunk.getBlock(channel);
                Type type = columnTypes.get(channel);
                Object value = formatValue(block, type, position);
                record.setField(channel, value);
            }
            try {
                write.write(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        try {
            if (!messageList.isEmpty()) {
                BatchTableCommit commit = writeBuilder.newCommit();
                commit.commit(messageList);
            } else {
                write.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {
        try {
            write.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object formatValue(Block block, Type type, int position) {
        if (block.isNull(position)) {
            return null;
        }

        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }

        if (type instanceof TinyintType) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }

        if (type instanceof SmallintType) {
            return Shorts.checkedCast(type.getLong(block, position));
        }

        if (type instanceof IntegerType) {
            return toIntExact(type.getLong(block, position));
        }

        if (type instanceof BigintType) {
            return type.getLong(block, position);
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }

        if (type instanceof DoubleType) {
            return type.getDouble(block, position);
        }

        if (type instanceof DateType) {
            return toIntExact(type.getLong(block, position));
        }

        if (type.equals(TIME_MILLIS)) {
            return toIntExact(type.getLong(block, position) / PICOSECONDS_PER_MILLISECOND);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            return Timestamp.fromEpochMillis(
                    type.getLong(block, position) / MICROSECONDS_PER_MILLISECOND);
        }

        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            long millisUtc = unpackMillisUtc(type.getLong(block, position));
            return Timestamp.fromEpochMillis(millisUtc);
        }

        if (type instanceof CharType) {
            return BinaryString.fromBytes(type.getSlice(block, position).getBytes());
        }

        if (type instanceof VarcharType) {
            return BinaryString.fromBytes(type.getSlice(block, position).getBytes());
        }

        if (type instanceof VarbinaryType) {
            return type.getSlice(block, position).getBytes();
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal value = readBigDecimal(decimalType, block, position);
            return Decimal.fromBigDecimal(
                    value, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
