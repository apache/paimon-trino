package org.apache.paimon.trino;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import io.airlift.slice.Slices;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link TrinoSplitManager}. */
public class TestTrinoSplitManager {

    @Test
    public void testPartitionFilter() {
        RowType partitionType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.BOOLEAN()),
                                new DataField(1, "b", DataTypes.TINYINT()),
                                new DataField(2, "c", DataTypes.SMALLINT()),
                                new DataField(3, "d", DataTypes.INT()),
                                new DataField(4, "e", DataTypes.BIGINT()),
                                new DataField(5, "f", DataTypes.FLOAT()),
                                new DataField(6, "g", DataTypes.DOUBLE()),
                                new DataField(7, "h", DataTypes.CHAR(4)),
                                new DataField(8, "i", DataTypes.TIMESTAMP(6)),
                                new DataField(9, "j", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                new DataField(10, "k", DataTypes.DECIMAL(10, 5))));

        BinaryRow partition1 = new BinaryRow(11);
        {
            BinaryRowWriter writer = new BinaryRowWriter(partition1);
            writer.writeBoolean(0, true);
            writer.writeByte(1, Byte.MAX_VALUE);
            writer.writeShort(2, Short.MAX_VALUE);
            writer.writeInt(3, Integer.MAX_VALUE);
            writer.writeLong(4, Long.MAX_VALUE);
            writer.writeFloat(5, Float.MAX_VALUE);
            writer.writeDouble(6, Double.MAX_VALUE);
            writer.writeString(7, BinaryString.fromString("char1"));
            writer.writeTimestamp(8, Timestamp.fromLocalDateTime(LocalDateTime.MAX), 6);
            writer.writeTimestamp(9, Timestamp.fromLocalDateTime(LocalDateTime.MAX), 6);
            writer.writeDecimal(10, Decimal.zero(10, 5), 10);
            writer.complete();
        }

        BinaryRow partition2 = new BinaryRow(11);
        {
            BinaryRowWriter writer = new BinaryRowWriter(partition2);
            writer.writeBoolean(0, false);
            writer.writeByte(1, Byte.MIN_VALUE);
            writer.writeShort(2, Short.MIN_VALUE);
            writer.writeInt(3, Integer.MIN_VALUE);
            writer.writeLong(4, Long.MIN_VALUE);
            writer.writeFloat(5, Float.MIN_VALUE);
            writer.writeDouble(6, Double.MIN_VALUE);
            writer.writeString(7, BinaryString.fromString("char2"));
            writer.writeTimestamp(8, Timestamp.fromLocalDateTime(LocalDateTime.MIN), 6);
            writer.writeTimestamp(9, Timestamp.fromLocalDateTime(LocalDateTime.MIN), 6);
            writer.writeDecimal(10, Decimal.fromUnscaledLong(10000, 10, 5), 10);
            writer.complete();
        }

        List<Split> splits =
                Arrays.asList(
                        new DataSplit() {
                            @Override
                            public BinaryRow partition() {
                                return partition1;
                            }
                        },
                        new DataSplit() {
                            @Override
                            public BinaryRow partition() {
                                return partition2;
                            }
                        });

        List<TrinoColumnHandle> trinoColumnHandles =
                partitionType.getFields().stream()
                        .map(dataField -> TrinoColumnHandle.of(dataField.name(), dataField.type()))
                        .collect(Collectors.toList());

        BiConsumer<TrinoColumnHandle, Predicate<Object>> checker =
                (trinoColumnHandle, predicate) -> {
                    Constraint constraint =
                            new Constraint(
                                    TupleDomain.all(),
                                    columnHandleNullableValueMap ->
                                            predicate.test(
                                                    columnHandleNullableValueMap
                                                            .get(trinoColumnHandle)
                                                            .getValue()),
                                    Set.of(trinoColumnHandle));
                    List<Split> filteredSplits =
                            TrinoSplitManager.filterByPartition(
                                    constraint, trinoColumnHandles, partitionType, splits);
                    assertThat(filteredSplits.size()).isEqualTo(1);
                };

        // test boolean
        checker.accept(trinoColumnHandles.get(0), value -> value.equals(true));

        // test tinyint
        checker.accept(trinoColumnHandles.get(1), value -> (long) value == 127);

        // test smallint
        checker.accept(trinoColumnHandles.get(2), value -> (long) value > 0);

        // test int
        checker.accept(trinoColumnHandles.get(3), value -> (long) value > 0);

        // test bigint
        checker.accept(trinoColumnHandles.get(4), value -> (long) value > 0);

        // test float
        checker.accept(
                trinoColumnHandles.get(5),
                value -> (long) value == floatToIntBits(Float.MAX_VALUE));

        // test double
        checker.accept(trinoColumnHandles.get(6), value -> (double) value == Double.MAX_VALUE);

        // test char
        checker.accept(trinoColumnHandles.get(7), value -> value.equals(Slices.utf8Slice("char1")));

        // test timestamp
        checker.accept(trinoColumnHandles.get(8), value -> (long) value > 0);

        // test timestamp with local time zone
        checker.accept(trinoColumnHandles.get(9), value -> (long) value == 1829587348619264L);

        // test decimal
        checker.accept(trinoColumnHandles.get(10), value -> (long) value == 0);
    }
}
