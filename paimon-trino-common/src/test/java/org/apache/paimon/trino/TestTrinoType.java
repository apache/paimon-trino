/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoTypeUtils}. */
public class TestTrinoType {

    @Test
    public void testFromPaimonType() {
        Type charType = TrinoTypeUtils.fromPaimonType(DataTypes.CHAR(1));
        assertThat(Objects.requireNonNull(charType).getDisplayName()).isEqualTo("char(1)");

        Type varCharType = TrinoTypeUtils.fromPaimonType(DataTypes.VARCHAR(10));
        assertThat(Objects.requireNonNull(varCharType).getDisplayName()).isEqualTo("varchar(10)");

        Type booleanType = TrinoTypeUtils.fromPaimonType(DataTypes.BOOLEAN());
        assertThat(Objects.requireNonNull(booleanType).getDisplayName()).isEqualTo("boolean");

        Type binaryType = TrinoTypeUtils.fromPaimonType(DataTypes.BINARY(10));
        assertThat(Objects.requireNonNull(binaryType).getDisplayName()).isEqualTo("varbinary");

        Type varBinaryType = TrinoTypeUtils.fromPaimonType(DataTypes.VARBINARY(10));
        assertThat(Objects.requireNonNull(varBinaryType).getDisplayName()).isEqualTo("varbinary");

        assertThat(TrinoTypeUtils.fromPaimonType(DataTypes.DECIMAL(38, 0)).getDisplayName())
                .isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(TrinoTypeUtils.fromPaimonType(decimal).getDisplayName())
                .isEqualTo("decimal(2,2)");

        Type tinyIntType = TrinoTypeUtils.fromPaimonType(DataTypes.TINYINT());
        assertThat(Objects.requireNonNull(tinyIntType).getDisplayName()).isEqualTo("tinyint");

        Type smallIntType = TrinoTypeUtils.fromPaimonType(DataTypes.SMALLINT());
        assertThat(Objects.requireNonNull(smallIntType).getDisplayName()).isEqualTo("smallint");

        Type intType = TrinoTypeUtils.fromPaimonType(DataTypes.INT());
        assertThat(Objects.requireNonNull(intType).getDisplayName()).isEqualTo("integer");

        Type bigIntType = TrinoTypeUtils.fromPaimonType(DataTypes.BIGINT());
        assertThat(Objects.requireNonNull(bigIntType).getDisplayName()).isEqualTo("bigint");

        Type floatType = TrinoTypeUtils.fromPaimonType(DataTypes.FLOAT());
        assertThat(Objects.requireNonNull(floatType).getDisplayName()).isEqualTo("real");

        Type doubleType = TrinoTypeUtils.fromPaimonType(DataTypes.DOUBLE());
        assertThat(Objects.requireNonNull(doubleType).getDisplayName()).isEqualTo("double");

        Type dateType = TrinoTypeUtils.fromPaimonType(DataTypes.DATE());
        assertThat(Objects.requireNonNull(dateType).getDisplayName()).isEqualTo("date");

        Type timeType = TrinoTypeUtils.fromPaimonType(new TimeType());
        assertThat(Objects.requireNonNull(timeType).getDisplayName()).isEqualTo("time(3)");

        Type timestampType6 = TrinoTypeUtils.fromPaimonType(DataTypes.TIMESTAMP());
        assertThat(Objects.requireNonNull(timestampType6).getDisplayName())
                .isEqualTo("timestamp(6)");

        Type timestampType0 =
                TrinoTypeUtils.fromPaimonType(new org.apache.paimon.types.TimestampType(0));
        assertThat(Objects.requireNonNull(timestampType0).getDisplayName())
                .isEqualTo("timestamp(0)");

        Type localZonedTimestampType =
                TrinoTypeUtils.fromPaimonType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        assertThat(Objects.requireNonNull(localZonedTimestampType).getDisplayName())
                .isEqualTo("timestamp(3) with time zone");

        Type arrayType = TrinoTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(Objects.requireNonNull(arrayType).getDisplayName()).isEqualTo("array(varchar)");

        Type multisetType = TrinoTypeUtils.fromPaimonType(DataTypes.MULTISET(DataTypes.STRING()));
        assertThat(Objects.requireNonNull(multisetType).getDisplayName())
                .isEqualTo("map(varchar, integer)");

        Type mapType =
                TrinoTypeUtils.fromPaimonType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()));
        assertThat(Objects.requireNonNull(mapType).getDisplayName())
                .isEqualTo("map(bigint, varchar)");

        Type row =
                TrinoTypeUtils.fromPaimonType(
                        DataTypes.ROW(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))));
        assertThat(Objects.requireNonNull(row).getDisplayName())
                .isEqualTo("row(id integer, name varchar)");
    }

    @Test
    public void testToPaimonType() {
        DataType charType = TrinoTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType =
                TrinoTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("VARCHAR(2147483646)");

        DataType booleanType = TrinoTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType varbinaryType = TrinoTypeUtils.toPaimonType(VarbinaryType.VARBINARY);
        assertThat(varbinaryType.asSQLString()).isEqualTo("BYTES");

        DataType decimalType = TrinoTypeUtils.toPaimonType(DecimalType.createDecimalType(2, 2));
        assertThat(decimalType.asSQLString()).isEqualTo("DECIMAL(2, 2)");

        DataType tinyintType = TrinoTypeUtils.toPaimonType(TinyintType.TINYINT);
        assertThat(tinyintType.asSQLString()).isEqualTo("TINYINT");

        DataType smallintType = TrinoTypeUtils.toPaimonType(SmallintType.SMALLINT);
        assertThat(smallintType.asSQLString()).isEqualTo("SMALLINT");

        DataType intType = TrinoTypeUtils.toPaimonType(IntegerType.INTEGER);
        assertThat(intType.asSQLString()).isEqualTo("INT");

        DataType bigintType = TrinoTypeUtils.toPaimonType(BigintType.BIGINT);
        assertThat(bigintType.asSQLString()).isEqualTo("BIGINT");

        DataType floatType = TrinoTypeUtils.toPaimonType(RealType.REAL);
        assertThat(floatType.asSQLString()).isEqualTo("FLOAT");

        DataType doubleType = TrinoTypeUtils.toPaimonType(DoubleType.DOUBLE);
        assertThat(doubleType.asSQLString()).isEqualTo("DOUBLE");

        DataType dateType = TrinoTypeUtils.toPaimonType(DateType.DATE);
        assertThat(dateType.asSQLString()).isEqualTo("DATE");

        DataType timeType = TrinoTypeUtils.toPaimonType(io.trino.spi.type.TimeType.TIME_MILLIS);
        assertThat(timeType.asSQLString()).isEqualTo("TIME(0)");

        DataType timestampType0 = TrinoTypeUtils.toPaimonType(TimestampType.TIMESTAMP_SECONDS);
        assertThat(timestampType0.asSQLString()).isEqualTo("TIMESTAMP(0)");

        DataType timestampType3 = TrinoTypeUtils.toPaimonType(TimestampType.TIMESTAMP_MILLIS);
        assertThat(timestampType3.asSQLString()).isEqualTo("TIMESTAMP(3)");

        DataType timestampType6 = TrinoTypeUtils.toPaimonType(TimestampType.TIMESTAMP_MICROS);
        assertThat(timestampType6.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampWithTimeZoneType =
                TrinoTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
        assertThat(timestampWithTimeZoneType.asSQLString())
                .isEqualTo("TIMESTAMP(6) WITH LOCAL TIME ZONE");

        DataType arrayType = TrinoTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        DataType mapType =
                TrinoTypeUtils.toPaimonType(
                        new MapType(
                                IntegerType.INTEGER,
                                VarcharType.createUnboundedVarcharType(),
                                new TypeOperators()));
        assertThat(mapType.asSQLString()).isEqualTo("MAP<INT, VARCHAR(2147483646)>");

        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(java.util.Optional.of("id"), IntegerType.INTEGER));
        fields.add(
                new RowType.Field(
                        java.util.Optional.of("name"), VarcharType.createUnboundedVarcharType()));
        Type type = RowType.from(fields);
        DataType rowType = TrinoTypeUtils.toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` VARCHAR(2147483646)>");
    }
}
