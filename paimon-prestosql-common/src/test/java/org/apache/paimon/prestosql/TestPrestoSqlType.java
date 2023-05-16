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

package org.apache.paimon.prestosql;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;

import com.google.common.collect.Lists;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.TestingConnectorContext;
import io.prestosql.type.MapParametricType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrestoSqlTypeUtils}. */
public class TestPrestoSqlType {

    @Test
    public void testFromPaimonType() {
        TypeManager typeManager = new TestingConnectorContext().getTypeManager();
        Type charType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.CHAR(1), typeManager);
        assertThat(Objects.requireNonNull(charType).getDisplayName()).isEqualTo("char(1)");

        Type varCharType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.VARCHAR(10), typeManager);
        assertThat(Objects.requireNonNull(varCharType).getDisplayName()).isEqualTo("varchar(10)");

        Type booleanType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.BOOLEAN(), typeManager);
        assertThat(Objects.requireNonNull(booleanType).getDisplayName()).isEqualTo("boolean");

        Type binaryType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.BINARY(10), typeManager);
        assertThat(Objects.requireNonNull(binaryType).getDisplayName()).isEqualTo("varbinary");

        Type varBinaryType =
                PrestoSqlTypeUtils.fromPaimonType(DataTypes.VARBINARY(10), typeManager);
        assertThat(Objects.requireNonNull(varBinaryType).getDisplayName()).isEqualTo("varbinary");

        assertThat(
                        PrestoSqlTypeUtils.fromPaimonType(DataTypes.DECIMAL(38, 0), typeManager)
                                .getDisplayName())
                .isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(PrestoSqlTypeUtils.fromPaimonType(decimal, typeManager).getDisplayName())
                .isEqualTo("decimal(2,2)");

        Type tinyIntType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.TINYINT(), typeManager);
        assertThat(Objects.requireNonNull(tinyIntType).getDisplayName()).isEqualTo("tinyint");

        Type smallIntType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.SMALLINT(), typeManager);
        assertThat(Objects.requireNonNull(smallIntType).getDisplayName()).isEqualTo("smallint");

        Type intType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.INT(), typeManager);
        assertThat(Objects.requireNonNull(intType).getDisplayName()).isEqualTo("integer");

        Type bigIntType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.BIGINT(), typeManager);
        assertThat(Objects.requireNonNull(bigIntType).getDisplayName()).isEqualTo("bigint");

        Type floatType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.FLOAT(), typeManager);
        assertThat(Objects.requireNonNull(floatType).getDisplayName()).isEqualTo("real");

        Type doubleType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.DOUBLE(), typeManager);
        assertThat(Objects.requireNonNull(doubleType).getDisplayName()).isEqualTo("double");

        Type dateType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.DATE(), typeManager);
        assertThat(Objects.requireNonNull(dateType).getDisplayName()).isEqualTo("date");

        Type timeType = PrestoSqlTypeUtils.fromPaimonType(new TimeType(), typeManager);
        assertThat(Objects.requireNonNull(timeType).getDisplayName()).isEqualTo("time");

        Type timestampType = PrestoSqlTypeUtils.fromPaimonType(DataTypes.TIMESTAMP(), typeManager);
        assertThat(Objects.requireNonNull(timestampType).getDisplayName()).isEqualTo("timestamp");

        Type localZonedTimestampType =
                PrestoSqlTypeUtils.fromPaimonType(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), typeManager);
        assertThat(Objects.requireNonNull(localZonedTimestampType).getDisplayName())
                .isEqualTo("timestamp with time zone");

        Type arrayType =
                PrestoSqlTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.STRING()), typeManager);
        assertThat(Objects.requireNonNull(arrayType).getDisplayName()).isEqualTo("array(varchar)");

        Type multisetType =
                PrestoSqlTypeUtils.fromPaimonType(
                        DataTypes.MULTISET(DataTypes.STRING()), typeManager);
        assertThat(Objects.requireNonNull(multisetType).getDisplayName())
                .isEqualTo("map(varchar, integer)");

        Type mapType =
                PrestoSqlTypeUtils.fromPaimonType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()), typeManager);
        assertThat(Objects.requireNonNull(mapType).getDisplayName())
                .isEqualTo("map(bigint, varchar)");

        Type row =
                PrestoSqlTypeUtils.fromPaimonType(
                        DataTypes.ROW(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))),
                        typeManager);
        assertThat(Objects.requireNonNull(row).getDisplayName())
                .isEqualTo("row(id integer, name varchar)");
    }

    @Test
    public void testToPaimonType() {
        DataType charType = PrestoSqlTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType =
                PrestoSqlTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("VARCHAR(2147483646)");

        DataType booleanType = PrestoSqlTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType varbinaryType = PrestoSqlTypeUtils.toPaimonType(VarbinaryType.VARBINARY);
        assertThat(varbinaryType.asSQLString()).isEqualTo("BYTES");

        DataType decimalType = PrestoSqlTypeUtils.toPaimonType(DecimalType.createDecimalType(2, 2));
        assertThat(decimalType.asSQLString()).isEqualTo("DECIMAL(2, 2)");

        DataType tinyintType = PrestoSqlTypeUtils.toPaimonType(TinyintType.TINYINT);
        assertThat(tinyintType.asSQLString()).isEqualTo("TINYINT");

        DataType smallintType = PrestoSqlTypeUtils.toPaimonType(SmallintType.SMALLINT);
        assertThat(smallintType.asSQLString()).isEqualTo("SMALLINT");

        DataType intType = PrestoSqlTypeUtils.toPaimonType(IntegerType.INTEGER);
        assertThat(intType.asSQLString()).isEqualTo("INT");

        DataType bigintType = PrestoSqlTypeUtils.toPaimonType(BigintType.BIGINT);
        assertThat(bigintType.asSQLString()).isEqualTo("BIGINT");

        DataType floatType = PrestoSqlTypeUtils.toPaimonType(RealType.REAL);
        assertThat(floatType.asSQLString()).isEqualTo("FLOAT");

        DataType doubleType = PrestoSqlTypeUtils.toPaimonType(DoubleType.DOUBLE);
        assertThat(doubleType.asSQLString()).isEqualTo("DOUBLE");

        DataType dateType = PrestoSqlTypeUtils.toPaimonType(DateType.DATE);
        assertThat(dateType.asSQLString()).isEqualTo("DATE");

        DataType timeType = PrestoSqlTypeUtils.toPaimonType(io.prestosql.spi.type.TimeType.TIME);
        assertThat(timeType.asSQLString()).isEqualTo("TIME(0)");

        DataType timestampType = PrestoSqlTypeUtils.toPaimonType(TimestampType.TIMESTAMP);
        assertThat(timestampType.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampWithTimeZoneType =
                PrestoSqlTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(timestampWithTimeZoneType.asSQLString())
                .isEqualTo("TIMESTAMP(6) WITH LOCAL TIME ZONE");

        DataType arrayType = PrestoSqlTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        TypeManager typeManager = new TestingConnectorContext().getTypeManager();

        List<TypeParameter> parameters =
                Lists.newArrayList(
                        TypeParameter.of(IntegerType.INTEGER),
                        TypeParameter.of(VarcharType.createUnboundedVarcharType()));
        MapType mapType = (MapType) new MapParametricType().createType(typeManager, parameters);

        DataType toMapType = PrestoSqlTypeUtils.toPaimonType(mapType);
        assertThat(toMapType.asSQLString()).isEqualTo("MAP<INT, VARCHAR(2147483646)>");

        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(java.util.Optional.of("id"), IntegerType.INTEGER));
        fields.add(
                new RowType.Field(
                        java.util.Optional.of("name"), VarcharType.createUnboundedVarcharType()));
        Type type = RowType.from(fields);
        DataType rowType = PrestoSqlTypeUtils.toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` VARCHAR(2147483646)>");
    }
}
