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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** PrestoSql type from Paimon Type. */
public class PrestoSqlTypeUtils {

    public static Type fromPaimonType(DataType type, TypeManager typeManager) {
        return type.accept(PaimonToPrestoSqlTypeVistor.INSTANCE.setTypeManager(typeManager));
    }

    public static DataType toPaimonType(Type prestoSqlType) {
        return PrestoSqlToPaimonTypeVistor.INSTANCE.visit(prestoSqlType);
    }

    private static class PaimonToPrestoSqlTypeVistor extends DataTypeDefaultVisitor<Type> {

        private static final PaimonToPrestoSqlTypeVistor INSTANCE =
                new PaimonToPrestoSqlTypeVistor();
        private TypeManager typeManager;

        public PaimonToPrestoSqlTypeVistor setTypeManager(TypeManager typeManager) {
            this.typeManager = typeManager;
            return this;
        }

        @Override
        public Type visit(CharType charType) {
            return io.prestosql.spi.type.CharType.createCharType(
                    Math.min(io.prestosql.spi.type.CharType.MAX_LENGTH, charType.getLength()));
        }

        @Override
        public Type visit(VarCharType varCharType) {
            if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                return VarcharType.createUnboundedVarcharType();
            }
            return VarcharType.createVarcharType(
                    Math.min(VarcharType.MAX_LENGTH, varCharType.getLength()));
        }

        @Override
        public Type visit(BooleanType booleanType) {
            return io.prestosql.spi.type.BooleanType.BOOLEAN;
        }

        @Override
        public Type visit(BinaryType binaryType) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(VarBinaryType varBinaryType) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(DecimalType decimalType) {
            return io.prestosql.spi.type.DecimalType.createDecimalType(
                    decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public Type visit(TinyIntType tinyIntType) {
            return TinyintType.TINYINT;
        }

        @Override
        public Type visit(SmallIntType smallIntType) {
            return SmallintType.SMALLINT;
        }

        @Override
        public Type visit(IntType intType) {
            return IntegerType.INTEGER;
        }

        @Override
        public Type visit(BigIntType bigIntType) {
            return BigintType.BIGINT;
        }

        @Override
        public Type visit(FloatType floatType) {
            return RealType.REAL;
        }

        @Override
        public Type visit(DoubleType doubleType) {
            return io.prestosql.spi.type.DoubleType.DOUBLE;
        }

        @Override
        public Type visit(DateType dateType) {
            return io.prestosql.spi.type.DateType.DATE;
        }

        @Override
        public Type visit(TimeType timeType) {
            return io.prestosql.spi.type.TimeType.TIME;
        }

        @Override
        public Type visit(TimestampType timestampType) {
            return io.prestosql.spi.type.TimestampType.TIMESTAMP;
        }

        @Override
        public Type visit(LocalZonedTimestampType localZonedTimestampType) {
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        }

        @Override
        public Type visit(ArrayType arrayType) {
            DataType elementType = arrayType.getElementType();
            return new io.prestosql.spi.type.ArrayType(elementType.accept(this));
        }

        @Override
        public Type visit(MultisetType multisetType) {
            return new MapType(multisetType.getElementType(), new IntType()).accept(this);
        }

        @Override
        public Type visit(MapType mapType) {
            TypeSignature keyType =
                    Objects.requireNonNull(fromPaimonType(mapType.getKeyType(), typeManager))
                            .getTypeSignature();
            TypeSignature valueType =
                    Objects.requireNonNull(fromPaimonType(mapType.getValueType(), typeManager))
                            .getTypeSignature();
            return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(
                            TypeSignatureParameter.typeParameter(keyType),
                            TypeSignatureParameter.typeParameter(valueType)));
        }

        @Override
        public Type visit(RowType rowType) {
            List<io.prestosql.spi.type.RowType.Field> fields =
                    rowType.getFields().stream()
                            .map(
                                    field ->
                                            io.prestosql.spi.type.RowType.field(
                                                    field.name(), field.type().accept(this)))
                            .collect(Collectors.toList());
            return io.prestosql.spi.type.RowType.from(fields);
        }

        @Override
        protected Type defaultMethod(DataType logicalType) {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }

    private static class PrestoSqlToPaimonTypeVistor {

        private static final PrestoSqlToPaimonTypeVistor INSTANCE =
                new PrestoSqlToPaimonTypeVistor();

        private final AtomicInteger currentIndex = new AtomicInteger(0);

        public DataType visit(Type prestosqlType) {
            if (prestosqlType instanceof io.prestosql.spi.type.CharType) {
                return DataTypes.CHAR(
                        Math.min(
                                io.prestosql.spi.type.CharType.MAX_LENGTH,
                                ((io.prestosql.spi.type.CharType) prestosqlType).getLength()));
            } else if (prestosqlType instanceof VarcharType) {
                Optional<Integer> length = ((VarcharType) prestosqlType).getLength();
                if (length.isPresent()) {
                    return DataTypes.VARCHAR(
                            Math.min(
                                    VarcharType.MAX_LENGTH,
                                    ((VarcharType) prestosqlType).getBoundedLength()));
                }
                return DataTypes.VARCHAR(VarcharType.MAX_LENGTH);
            } else if (prestosqlType instanceof io.prestosql.spi.type.BooleanType) {
                return DataTypes.BOOLEAN();
            } else if (prestosqlType instanceof VarbinaryType) {
                return DataTypes.VARBINARY(Integer.MAX_VALUE);
            } else if (prestosqlType instanceof io.prestosql.spi.type.DecimalType) {
                return DataTypes.DECIMAL(
                        ((io.prestosql.spi.type.DecimalType) prestosqlType).getPrecision(),
                        ((io.prestosql.spi.type.DecimalType) prestosqlType).getScale());
            } else if (prestosqlType instanceof TinyintType) {
                return DataTypes.TINYINT();
            } else if (prestosqlType instanceof SmallintType) {
                return DataTypes.SMALLINT();
            } else if (prestosqlType instanceof IntegerType) {
                return DataTypes.INT();
            } else if (prestosqlType instanceof BigintType) {
                return DataTypes.BIGINT();
            } else if (prestosqlType instanceof RealType) {
                return DataTypes.FLOAT();
            } else if (prestosqlType instanceof io.prestosql.spi.type.DoubleType) {
                return DataTypes.DOUBLE();
            } else if (prestosqlType instanceof io.prestosql.spi.type.DateType) {
                return DataTypes.DATE();
            } else if (prestosqlType instanceof io.prestosql.spi.type.TimeType) {
                return new TimeType();
            } else if (prestosqlType instanceof io.prestosql.spi.type.TimestampType) {
                return DataTypes.TIMESTAMP();
            } else if (prestosqlType instanceof TimestampWithTimeZoneType) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            } else if (prestosqlType instanceof io.prestosql.spi.type.ArrayType) {
                return DataTypes.ARRAY(
                        visit(((io.prestosql.spi.type.ArrayType) prestosqlType).getElementType()));
            } else if (prestosqlType instanceof io.prestosql.spi.type.MapType) {
                return DataTypes.MAP(
                        visit(((io.prestosql.spi.type.MapType) prestosqlType).getKeyType()),
                        visit(((io.prestosql.spi.type.MapType) prestosqlType).getValueType()));
            } else if (prestosqlType instanceof io.prestosql.spi.type.RowType) {
                io.prestosql.spi.type.RowType rowType =
                        (io.prestosql.spi.type.RowType) prestosqlType;
                List<DataField> dataFields =
                        rowType.getFields().stream()
                                .map(
                                        field ->
                                                new DataField(
                                                        currentIndex.getAndIncrement(),
                                                        field.getName().get(),
                                                        visit(field.getType())))
                                .collect(Collectors.toList());
                return new RowType(true, dataFields);
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + prestosqlType);
            }
        }
    }
}
