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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import io.airlift.slice.Slice;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;

/** PrestoSql filter to flink predicate. */
public class PrestoSqlFilterConverter {

    private final RowType rowType;
    private final PredicateBuilder builder;

    public PrestoSqlFilterConverter(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<PrestoSqlColumnHandle> tupleDomain) {
        if (tupleDomain.isAll()) {
            // TODO alwaysTrue
            return Optional.empty();
        }

        if (!tupleDomain.getDomains().isPresent()) {
            // TODO alwaysFalse
            return Optional.empty();
        }

        Map<PrestoSqlColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        for (Map.Entry<PrestoSqlColumnHandle, Domain> entry : domainMap.entrySet()) {
            PrestoSqlColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            int index = fieldNames.indexOf(columnHandle.getColumnName());
            if (index != -1) {
                try {
                    conjuncts.add(toPredicate(index, columnHandle.getPrestoSqlType(), domain));
                } catch (UnsupportedOperationException ignored) {
                }
            }
        }

        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(and(conjuncts));
    }

    private Predicate toPredicate(int columnIndex, Type type, Domain domain) {
        if (domain.isAll()) {
            // TODO alwaysTrue
            throw new UnsupportedOperationException();
        }
        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return builder.isNull((columnIndex));
            }
            // TODO alwaysFalse
            throw new UnsupportedOperationException();
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                // TODO alwaysTrue
                throw new UnsupportedOperationException();
            }
            return builder.isNotNull((columnIndex));
        }

        // TODO support structural types
        if (type instanceof ArrayType
                || type instanceof MapType
                || type instanceof io.prestosql.spi.type.RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException();
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(getLiteralValue(type, getLowBoundedValue(range)));
                } else {
                    predicates.add(toPredicate(columnIndex, range));
                }
            }

            if (!values.isEmpty()) {
                predicates.add(builder.in(columnIndex, values));
            }

            if (domain.isNullAllowed()) {
                predicates.add(builder.isNull(columnIndex));
            }
            return or(predicates);
        }

        throw new UnsupportedOperationException();
    }

    private Predicate toPredicate(int columnIndex, Range range) {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object value = getLiteralValue(type, range.getSingleValue());
            return builder.equal(columnIndex, value);
        }

        List<Predicate> conjuncts = new ArrayList<>(2);
        if (isLowUnbounded(range)) {
            Object low = getLiteralValue(type, getLowBoundedValue(range));
            Predicate lowBound;
            if (isLowInclusive(range)) {
                lowBound = builder.greaterOrEqual(columnIndex, low);
            } else {
                lowBound = builder.greaterThan(columnIndex, low);
            }
            conjuncts.add(lowBound);
        }

        if (isHighUnbounded(range)) {
            Object high = getLiteralValue(type, getHighBoundedValue(range));
            Predicate highBound;
            if (isHighInclusive(range)) {
                highBound = builder.lessOrEqual(columnIndex, high);
            } else {
                highBound = builder.lessThan(columnIndex, high);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }

    private Object getLiteralValue(Type type, Object prestosqlNativeValue) {
        requireNonNull(prestosqlNativeValue, "prestosqlNativeValue is null");

        if (type instanceof BooleanType) {
            return prestosqlNativeValue;
        }

        if (type instanceof IntegerType) {
            return toIntExact((long) prestosqlNativeValue);
        }

        if (type instanceof BigintType) {
            return prestosqlNativeValue;
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) prestosqlNativeValue));
        }

        if (type instanceof DoubleType) {
            return prestosqlNativeValue;
        }

        if (type instanceof DateType) {
            return toIntExact(((Long) prestosqlNativeValue));
        }

        if (type instanceof TimeType) {
            return toIntExact(((Long) prestosqlNativeValue));
        }

        if (type instanceof TimestampType) {
            return Timestamp.fromEpochMillis((long) prestosqlNativeValue);
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return Timestamp.fromEpochMillis(unpackMillisUtc((Long) prestosqlNativeValue));
        }

        if (type instanceof VarcharType) {
            return BinaryString.fromBytes(((Slice) prestosqlNativeValue).getBytes());
        }

        if (type instanceof VarbinaryType) {
            return ((Slice) prestosqlNativeValue).getBytes();
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal bigDecimal;
            if (prestosqlNativeValue instanceof Long) {
                bigDecimal =
                        BigDecimal.valueOf((long) prestosqlNativeValue)
                                .movePointLeft(decimalType.getScale());
            } else {
                bigDecimal =
                        new BigDecimal(
                                DecimalUtils.toBigInteger(prestosqlNativeValue),
                                decimalType.getScale());
            }
            return Decimal.fromBigDecimal(
                    bigDecimal, decimalType.getPrecision(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private Object getHighBoundedValue(Range range) {
        return range.getHigh().getValue();
    }

    private Object getLowBoundedValue(Range range) {
        return range.getLow().getValue();
    }

    private boolean isHighInclusive(Range range) {
        Marker marker = range.getHigh();
        return marker.getValueBlock().isPresent() && marker.getBound() == Marker.Bound.EXACTLY;
    }

    private boolean isLowInclusive(Range range) {
        Marker marker = range.getLow();
        return marker.getValueBlock().isPresent() && marker.getBound() == Marker.Bound.EXACTLY;
    }

    private boolean isLowUnbounded(Range range) {
        return range.getLow().getValueBlock().isPresent();
    }

    private boolean isHighUnbounded(Range range) {
        return range.getHigh().getValueBlock().isPresent();
    }
}
