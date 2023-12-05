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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;
import static org.apache.paimon.trino.TrinoTypeUtils.convertTrinoValueToPaimon;

/**
 * Trino filter to flink predicate.
 */
public class TrinoFilterConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoFilterConverter.class);

    private final RowType rowType;
    private final PredicateBuilder builder;

    public TrinoFilterConverter(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    public Optional<Predicate> convert(TupleDomain<TrinoColumnHandle> tupleDomain) {
        return convert(tupleDomain, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    public Optional<Predicate> convert(
            TupleDomain<TrinoColumnHandle> tupleDomain,
            LinkedHashMap<TrinoColumnHandle, Domain> acceptedDomains,
            LinkedHashMap<TrinoColumnHandle, Domain> unsupportedDomains) {
        if (tupleDomain.isAll()) {
            // TODO alwaysTrue
            return Optional.empty();
        }

        if (tupleDomain.getDomains().isEmpty()) {
            // TODO alwaysFalse
            return Optional.empty();
        }

        Map<TrinoColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        List<Predicate> conjuncts = new ArrayList<>();
        List<String> fieldNames = FieldNameUtils.fieldNames(rowType);
        for (Map.Entry<TrinoColumnHandle, Domain> entry : domainMap.entrySet()) {
            TrinoColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            String field = columnHandle.getColumnName();
            int index = fieldNames.indexOf(field);
            if (index != -1) {
                try {
                    conjuncts.add(toPredicate(index, columnHandle.getTrinoType(), domain));
                    acceptedDomains.put(columnHandle, domain);
                    continue;
                } catch (UnsupportedOperationException exception) {
                    LOG.warn(
                            "Unsupported predicate, maybe the type of column is not supported yet.",
                            exception);
                }
            }
            unsupportedDomains.put(columnHandle, domain);
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
                || type instanceof io.trino.spi.type.RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException();
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> values = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    values.add(convertTrinoValueToPaimon(type, range.getLowBoundedValue()));
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
            Object value = convertTrinoValueToPaimon(type, range.getSingleValue());
            return builder.equal(columnIndex, value);
        }

        List<Predicate> conjuncts = new ArrayList<>(2);
        if (!range.isLowUnbounded()) {
            Object low = convertTrinoValueToPaimon(type, range.getLowBoundedValue());
            Predicate lowBound;
            if (range.isLowInclusive()) {
                lowBound = builder.greaterOrEqual(columnIndex, low);
            } else {
                lowBound = builder.greaterThan(columnIndex, low);
            }
            conjuncts.add(lowBound);
        }

        if (!range.isHighUnbounded()) {
            Object high = convertTrinoValueToPaimon(type, range.getHighBoundedValue());
            Predicate highBound;
            if (range.isHighInclusive()) {
                highBound = builder.lessOrEqual(columnIndex, high);
            } else {
                highBound = builder.lessThan(columnIndex, high);
            }
            conjuncts.add(highBound);
        }

        return and(conjuncts);
    }
}
