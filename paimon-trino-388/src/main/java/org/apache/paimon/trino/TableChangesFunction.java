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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.InstantiationUtil;

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/** TableChangesFunction. */
public class TableChangesFunction extends AbstractConnectorTableFunction {

    private static final Slice INVALID_VALUE = Slices.utf8Slice("invalid");
    private static final String FUNCTION_NAME = "table_changes";
    private static final String SCHEMA_NAME_VAR_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME_VAR_NAME = "TABLE_NAME";
    private static final String INCREMENTAL_BETWEEN_SCAN_MODE =
            TrinoTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key())
                    .toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN_TIMESTAMP =
            TrinoTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key())
                    .toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN =
            TrinoTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN.key())
                    .toUpperCase(ENGLISH);
    private final TrinoMetadata trinoMetadata;

    @Inject
    public TableChangesFunction(TrinoMetadataFactory trinoMetadataFactory) {
        super(
                "system",
                FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(TABLE_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INCREMENTAL_BETWEEN_SCAN_MODE)
                                .defaultValue(
                                        Slices.utf8Slice(
                                                CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE
                                                        .defaultValue()
                                                        .toString()))
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INCREMENTAL_BETWEEN)
                                .defaultValue(INVALID_VALUE)
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(INCREMENTAL_BETWEEN_TIMESTAMP)
                                .defaultValue(INVALID_VALUE)
                                .type(VARCHAR)
                                .build()),
                GENERIC_TABLE);
        this.trinoMetadata =
                requireNonNull(trinoMetadataFactory, "trinoMetadataFactory is null").create();
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments) {
        String schema = getSchemaName(arguments);
        String table = getTableName(arguments);

        Slice incrementalBetweenValue =
                (Slice) ((ScalarArgument) arguments.get(INCREMENTAL_BETWEEN)).getValue();
        Slice incrementalBetweenTimestamp =
                (Slice) ((ScalarArgument) arguments.get(INCREMENTAL_BETWEEN_TIMESTAMP)).getValue();
        if (incrementalBetweenValue.equals(INVALID_VALUE)
                && incrementalBetweenTimestamp.equals(INVALID_VALUE)) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Either "
                            + INCREMENTAL_BETWEEN
                            + " or "
                            + INCREMENTAL_BETWEEN_TIMESTAMP
                            + " must be provided");
        }

        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        try {
            TrinoTableHandle trinoTableHandle =
                    trinoMetadata.getTableHandle(session, schemaTableName);
            Table paimonTable = trinoTableHandle.table();
            Map<String, String> options = new HashMap<>(paimonTable.options());
            if (!incrementalBetweenValue.equals(INVALID_VALUE)) {
                options.put(
                        CoreOptions.INCREMENTAL_BETWEEN.key(),
                        incrementalBetweenValue.toStringUtf8());
            }
            if (!incrementalBetweenTimestamp.equals(INVALID_VALUE)) {
                options.put(
                        CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(),
                        incrementalBetweenTimestamp.toStringUtf8());
            }
            paimonTable = paimonTable.copy(options);

            ImmutableList.Builder<Descriptor.Field> columns = ImmutableList.builder();
            List<ColumnHandle> projectedColumns = new ArrayList<>();
            paimonTable.rowType().getFields().stream()
                    .forEach(
                            column -> {
                                columns.add(
                                        new Descriptor.Field(
                                                column.name(),
                                                Optional.of(
                                                        TrinoTypeUtils.fromPaimonType(
                                                                column.type()))));
                                projectedColumns.add(
                                        TrinoColumnHandle.of(
                                                column.name().toLowerCase(), column.type()));
                            });
            return TableFunctionAnalysis.builder()
                    .returnedType(new Descriptor(columns.build()))
                    .handle(
                            new TrinoTableHandle(
                                    schema,
                                    table,
                                    InstantiationUtil.serializeObject(paimonTable),
                                    TupleDomain.all(),
                                    Optional.of(projectedColumns),
                                    OptionalLong.empty()))
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize table", e);
        }
    }

    private static String getSchemaName(Map<String, Argument> arguments) {
        if (argumentExists(arguments, SCHEMA_NAME_VAR_NAME)) {
            return ((Slice)
                            checkNonNull(
                                    ((ScalarArgument) arguments.get(SCHEMA_NAME_VAR_NAME))
                                            .getValue()))
                    .toStringUtf8();
        }
        throw new TrinoException(
                INVALID_FUNCTION_ARGUMENT, SCHEMA_NAME_VAR_NAME + " argument not found");
    }

    private static String getTableName(Map<String, Argument> arguments) {
        if (argumentExists(arguments, TABLE_NAME_VAR_NAME)) {
            return ((Slice)
                            checkNonNull(
                                    ((ScalarArgument) arguments.get(TABLE_NAME_VAR_NAME))
                                            .getValue()))
                    .toStringUtf8();
        }
        throw new TrinoException(
                INVALID_FUNCTION_ARGUMENT, TABLE_NAME_VAR_NAME + " argument not found");
    }

    private static boolean argumentExists(Map<String, Argument> arguments, String key) {
        Argument argument = arguments.get(key);
        if (argument instanceof ScalarArgument) {
            return !(((ScalarArgument) argument).getValue() == null);
        }
        throw new IllegalArgumentException("Unsupported argument type: " + argument);
    }

    private static Object checkNonNull(Object argumentValue) {
        if (argumentValue == null) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " arguments may not be null");
        }
        return argumentValue;
    }
}
