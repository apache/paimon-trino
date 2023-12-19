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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

/** Trino {@link ConnectorMetadata}. */
public class TrinoMetadata extends TrinoMetadataBase {

    @Inject
    public TrinoMetadata(Options catalogOptions, Configuration configuration) {
        super(catalogOptions, configuration);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent()) {
            throw new TrinoException(
                    NOT_SUPPORTED, "Read paimon table with start version is not supported");
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        if (endVersion.isPresent()) {
            ConnectorTableVersion version = endVersion.get();
            Type versionType = version.getVersionType();
            switch (version.getPointerType()) {
                case TEMPORAL:
                    {
                        if (!(versionType instanceof TimestampWithTimeZoneType)) {
                            throw new TrinoException(
                                    NOT_SUPPORTED,
                                    "Unsupported type for table version: "
                                            + versionType.getDisplayName());
                        }
                        TimestampWithTimeZoneType timeZonedVersionType =
                                (TimestampWithTimeZoneType) versionType;
                        long epochMillis =
                                timeZonedVersionType.isShort()
                                        ? unpackMillisUtc((long) version.getVersion())
                                        : ((LongTimestampWithTimeZone) version.getVersion())
                                                .getEpochMillis();
                        dynamicOptions.put(
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                String.valueOf(epochMillis));
                        break;
                    }
                case TARGET_ID:
                    {
                        dynamicOptions.put(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                version.getVersion().toString());
                        break;
                    }
            }
        }
        return getTableHandle(tableName, dynamicOptions);
    }

    @Override
    public void setTableProperties(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<String, Optional<Object>> properties) {
        TrinoTableHandle trinoTableHandle = (TrinoTableHandle) tableHandle;
        Identifier identifier =
                new Identifier(trinoTableHandle.getSchemaName(), trinoTableHandle.getTableName());
        List<SchemaChange> changes = new ArrayList<>();
        Map<String, String> options =
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> (String) e.getValue().get()));
        options.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        // TODO: remove options, SET PROPERTIES x = DEFAULT
        try {
            catalog.alterTable(identifier, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    format("failed to alter table: '%s'", trinoTableHandle.getTableName()), e);
        }
    }
}
