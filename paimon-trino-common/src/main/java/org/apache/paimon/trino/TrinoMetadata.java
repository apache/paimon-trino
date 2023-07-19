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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.security.SecurityContext;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

/** Trino {@link ConnectorMetadata}. */
public class TrinoMetadata extends TrinoMetadataBase {

    private final Catalog catalog;

    public TrinoMetadata(Options catalogOptions) {
        super(catalogOptions);
        try {
            SecurityContext.install(catalogOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
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
