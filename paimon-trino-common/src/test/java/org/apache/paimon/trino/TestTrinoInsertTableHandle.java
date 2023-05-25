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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoInsertTableHandle}. */
public class TestTrinoInsertTableHandle {

    @Test
    public void testTrinoTableHandle() throws Exception {
        byte[] serializedTable = TrinoTestUtils.getSerializedTable(true);
        List<DataField> dataFieldList = TrinoTestUtils.DATA_FIELD_LIST;
        List<String> columnNames = new ArrayList<>(dataFieldList.size());
        List<Type> columnTypes = new ArrayList<>(dataFieldList.size());

        for (DataField dataField : dataFieldList) {
            columnNames.add(dataField.name());
            DataType dataType = dataField.type();
            Type type = TrinoTypeUtils.fromPaimonType(dataType);
            columnTypes.add(type);
        }

        TrinoInsertTableHandle trinoInsertTableHandle =
                new TrinoInsertTableHandle(
                        "test", "user", columnNames, columnTypes, serializedTable);
        testRoundTrip(trinoInsertTableHandle);
    }

    private void testRoundTrip(TrinoInsertTableHandle expected) {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<TrinoInsertTableHandle> codec =
                new JsonCodecFactory(objectMapperProvider).jsonCodec(TrinoInsertTableHandle.class);

        String json = codec.toJson(expected);
        TrinoInsertTableHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getSchemaName()).isEqualTo(expected.getSchemaName());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getSerializedTable()).isEqualTo(expected.getSerializedTable());

        for (int i = 0; i < actual.getColumnNames().size(); i++) {
            assertThat(actual.getColumnNames().get(i)).isEqualTo(expected.getColumnNames().get(i));
        }

        for (int i = 0; i < actual.getColumnTypes().size(); i++) {
            assertThat(actual.getColumnTypes().get(i)).isEqualTo(expected.getColumnTypes().get(i));
        }
    }
}
