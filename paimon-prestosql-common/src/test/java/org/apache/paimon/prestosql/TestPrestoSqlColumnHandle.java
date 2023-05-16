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

import org.apache.paimon.types.DataTypes;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.type.Type;
import io.prestosql.type.TypeDeserializer;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link PrestoSqlColumnHandle}. */
public class TestPrestoSqlColumnHandle {

    @Test
    public void testPrestoSqlColumnHandle() {
        PrestoSqlColumnHandle expected = PrestoSqlColumnHandle.of("name", DataTypes.STRING(), null);
        testRoundTrip(expected);
    }

    private void testRoundTrip(PrestoSqlColumnHandle expected) {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        Metadata metadata = MetadataManager.createTestMetadataManager();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(Type.class, new TypeDeserializer(metadata)));
        JsonCodec<PrestoSqlColumnHandle> codec =
                new JsonCodecFactory(objectMapperProvider).jsonCodec(PrestoSqlColumnHandle.class);

        String json = codec.toJson(expected);
        PrestoSqlColumnHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getColumnName()).isEqualTo(expected.getColumnName());
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
        assertThat(actual.getPrestoSqlType()).isEqualTo(expected.getPrestoSqlType());
    }
}
