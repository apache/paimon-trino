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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.types.DataTypes;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.testng.annotations.Test;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoColumnHandle}. */
public class TestTrinoColumnHandle {

    @Test
    public void testTrinoColumnHandle() {
        TrinoColumnHandle expected = TrinoColumnHandle.of("name", DataTypes.STRING());
        testRoundTrip(expected);
    }

    private void testRoundTrip(TrinoColumnHandle expected) {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<TrinoColumnHandle> codec =
                new JsonCodecFactory(objectMapperProvider).jsonCodec(TrinoColumnHandle.class);

        String json = codec.toJson(expected);
        TrinoColumnHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getColumnName()).isEqualTo(expected.getColumnName());
        assertThat(actual.getTypeString()).isEqualTo(expected.getTypeString());
        assertThat(actual.getTrinoType()).isEqualTo(expected.getTrinoType());
    }
}
