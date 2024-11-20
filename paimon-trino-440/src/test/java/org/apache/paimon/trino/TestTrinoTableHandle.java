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

import io.airlift.json.JsonCodec;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoTableHandle}. */
public class TestTrinoTableHandle {

    private final JsonCodec<TrinoTableHandle> codec = JsonCodec.jsonCodec(TrinoTableHandle.class);

    @Test
    public void testPrestoTableHandle() throws Exception {
        TrinoTableHandle expected =
                new TrinoTableHandle(
                        "test",
                        "user",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        OptionalLong.empty());
        testRoundTrip(expected);
    }

    private void testRoundTrip(TrinoTableHandle expected) {
        String json = codec.toJson(expected);
        TrinoTableHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getSchemaName()).isEqualTo(expected.getSchemaName());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getFilter()).isEqualTo(expected.getFilter());
        assertThat(actual.getProjectedColumns()).isEqualTo(expected.getProjectedColumns());
    }
}
