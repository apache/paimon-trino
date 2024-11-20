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
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoSplit}. */
public class TestTrinoSplit {

    private final JsonCodec<TrinoSplit> codec = JsonCodec.jsonCodec(TrinoSplit.class);

    @Test
    public void testJsonRoundTrip() throws Exception {
        byte[] serializedTable = TrinoTestUtils.getSerializedTable();
        TrinoSplit expected = new TrinoSplit(Arrays.toString(serializedTable), 0.1);
        String json = codec.toJson(expected);
        TrinoSplit actual = codec.fromJson(json);
        assertThat(actual.getSplitSerialized()).isEqualTo(expected.getSplitSerialized());
    }
}
