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

package org.apache.paimon.trino.fileio;

import org.apache.paimon.fs.Path;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TrinoFileIO}. */
public class TrinoFileIOTest {

    @Test
    void testAtomicWriteUsesExclusiveCreateForObjectStore() throws Exception {
        TestingOutputFile outputFile = new TestingOutputFile();
        TrinoFileSystem fileSystem =
                (TrinoFileSystem)
                        Proxy.newProxyInstance(
                                TrinoFileSystem.class.getClassLoader(),
                                new Class<?>[] {TrinoFileSystem.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("newOutputFile")) {
                                        return outputFile;
                                    }
                                    throw new UnsupportedOperationException(method.getName());
                                });
        TrinoFileIO fileIO = new TrinoFileIO(fileSystem, new Path("s3://bucket/warehouse"));
        Path schemaPath = new Path("s3://bucket/warehouse/test.db/table/schema/schema-0");

        assertThat(fileIO.tryToWriteAtomic(schemaPath, "schema-v0")).isTrue();
        assertThat(outputFile.content()).isEqualTo("schema-v0");
        assertThat(fileIO.tryToWriteAtomic(schemaPath, "other")).isFalse();
        assertThat(outputFile.content()).isEqualTo("schema-v0");
    }

    private static class TestingOutputFile implements TrinoOutputFile {

        private byte[] content;

        @Override
        public void createOrOverwrite(byte[] data) {
            content = data.clone();
        }

        @Override
        public void createExclusive(byte[] data) throws IOException {
            if (content != null) {
                throw new FileAlreadyExistsException(location().toString());
            }
            content = data.clone();
        }

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Location location() {
            return Location.of("s3://bucket/warehouse/test.db/table/schema/schema-0");
        }

        private String content() {
            return new String(content, StandardCharsets.UTF_8);
        }
    }
}
