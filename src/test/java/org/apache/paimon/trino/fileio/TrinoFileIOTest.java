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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    void testAtomicWriteFallsBackWhenExclusiveCreateIsUnsupported() throws Exception {
        AtomicBoolean exclusiveCreateAttempted = new AtomicBoolean();
        AtomicBoolean renamed = new AtomicBoolean();
        AtomicReference<byte[]> content = new AtomicReference<>();
        TrinoFileSystem fileSystem =
                (TrinoFileSystem)
                        Proxy.newProxyInstance(
                                TrinoFileSystem.class.getClassLoader(),
                                new Class<?>[] {TrinoFileSystem.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "newOutputFile":
                                            return new UnsupportedExclusiveOutputFile(
                                                    (Location) args[0],
                                                    exclusiveCreateAttempted,
                                                    content);
                                        case "directoryExists":
                                            return Optional.empty();
                                        case "renameFile":
                                            renamed.set(true);
                                            return null;
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
        TrinoFileIO fileIO = new TrinoFileIO(fileSystem, new Path("oss://bucket/warehouse"));
        Path schemaPath = new Path("oss://bucket/warehouse/test.db/table/schema/schema-0");

        assertThat(fileIO.tryToWriteAtomic(schemaPath, "schema-v0")).isTrue();
        assertThat(exclusiveCreateAttempted).isTrue();
        assertThat(renamed).isTrue();
        assertThat(new String(content.get(), StandardCharsets.UTF_8)).isEqualTo("schema-v0");
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

    private static class UnsupportedExclusiveOutputFile implements TrinoOutputFile {

        private final Location location;
        private final AtomicBoolean exclusiveCreateAttempted;
        private final AtomicReference<byte[]> content;

        private UnsupportedExclusiveOutputFile(
                Location location,
                AtomicBoolean exclusiveCreateAttempted,
                AtomicReference<byte[]> content) {
            this.location = location;
            this.exclusiveCreateAttempted = exclusiveCreateAttempted;
            this.content = content;
        }

        @Override
        public void createOrOverwrite(byte[] data) {
            content.set(data.clone());
        }

        @Override
        public void createExclusive(byte[] data) {
            exclusiveCreateAttempted.set(true);
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext) {
            return new ByteArrayOutputStream() {
                @Override
                public void close() {
                    content.set(toByteArray());
                }
            };
        }

        @Override
        public Location location() {
            return location;
        }
    }
}
