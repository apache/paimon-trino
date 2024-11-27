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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;

import io.trino.filesystem.TrinoFileSystem;

/** Trino file io loader for paimon. */
public class TrinoFileIOLoader implements FileIOLoader {

    private final TrinoFileSystem trinoFileSystem;

    public TrinoFileIOLoader(TrinoFileSystem trinoFileSystem) {
        this.trinoFileSystem = trinoFileSystem;
    }

    @Override
    public String getScheme() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileIO load(Path path) {
        return new TrinoFileIO(trinoFileSystem, path);
    }
}
