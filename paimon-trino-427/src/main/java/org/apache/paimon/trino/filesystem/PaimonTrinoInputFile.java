/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.paimon.trino.filesystem;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;

/** Paimon trino input file. */
public class PaimonTrinoInputFile implements TrinoInputFile {

    private final FileIO fileIO;
    private final Path path;

    public PaimonTrinoInputFile(FileIO fileIO, Path path) {
        this.fileIO = fileIO;
        this.path = path;
    }

    @Override
    public TrinoInput newInput() throws IOException {
        try {
            return new PaimonTrinoInput(fileIO.newInputStream(path));
        } catch (IOException e) {
            if (e.getMessage().contains("Null IO stream")) {
                return newInput();
            }
            throw e;
        }
    }

    @Override
    public TrinoInputStream newStream() throws IOException {
        try {
            return new PaimonTrinoInputStream(fileIO.newInputStream(path));
        } catch (IOException e) {
            if (e.getMessage().contains("Null IO stream")) {
                return newStream();
            }
            throw e;
        }
    }

    @Override
    public long length() throws IOException {
        return fileIO.getFileSize(path);
    }

    @Override
    public Instant lastModified() throws IOException {
        return Instant.ofEpochSecond(fileIO.getFileStatus(path).getModificationTime());
    }

    @Override
    public boolean exists() throws IOException {
        return fileIO.exists(path);
    }

    @Override
    public Location location() {
        return Location.of(path.toString());
    }
}
