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

package org.apache.paimon.trino.fileio;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Trino file io for paimon. */
public class TrinoFileIO implements FileIO {

    private final TrinoFileSystem trinoFileSystem;
    private final boolean objectStore;

    public TrinoFileIO(TrinoFileSystem trinoFileSystem, @Nullable Path path) {
        this.trinoFileSystem = trinoFileSystem;
        this.objectStore = path == null || checkObjectStore(path.toUri().getScheme());
    }

    @Override
    public boolean isObjectStore() {
        return objectStore;
    }

    @Override
    public void configure(CatalogContext catalogContext) {}

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return new TrinoInputStreamWrapper(
                trinoFileSystem.newInputFile(Location.of(path.toString())).newStream());
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        TrinoOutputFile trinoOutputFile =
                trinoFileSystem.newOutputFile(Location.of(path.toString()));
        return new PositionOutputStreamWrapper(
                overwrite ? trinoOutputFile.createOrOverwrite() : trinoOutputFile.create());
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return status(path);
    }

    private FileStatus status(Path path) throws IOException {
        if (trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)) {
            return new TrinoDirectoryFileStatus(path);
        } else {
            TrinoInputFile trinoInputFile =
                    trinoFileSystem.newInputFile(Location.of(path.toString()));
            return new TrinoFileStatus(
                    trinoInputFile.length(), path, trinoInputFile.lastModified().getEpochSecond());
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        List<FileStatus> fileStatusList = new ArrayList<>();
        Location location = Location.of(path.toString());
        // In version trino 420, trinoFileSystem could only search for files (directories don't
        // count in files in trino)
        // Therefore, this interface only return FileStatus those are file.
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            FileIterator fileIterator = trinoFileSystem.listFiles(location);
            while (fileIterator.hasNext()) {
                FileEntry fileEntry = fileIterator.next();
                fileStatusList.add(
                        new TrinoFileStatus(
                                fileEntry.length(),
                                new Path(fileEntry.location().toString()),
                                fileEntry.lastModified().getEpochSecond()));
            }
        }
        return fileStatusList.toArray(new FileStatus[0]);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return trinoFileSystem.directoryExists(Location.of(path.toString())).orElse(false)
                || existFile(Location.of(path.toString()));
    }

    private boolean existFile(Location location) throws IOException {
        try {
            return trinoFileSystem.newInputFile(location).exists();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        Location location = Location.of(path.toString());
        if (trinoFileSystem.directoryExists(location).orElse(false)) {
            trinoFileSystem.deleteDirectory(location);
            return true;
        } else if (existFile(location)) {
            trinoFileSystem.deleteFile(location);
            return true;
        }

        return false;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path source, Path target) throws IOException {
        Location sourceLocation = Location.of(source.toString());
        Location targetLocation = Location.of(target.toString());
        if (trinoFileSystem.directoryExists(sourceLocation).orElse(false)) {
            throw new UnsupportedOperationException();
        } else {
            trinoFileSystem.renameFile(sourceLocation, targetLocation);
        }
        return true;
    }

    private static boolean checkObjectStore(String scheme) {
        scheme = scheme.toLowerCase();
        if (!scheme.startsWith("s3")
                && !scheme.startsWith("emr")
                && !scheme.startsWith("oss")
                && !scheme.startsWith("wasb")) {
            return scheme.startsWith("http") || scheme.startsWith("ftp");
        } else {
            return true;
        }
    }
}
