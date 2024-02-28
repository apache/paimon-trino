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

import org.apache.paimon.fs.SeekableInputStream;

import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

/** Paimon trino input stream. */
public class PaimonTrinoInputStream extends TrinoInputStream {

    private final SeekableInputStream inputStream;

    public PaimonTrinoInputStream(SeekableInputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public long getPosition() throws IOException {
        return inputStream.getPos();
    }

    @Override
    public void seek(long position) throws IOException {
        inputStream.seek(position);
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return inputStream.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputStream.read(b, off, len);
    }
}
