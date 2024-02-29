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

import io.trino.filesystem.TrinoInput;

import java.io.IOException;

/** Paimon trino input. */
public class PaimonTrinoInput implements TrinoInput {

    private final SeekableInputStream inputStream;
    private final PaimonTrinoInputFile inputFile;

    public PaimonTrinoInput(SeekableInputStream inputStream, PaimonTrinoInputFile inputFile) {
        this.inputStream = inputStream;
        this.inputFile = inputFile;
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException {
        long start = inputStream.getPos();
        inputStream.seek(position);
        inputStream.read(buffer, bufferOffset, bufferLength);
        inputStream.seek(start);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength) throws IOException {
        long startPos = inputStream.getPos();
        try {
            long available = inputFile.length();
            long position = available - bufferLength;
            if (position >= 0) {
                inputStream.seek(position);
            } else {
                inputStream.seek(0);
            }
            return inputStream.read(buffer, bufferOffset, bufferLength);
        } finally {
            inputStream.seek(startPos);
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
