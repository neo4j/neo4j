/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.queryapi.request;

import java.io.IOException;
import java.io.InputStream;

/**
 * InputStream where the first byte is peeked so we can determine whether the stream being provided
 * has data or not. Needed because Jetty does not provide a Buffered reader which we can mark and reset.
 */
public class PeekedFirstByteInputStream extends InputStream {

    private final InputStream delegateInputStream;
    private int peekedByte;
    private boolean byteReadFromDelegateStream;
    private boolean peekedByteConsumed;

    public PeekedFirstByteInputStream(InputStream inputStream) {
        this.delegateInputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        if (!peekedByteConsumed) {
            byteReadFromDelegateStream = false;
            peekedByteConsumed = true;
            return peekedByte;
        } else {
            return delegateInputStream.read();
        }
    }

    public int peek() throws IOException {
        if (!byteReadFromDelegateStream) {
            peekedByte = delegateInputStream.read();
            byteReadFromDelegateStream = true;
            peekedByteConsumed = false;
        }

        return peekedByte;
    }

    @Override
    public void close() throws IOException {
        delegateInputStream.close();
    }
}
