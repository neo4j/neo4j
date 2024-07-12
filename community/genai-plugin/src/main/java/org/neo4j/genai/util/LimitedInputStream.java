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
package org.neo4j.genai.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends FilterInputStream {
    private final long expected;
    private long count;

    public LimitedInputStream(InputStream in, long expected) {
        super(in);
        this.expected = expected;
    }

    @Override
    public int read() throws IOException {
        return check(super.read());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return check(super.read(b, off, len));
    }

    private int check(int read) throws IOException {
        if ((count += read) > expected) {
            throw new IOException("Stream reads exceeded maximum expected number of bytes %d B".formatted(expected));
        }
        return read;
    }
}
