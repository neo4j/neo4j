/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.csv.reader;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;

/**
 * A {@link Readable}, but focused on {@code char[]}, via a {@link SectionedCharBuffer} with one of the main reasons
 * that {@link Reader#read(CharBuffer)} creates a new {@code char[]} as big as the data it's about to read
 * every call. However {@link Reader#read(char[], int, int)} doesn't, and so leaves no garbage.
 *
 * The fact that this is a separate interface means that {@link Readable} instances need to be wrapped,
 * but that's fine since the buffer size should be reasonably big such that {@link #read(SectionedCharBuffer, int)}
 * isn't called too often. Therefore the wrapping overhead should not be noticeable at all.
 *
 * Also took the opportunity to let {@link CharReadable} extends {@link Closeable}, something that
 * {@link Readable} doesn't.
 */
public interface CharReadable extends Closeable, SourceTraceability
{
    /**
     * Reads characters into the {@link SectionedCharBuffer buffer}.
     * This method will block until data is available, an I/O error occurs, or the end of the stream is reached.
     * The caller is responsible for passing in {@code from} which index existing characters should be saved,
     * using {@link SectionedCharBuffer#compact(SectionedCharBuffer, int) compaction}, before reading into the
     * front section of the buffer, using {@link SectionedCharBuffer#readFrom(Reader)}.
     * The returned {@link SectionedCharBuffer} can be the same as got passed in, or another buffer if f.ex.
     * double-buffering is used. If this reader reached eof, i.e. equal state to that of {@link Reader#read(char[])}
     * returning {@code -1} then {@link SectionedCharBuffer#hasAvailable()} for the returned instances will
     * return {@code false}.
     *
     * @param buffer {@link SectionedCharBuffer} to read new data into.
     * @param from index into the buffer array where characters to save (compact) starts (inclusive).
     * @return a {@link SectionedCharBuffer} containing new data.
     * @throws IOException if an I/O error occurs.
     */
    SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException;

    public static abstract class Adapter extends SourceTraceability.Adapter implements CharReadable
    {
        @Override
        public void close() throws IOException
        {   // Nothing to close
        }
    }
}
