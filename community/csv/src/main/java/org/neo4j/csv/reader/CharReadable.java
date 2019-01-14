/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

    /**
     * Reads characters into the given array starting at {@code offset}, reading {@code length} number of characters.
     *
     * Similar to {@link Reader#read(char[], int, int)}
     * @param into char[] to read the data into.
     * @param offset offset to start reading into the char[].
     * @param length number of bytes to read maxuimum.
     * @return number of bytes read, or 0 if there were no bytes read and end of readable is reached.
     * @throws IOException on read error.
     */
    int read( char[] into, int offset, int length ) throws IOException;

    /**
     * @return length of this source, in bytes.
     */
    long length();

    abstract class Adapter extends SourceTraceability.Adapter implements CharReadable
    {
        @Override
        public void close() throws IOException
        {   // Nothing to close
        }
    }

    CharReadable EMPTY = new CharReadable()
    {
        @Override
        public long position()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return "EMPTY";
        }

        @Override
        public int read( char[] into, int offset, int length )
        {
            return -1;
        }

        @Override
        public SectionedCharBuffer read( SectionedCharBuffer buffer, int from )
        {
            buffer.compact( buffer, from );
            return buffer;
        }

        @Override
        public long length()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    };
}
