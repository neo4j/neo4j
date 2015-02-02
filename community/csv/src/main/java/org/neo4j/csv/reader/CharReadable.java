/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * A {@link Readable}, but focused on {@code char[]} instead of {@link CharBuffer}, with the main reaon
 * that {@link Reader#read(CharBuffer)} creates a new {@code char[]} as big as the data it's about to read
 * every call. However {@link Reader#read(char[], int, int)} doesn't, and so leaves no garbage.
 *
 * The fact that this is a separate interface means that {@link Readable} instances need to be wrapped,
 * but that's fine since the buffer size should be reasonably big such that {@link #read(char[], int, int)}
 * isn't called too often. Therefore the wrapping overhead should not be noticeable at all.
 *
 * Also took the opportunity to let {@link CharReadable} extends {@link Closeable}, something that
 * {@link Readable} doesn't.
 */
public interface CharReadable extends Closeable
{
    /**
     * Reads characters into a portion of an array. This method will block until some input is available,
     * an I/O error occurs, or the end of the stream is reached.
     *
     * @param buffer {@code char[]} buffer to read the data into.
     * @param offset offset at which to start storing characters in {@code buffer}.
     * @param length maximum number of characters to read.
     * @return the number of characters read, or -1 if the end of the stream has been reached.
     * @throws IOException if an I/O error occurs.
     */
    int read( char[] buffer, int offset, int length ) throws IOException;

    /**
     * @return a low-level byte-like position of f.ex. total number of read bytes.
     */
    long position();
}
