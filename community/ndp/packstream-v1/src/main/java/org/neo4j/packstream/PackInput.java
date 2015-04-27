/*
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
package org.neo4j.packstream;

import java.io.IOException;

public interface PackInput
{
    /**
     * Ensure the specified number of bytes are available for reading
     *
     * @throws org.neo4j.packstream.PackStream.EndOfStream if there are not enough bytes available
     */
    PackInput ensure( int numBytes ) throws IOException;

    /** Attempt to make up to the specified bytes available for reading */
    PackInput attemptUpTo( int numBytes ) throws IOException;

    /** Attempt to make the specified number of bytes available for reading. */
    boolean attempt( int numBytes ) throws IOException;

    byte get();

    /**
     * Number of bytes immediately readable. This differs from {@link #ensure(int)} and {@link #attempt(int)} in that
     * this does not load more bytes from any underlying source.
     */
    int remaining();

    short getShort();

    int getInt();

    long getLong();

    double getDouble();

    PackInput get( byte[] into, int offset, int toRead );

    /** Get the next byte without forwarding the internal pointer */
    byte peek();
}
