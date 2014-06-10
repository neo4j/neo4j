/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache;

import java.io.IOException;

public interface PageCursor extends AutoCloseable
{
    byte getByte();
    void putByte( byte value );

    long getLong();
    void putLong( long value );

    int getInt();
    void putInt( int value );

    long getUnsignedInt();

    void getBytes( byte[] data );
    void putBytes( byte[] data );


    short getShort();
    void putShort( short value );


    void setOffset( int offset );
    int getOffset();

    // TODO remove all the methods above this comment

    void rewind() throws IOException;

    boolean next() throws IOException;

    void close(); // TODO remove because it's specified by AutoClosable?
}
