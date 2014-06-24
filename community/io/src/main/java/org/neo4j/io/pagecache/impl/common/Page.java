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
package org.neo4j.io.pagecache.impl.common;

public interface Page
{
    byte getByte( int offset );

    long getLong( int offset );
    void putLong( long value, int offset );

    int getInt( int offset );
    long getUnsignedInt( int offset );
    void putInt( int value, int offset );

    void getBytes( byte[] data, int offset );
    void putBytes( byte[] data, int offset );

    void putByte( byte value, int offset );

    short getShort( int offset );
    void putShort( short value, int offset );
}
