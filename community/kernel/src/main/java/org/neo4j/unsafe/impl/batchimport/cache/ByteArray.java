/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Abstraction of a {@code byte[]} so that different implementations can be plugged in, for example
 * off-heap, dynamically growing, or other implementations. This interface is slightly different than
 * {@link IntArray} and {@link LongArray} in that one item in the array isn't necessarily just a byte,
 * instead item size can be set to any number and the bytes in an item can be read and written as other
 * number representations like {@link #setInt(long, int, int) int} or {@link #setLong(long, int, long) long},
 * even a special {@link #set6BLong(long, int, long) 6B long}. More can easily be added on demand.
 *
 * @see NumberArrayFactory
 */
public interface ByteArray extends NumberArray<ByteArray>
{
    void get( long index, byte[] into );

    byte getByte( long index, int offset );

    short getShort( long index, int offset );

    int getInt( long index, int offset );

    long get6BLong( long index, int offset );

    long getLong( long index, int offset );

    void set( long index, byte[] value );

    void setByte( long index, int offset, byte value );

    void setShort( long index, int offset, short value );

    void setInt( long index, int offset, int value );

    void set6BLong( long index, int offset, long value );

    void setLong( long index, int offset, long value );
}
