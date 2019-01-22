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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Abstraction of a {@code byte[]} so that different implementations can be plugged in, for example
 * off-heap, dynamically growing, or other implementations. This interface is slightly different than
 * {@link IntArray} and {@link LongArray} in that one index in the array isn't necessarily just a byte,
 * instead item size can be set to any number and the bytes in an index can be read and written as other
 * number representations like {@link #setInt(long, int, int) int} or {@link #setLong(long, int, long) long},
 * even a special {@link #set6ByteLong(long, int, long) 6B long}. More can easily be added on demand.
 *
 * Each index in the array can hold multiple values, each at its own offset (starting from 0 at each index), e.g.
 * an array could have items holding values <pre>byte, int, short, long</pre>, where:
 * - the byte would be accessed using offset=0
 * - the int would be accessed using offset=1
 * - the short would be accessed using offset=5
 * - the long would be accessed using offset=7
 *
 * @see NumberArrayFactory
 */
public interface ByteArray extends NumberArray<ByteArray>
{
    /**
     * Gets the data at the given {@code index}. The data is read into the given byte array.
     *
     * @param index array index to get.
     * @param into byte[] to read into.
     */
    void get( long index, byte[] into );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a byte at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the byte at the given offset at the given array index.
     */
    byte getByte( long index, int offset );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a short at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the short at the given offset at the given array index.
     */
    short getShort( long index, int offset );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a int at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the int at the given offset at the given array index.
     */
    int getInt( long index, int offset );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a 5-byte long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the 5-byte long at the given offset at the given array index.
     */
    long get5ByteLong( long index, int offset );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a 6-byte long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the 6-byte long at the given offset at the given array index.
     */
    long get6ByteLong( long index, int offset );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the long at the given offset at the given array index.
     */
    long getLong( long index, int offset );

    /**
     * Sets the given {@code data} at the given {@code index}, overwriting all the values in it.
     *
     * @param index array index to get.
     * @param value the byte[] to copy into the given offset at the given array index.
     */
    void set( long index, byte[] value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a byte at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the byte value to set at the given offset at the given array index.
     */
    void setByte( long index, int offset, byte value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a short at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the short value to set at the given offset at the given array index.
     */
    void setShort( long index, int offset, short value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a int at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the int value to set at the given offset at the given array index.
     */
    void setInt( long index, int offset, int value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a 5-byte long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the 5-byte long value to set at the given offset at the given array index.
     */
    void set5ByteLong( long index, int offset, long value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a 6-byte long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the 6-byte long value to set at the given offset at the given array index.
     */
    void set6ByteLong( long index, int offset, long value );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a long at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the long value to set at the given offset at the given array index.
     */
    void setLong( long index, int offset, long value );

    /**
     * Gets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will get a 3-byte int at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to get the value from.
     * @return the 3-byte int at the given offset at the given array index.
     */
    int get3ByteInt( long index, int offset );

    /**
     * Sets a part of an item, at the given {@code index}. An item in this array can consist of
     * multiple values. This call will set a 3-byte int at the given {@code offset}.
     *
     * @param index array index to get.
     * @param offset offset into this index to set the value for.
     * @param value the 3-byte int value to set at the given offset at the given array index.
     */
    void set3ByteInt( long index, int offset, int value );
}
