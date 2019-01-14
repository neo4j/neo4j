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
package org.neo4j.values.storable;

import java.util.Arrays;

/**
 * Helper class for dynamically building {@link UTF8StringValue}.
 * <p>
 * Either add individual bytes via {@link #add(byte)} or add a unicode code point via {@link #addCodePoint(int)}. The
 * builder maintains an internal {@code byte[]} and grows it as necessary.
 */
class UTF8StringValueBuilder
{
    private static final int DEFAULT_SIZE = 8;
    private byte[] bytes;
    private int length;

    UTF8StringValueBuilder()
    {
        this( DEFAULT_SIZE );
    }

    UTF8StringValueBuilder( int initialCapacity )
    {
        this.bytes = new byte[initialCapacity];
    }

    /**
     * Add a single byte to the builder.
     *
     * @param b the byte to add.
     */
    void add( byte b )
    {
        if ( bytes.length == length )
        {
            ensureCapacity();
        }
        bytes[length++] = b;
    }

    private void ensureCapacity()
    {
        int newCapacity = bytes.length << 1;
        if ( newCapacity < 0 )
        {
            throw new IllegalStateException( "Fail to increase capacity." );
        }
        this.bytes = Arrays.copyOf( bytes, newCapacity );
    }

    TextValue build()
    {
        return Values.utf8Value( bytes, 0, length );
    }

    /**
     * Add a single code point to the builder.
     * <p>
     * In UTF8 a code point use one to four bytes depending on the code point at hand. So we will have one of the
     * following cases (x marks the bits of the codepoint):
     * <ul>
     * <li>One byte (asciii): {@code 0xxx xxxx}</li>
     * <li>Two bytes: {@code 110xxxxx 10xxxxxx}</li>
     * <li>Three bytes: {@code 1110xxxx 10xxxxxx 10xxxxxx}</li>
     * <li>Four bytes: {@code 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx}</li>
     * </ul>
     *
     * @param codePoint the code point to add
     */
    void addCodePoint( int codePoint )
    {
        assert codePoint >= 0;
        if ( codePoint < 0x80 )
        {
            //one byte is all it takes
            add( (byte) codePoint );
        }
        else if ( codePoint < 0x800 )
        {
            //Require two bytes - will be laid out like:
            //b1       b2
            //110xxxxx 10xxxxxx
            add( (byte) (0b1100_0000 | (0b0001_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
        else if ( codePoint < 0x10000 )
        {
            //Require three bytes - will be laid out like:
            //b1       b2       b3
            //1110xxxx 10xxxxxx 10xxxxxx
            add( (byte) (0b1110_0000 | (0b0000_1111 & (codePoint >> 12))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
        else
        {
            //Require four bytes - will be laid out like:
            //b1       b2       b3       b4
            //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            add( (byte) (0b1111_0000 | (0b0001_1111 & (codePoint >> 18))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 12))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & (codePoint >> 6))) );
            add( (byte) (0b1000_0000 | (0b0011_1111 & codePoint)) );
        }
    }
}
