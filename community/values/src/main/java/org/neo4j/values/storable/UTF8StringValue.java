/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.storable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/*
* Just as a normal StringValue but is backed by a byte array and does string
* serialization lazily.
 *
 * TODO in this implementation most operations will actually load the string
 * such as hashCode. These could be implemented using
 * the byte array directly in later optimizations
*/
public final class UTF8StringValue extends StringValue
{
    private volatile String value;
    private final byte[] bytes;
    private final int offset;
    private final int length;

    UTF8StringValue( byte[] bytes, int offset, int length )
    {
        assert bytes != null;
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeUTF8( bytes, offset, length );
    }

    @Override
    public boolean equals( Value value )
    {
        if ( value instanceof org.neo4j.values.storable.UTF8StringValue )
        {
            return Arrays.equals( bytes, ((org.neo4j.values.storable.UTF8StringValue) value).bytes );
        }
        else
        {
            return super.equals( value );
        }
    }

    @Override
    String value()
    {
        String s = value;
        if ( s == null )
        {
            synchronized ( this )
            {
                s = value;
                if ( s == null )
                {
                    s = value = new String( bytes, offset, length, StandardCharsets.UTF_8 );

                }
            }
        }
        return s;
    }

    @Override
    public int length()
    {
        int count = 0, i = offset, len = offset + length;
        while ( i < len )
        {
            byte b = bytes[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                i++;
            }

            //The number of high bits tells us how many bytes we use to store the value
            //e.g. 110xxxx -> need two bytes, 1110xxxx -> need three bytes, 11110xxx -> needs
            //four bytes
            while ( b < 0 )
            {
                i++;
                b = (byte) (b << 1);
            }
            count++;
        }
        return count;
    }

    private static final int HIGH_BIT_MASK = 127;

    @Override
    public int computeHash()
    {
        if ( bytes.length == 0 )
        {
            return 0;
        }

        int hash = 1, i = offset, len = offset + length;
        while ( i < len )
        {
            byte b = bytes[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                hash = 31 * hash + b;
                i++;
                continue;
            }

            //We can now have one of three situations.
            //Byte1    Byte2    Byte3    Byte4
            //110xxxxx 10xxxxxx
            //1110xxxx 10xxxxxx 10xxxxxx
            //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            //Figure out how many bytes we need by reading the number of leading bytes
            int bytesNeeded = 0;
            while ( b < 0 )
            {
                bytesNeeded++;
                b = (byte) (b << 1);
            }
            int codePoint;
            switch ( bytesNeeded )
            {
            case 2:
                codePoint = (b << 4) | (bytes[i + 1] & HIGH_BIT_MASK);
                i += 2;
                break;
            case 3:
                codePoint = (b << 9) | ((bytes[i + 1] & HIGH_BIT_MASK) << 6) | (bytes[i + 2] & HIGH_BIT_MASK);
                i += 3;
                break;
            case 4:
                codePoint = (b << 14) | ((bytes[i + 1] & HIGH_BIT_MASK) << 12) | ((bytes[i + 2] & HIGH_BIT_MASK) << 6)
                            | (bytes[i + 3] & HIGH_BIT_MASK);
                i += 4;
                break;
            default:
                throw new IllegalArgumentException( "Malformed UTF8 value" );
            }
            hash = 31 * hash + codePoint;
        }

        return hash;
    }

    public byte[] bytes()
    {
        return bytes;
    }
}
