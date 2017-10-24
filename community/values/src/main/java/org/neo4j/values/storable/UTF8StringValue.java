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
 */
public final class UTF8StringValue extends StringValue
{
    //0111 1111, used for removing HIGH BIT from byte
    private static final int HIGH_BIT_MASK = 127;
    //0100 000, used for detecting non-continuation bytes 10xx xxxx
    private static final int NON_CONTINUATION_BIT_MASK = 64;

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

    @Override
    public int computeHash()
    {
        byte[] values = bytes;

        if ( values.length == 0 || length == 0 )
        {
            return 0;
        }

        int hash = 1, i = offset, len = offset + length;
        while ( i < len )
        {
            byte b = values[i];
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
            int codePoint = codePoint( b, i, bytesNeeded );
            i += bytesNeeded;

            hash = 31 * hash + codePoint;
        }

        return hash;
    }

    @Override
    public TextValue substring( int start, int end )
    {
        assert start > 0;
        assert end > start && end < length();
        byte[] values = bytes;

        int count = 0, byteStart = -1, byteEnd = -1, i = offset, len = offset + length;
        while ( i < len )
        {
            if ( count == start )
            {
                byteStart = i;
            }
            if ( count == end )
            {
                byteEnd = i;
                break;
            }
            byte b = values[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                i++;
            }

            while ( b < 0 )
            {
                i++;
                b = (byte) (b << 1);
            }
            count++;
        }
        if ( byteEnd < 0 )
        {
            byteEnd = len;
        }

        assert byteStart >= 0;
        assert byteEnd >= byteStart;
        return new UTF8StringValue( values, byteStart, byteEnd - byteStart );
    }

    @Override
    public TextValue trim()
    {
        byte[] values = bytes;

        if ( values.length == 0 || length == 0 )
        {
            return this;
        }

        int startIndex = trimLeftIndex();
        int endIndex = trimRightIndex();
        return new UTF8StringValue( values, startIndex, Math.max( endIndex + 1 - startIndex, 0 ) );
    }

    @Override
    public int compareTo( TextValue other )
    {
        if (!(other instanceof UTF8StringValue))
        {
            return super.compareTo( other );
        }
        UTF8StringValue otherUTF8 = (UTF8StringValue) other;
        int len1 = bytes.length;
        int len2 = otherUTF8.bytes.length;
        int lim = Math.min(len1, len2);
        int k = 0;
        while (k < lim) {
            byte c1 = bytes[k];
            byte c2 = otherUTF8.bytes[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    /**
     * Returns the left-most index into the underlying byte array that does not belong to a whitespace code point
     */
    private int trimLeftIndex()
    {
        int i = offset, len = offset + length;
        while ( i < len )
        {
            byte b = bytes[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                if ( b > 32 )
                {
                    return i;
                }
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
            int codePoint = codePoint( b, i, bytesNeeded );
            if ( !Character.isWhitespace( codePoint ) )
            {
                return i;
            }
            i += bytesNeeded;
        }
        return i;
    }

    /**
     * Returns the right-most index into the underlying byte array that does not belong to a whitespace code point
     */
    private int trimRightIndex()
    {
        int index = offset + length - 1;
        while ( index >= 0 )
        {
            byte b = bytes[index];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                if ( b > 32 )
                {
                    return index;
                }
                index--;
                continue;
            }

            //We can now have one of three situations.
            //Byte1    Byte2    Byte3    Byte4
            //110xxxxx 10xxxxxx
            //1110xxxx 10xxxxxx 10xxxxxx
            //11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            int bytesNeeded = 1;
            while ( (b & NON_CONTINUATION_BIT_MASK) == 0 )
            {
                bytesNeeded++;
                b = bytes[--index];
            }

            int codePoint = codePoint( (byte) (b << bytesNeeded), index, bytesNeeded );
            if ( !Character.isWhitespace( codePoint ) )
            {
                return Math.min( index + bytesNeeded, length - 1 );
            }
        }
        return index;
    }


    public byte[] bytes()
    {
        return bytes;
    }

    private int codePoint( byte currentByte, int i, int bytesNeeded )
    {
        int codePoint;
        byte[] values = bytes;
        switch ( bytesNeeded )
        {
        case 2:
            codePoint = (currentByte << 4) | (values[i + 1] & HIGH_BIT_MASK);
            break;
        case 3:
            codePoint = (currentByte << 9) | ((values[i + 1] & HIGH_BIT_MASK) << 6) | (values[i + 2] & HIGH_BIT_MASK);
            break;
        case 4:
            codePoint = (currentByte << 14) | ((values[i + 1] & HIGH_BIT_MASK) << 12) |
                        ((values[i + 2] & HIGH_BIT_MASK) << 6)
                        | (values[i + 3] & HIGH_BIT_MASK);
            break;
        default:
            throw new IllegalArgumentException( "Malformed UTF8 value" );
        }
        return codePoint;
    }
}
