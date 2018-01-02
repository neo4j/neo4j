/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/*
 * Just as a normal StringValue but is backed by a byte array and does string
 * serialization lazily when necessary.
 *
 */
public final class UTF8StringValue extends StringValue
{
    /** Used for removing the high order bit from byte. */
    private static final int HIGH_BIT_MASK = 0b0111_1111;
    /** Used for detecting non-continuation bytes. For example {@code 0b10xx_xxxx}. */
    private static final int NON_CONTINUATION_BIT_MASK = 0b0100_0000;

    private volatile String value;
    private final byte[] bytes;
    private final int offset;
    private final int byteLength;

    UTF8StringValue( byte[] bytes, int offset, int length )
    {
        assert bytes != null;
        this.bytes = bytes;
        this.offset = offset;
        this.byteLength = length;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeUTF8( bytes, offset, byteLength );
    }

    @Override
    public boolean equals( Value value )
    {
        if ( value instanceof UTF8StringValue )
        {
            UTF8StringValue other = (UTF8StringValue) value;
            if ( byteLength != other.byteLength )
            {
                return false;
            }
            for ( int i = offset, j = other.offset; i < byteLength; i++, j++ )
            {
                if ( bytes[i] != other.bytes[j] )
                {
                    return false;
                }
            }
            return true;
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
                    value = s = new String( bytes, offset, byteLength, StandardCharsets.UTF_8 );
                }
            }
        }
        return s;
    }

    @Override
    public int length()
    {
        int count = 0, i = offset, len = offset + byteLength;
        while ( i < len )
        {
            byte b = bytes[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                i++;
                count++;
                continue;
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

        if ( values.length == 0 || byteLength == 0 )
        {
            return 0;
        }

        int hash = 1, i = offset, len = offset + byteLength;
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
    public TextValue substring( int start, int length )
    {
        if ( start < 0 || length < 0 )
        {
            throw new IndexOutOfBoundsException( "Cannot handle negative start index nor negative length" );
        }
        if ( length == 0 )
        {
            return StringValue.EMTPY;
        }

        int end = start + length;
        byte[] values = bytes;
        int count = 0, byteStart = -1, byteEnd = -1, i = offset, len = offset + byteLength;
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
        if ( byteStart < 0 )
        {
            return StringValue.EMTPY;
        }
        return new UTF8StringValue( values, byteStart, byteEnd - byteStart );
    }

    @Override
    public TextValue trim()
    {
        byte[] values = bytes;

        if ( values.length == 0 || byteLength == 0 )
        {
            return this;
        }

        int startIndex = trimLeftIndex();
        int endIndex = trimRightIndex();
        if ( startIndex > endIndex )
        {
            return StringValue.EMTPY;
        }

        return new UTF8StringValue( values, startIndex, Math.max( endIndex + 1 - startIndex, 0 ) );
    }

    @Override
    public TextValue ltrim()
    {
        byte[] values = bytes;
        if ( values.length == 0 || byteLength == 0 )
        {
            return this;
        }

        int startIndex = trimLeftIndex();
        if ( startIndex >= values.length )
        {
            return StringValue.EMTPY;
        }
        return new UTF8StringValue( values, startIndex, values.length - startIndex );
    }

    @Override
    public TextValue rtrim()
    {
        byte[] values = bytes;
        if ( values.length == 0 || byteLength == 0 )
        {
            return this;
        }

        int endIndex = trimRightIndex();
        if ( endIndex < 0 )
        {
            return StringValue.EMTPY;
        }
        return new UTF8StringValue( values, offset, endIndex + 1 - offset );
    }

    @Override
    public TextValue reverse()
    {
        byte[] values = bytes;

        if ( values.length == 0 || byteLength == 0 )
        {
            return StringValue.EMTPY;
        }

        int i = offset, len = offset + byteLength;
        byte[] newValues = new byte[byteLength];
        while ( i < len )
        {
            byte b = values[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                //a single byte is trivial to reverse
                //just put it at the opposite end of the new array
                newValues[len - 1 - i] = b;
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
            //reversing when multiple bytes are needed for the code point we cannot just reverse
            //since we need to preserve the code point while moving it,
            //e.g. [A, b1,b2, B] -> [B, b1,b2, A]
            System.arraycopy( values, i, newValues, len - i - bytesNeeded, bytesNeeded );
            i += bytesNeeded;
        }

        return new UTF8StringValue( newValues, 0, newValues.length );
    }

    @Override
    public int compareTo( TextValue other )
    {
        if ( !(other instanceof UTF8StringValue) )
        {
            return super.compareTo( other );
        }
        UTF8StringValue otherUTF8 = (UTF8StringValue) other;
        int len1 = bytes.length;
        int len2 = otherUTF8.bytes.length;
        int lim = Math.min( len1, len2 );
        int i = 0;
        while ( i < lim )
        {
            byte b = bytes[i];
            int thisCodePoint;
            int thatCodePoint = codePointAt( otherUTF8.bytes, i );
            if ( b >= 0 )
            {
                i++;
                thisCodePoint = b;
            }
            else
            {
                int bytesNeeded = 0;
                while ( b < 0 )
                {
                    bytesNeeded++;
                    b = (byte) (b << 1);
                }
                thisCodePoint = codePoint( b, i, bytesNeeded );
                i += bytesNeeded;
            }
            if ( thisCodePoint != thatCodePoint )
            {
                return thisCodePoint - thatCodePoint;
            }
        }

        return length() - other.length();
    }

    private int codePointAt( byte[] bytes, int i )
    {
        assert i < bytes.length;
        byte b = bytes[i];
        if ( b >= 0 )
        {
            return b;
        }
        int bytesNeeded = 0;
        while ( b < 0 )
        {
            bytesNeeded++;
            b = (byte) (b << 1);
        }
        switch ( bytesNeeded )
        {
        case 2:
            return (b << 4) | (bytes[i + 1] & HIGH_BIT_MASK);
        case 3:
            return (b << 9) | ((bytes[i + 1] & HIGH_BIT_MASK) << 6) | (bytes[i + 2] & HIGH_BIT_MASK);
        case 4:
            return (b << 14) | ((bytes[i + 1] & HIGH_BIT_MASK) << 12) |
                   ((bytes[i + 2] & HIGH_BIT_MASK) << 6)
                   | (bytes[i + 3] & HIGH_BIT_MASK);
        default:
            throw new IllegalArgumentException( "Malformed UTF8 value" );
        }
    }

    /**
     * Returns the left-most index into the underlying byte array that does not belong to a whitespace code point
     */
    private int trimLeftIndex()
    {
        int i = offset, len = offset + byteLength;
        while ( i < len )
        {
            byte b = bytes[i];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                if ( !Character.isWhitespace( b ) )
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
        int index = offset + byteLength - 1;
        while ( index >= 0 )
        {
            byte b = bytes[index];
            //If high bit is zero (equivalent to the byte being positive in two's complement)
            //we are dealing with an ascii value and use a single byte for storing the value.
            if ( b >= 0 )
            {
                if ( !Character.isWhitespace( b ) )
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
                return Math.min( index + bytesNeeded - 1, bytes.length - 1 );
            }
            index--;

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
