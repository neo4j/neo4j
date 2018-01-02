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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.pagecache.PageCursor;

final class BigEndianByteArrayBuffer implements ReadableBuffer, WritableBuffer
{
    static BigEndianByteArrayBuffer buffer( int size )
    {
        return new BigEndianByteArrayBuffer( size );
    }

    final byte[] buffer;

    BigEndianByteArrayBuffer( int size )
    {
        this( new byte[size] );
    }

    BigEndianByteArrayBuffer( byte[] buffer )
    {
        this.buffer = buffer;
    }

    @Override
    public String toString()
    {
        return toString( buffer );
    }

    static String toString( byte[] buffer )
    {
        StringBuilder result = new StringBuilder( buffer.length * 6 + 1 ).append( '[' );
        for ( byte b : buffer )
        {
            if ( b >= 32 && b < 127 )
            {
                if ( b == '\'' )
                {
                    result.append( "'\\''" );
                }
                else
                {
                    result.append( '\'' ).append( (char) b ).append( '\'' );
                }
            }
            else
            {
                result.append( "0x" );
                if ( b < 16 )
                {
                    result.append( 0 );
                }
                result.append( Integer.toHexString( b & 0xFF ) );
            }
            result.append( ", " );
        }
        if ( result.length() > 1 )
        {
            result.setLength( result.length() - 2 );
        }
        result.append( ']' );
        return result.toString();
    }

    static int compare( byte[] key, byte[] searchSpace, int offset )
    {
        for ( int i = 0; i < key.length; i++ )
        {
            int result = (key[i] & 0xFF) - (searchSpace[offset + i] & 0xFF);
            if ( result != 0 )
            {
                return result;
            }
        }
        return 0;
    }

    public void clear()
    {
        fill( (byte) 0 );
    }

    public void fill( byte zero )
    {
        Arrays.fill( buffer, zero );
    }

    public boolean allZeroes()
    {
        for ( byte b : buffer )
        {
            if ( b != 0 )
            {
                return false;
            }
        }
        return true;
    }

    public boolean minusOneAtTheEnd()
    {
        for ( int i = 0; i < buffer.length / 2; i++ )
        {
            if ( buffer[i] != 0 )
            {
                return false;
            }
        }

        for ( int i = buffer.length / 2; i < buffer.length; i++)
        {
            if ( buffer[i] != -1 )
            {
                return false;
            }
        }
        return true;
    }

    public void dataFrom( ByteBuffer buffer )
    {
        buffer.get( this.buffer );
    }

    public void dataTo( byte[] target, int targetPos )
    {
        assert target.length >= targetPos + buffer.length : "insufficient space";
        System.arraycopy( buffer, 0, target, targetPos, buffer.length );
    }

    public int size()
    {
        return buffer.length;
    }

    public byte getByte( int offset )
    {
        offset = checkBounds( offset, 1 );
        return buffer[offset];
    }

    public BigEndianByteArrayBuffer putByte( int offset, byte value )
    {
        return putValue( offset, value, 1 );
    }

    public short getShort( int offset )
    {
        offset = checkBounds( offset, 2 );
        return (short) (((0xFF & buffer[offset]) << 8) |
                        (0xFF & buffer[offset + 1]));
    }

    public BigEndianByteArrayBuffer putShort( int offset, short value )
    {
        return putValue( offset, value, 2 );
    }

    public char getChar( int offset )
    {
        offset = checkBounds( offset, 2 );
        return (char) (((0xFF & buffer[offset]) << 8) |
                       (0xFF & buffer[offset + 1]));
    }

    public BigEndianByteArrayBuffer putChar( int offset, char value )
    {
        return putValue( offset, value, 2 );
    }

    public int getInt( int offset )
    {
        offset = checkBounds( offset, 4 );
        return (((0xFF & buffer[offset]) << 24) |
                ((0xFF & buffer[offset + 1]) << 16) |
                ((0xFF & buffer[offset + 2]) << 8) |
                (0xFF & buffer[offset + 3]));
    }

    public BigEndianByteArrayBuffer putInt( int offset, int value )
    {
        return putValue( offset, value, 4 );
    }

    public long getLong( int offset )
    {
        offset = checkBounds( offset, 8 );
        return (((0xFFL & buffer[offset]) << 56) |
                ((0xFFL & buffer[offset + 1]) << 48) |
                ((0xFFL & buffer[offset + 2]) << 40) |
                ((0xFFL & buffer[offset + 3]) << 32) |
                ((0xFFL & buffer[offset + 4]) << 24) |
                ((0xFFL & buffer[offset + 5]) << 16) |
                ((0xFFL & buffer[offset + 6]) << 8) |
                (0xFFL & buffer[offset + 7]));
    }

    @Override
    public byte[] get( int offset, byte[] target )
    {
        System.arraycopy( buffer, offset, target, 0, target.length );
        return target;
    }

    @Override
    public int compareTo( byte[] value )
    {
        return compare( buffer, value, 0 );
    }

    public BigEndianByteArrayBuffer putLong( int offset, long value )
    {
        return putValue( offset, value, 8 );
    }

    @Override
    public BigEndianByteArrayBuffer put( int offset, byte[] value )
    {
        System.arraycopy( value, 0, buffer, offset, value.length );
        return this;
    }

    @Override
    public void getFrom( PageCursor cursor )
    {
        cursor.getBytes( buffer );
    }

    private BigEndianByteArrayBuffer putValue( int offset, long value, int size )
    {
        offset = checkBounds( offset, size );
        while ( size-- > 0 )
        {
            buffer[offset + size] = (byte) (0xFF & value);
            value >>>= 8;
        }
        return this;
    }

    void putIntegerAtEnd( long value ) throws IOException
    {
        if ( value < -1 )
        {
            throw new IllegalArgumentException( "Negative values different form -1 are not supported." );
        }
        if ( this.size() < 8 )
        {
            if ( Long.numberOfLeadingZeros( value ) > (8 * this.size()) )
            {
                throw new IOException( String.format( "Cannot write integer value (%d), value capacity = %d",
                                                      value, this.size() ) );
            }
        }
        for ( int i = buffer.length; i-- > 0 && value != 0; )
        {
            buffer[i] = (byte) (0xFF & value);
            value >>>= 8;
        }
    }

    long getIntegerFromEnd()
    {
        long value = 0;
        for ( int i = Math.max( 0, buffer.length - 8 ); i < buffer.length; i++ )
        {
            value = (value << 8) | (0xFFL & buffer[i]);
        }
        return value;
    }

    public void read( WritableBuffer target )
    {
        target.put( 0, buffer );
    }

    private int checkBounds( int offset, int size )
    {
        if ( offset < 0 || offset > size() - size )
        {
            throw new IndexOutOfBoundsException( String.format( "offset=%d, buffer size=%d, data item size=%d",
                                                                offset, size(), size ) );
        }
        return offset;
    }
}
