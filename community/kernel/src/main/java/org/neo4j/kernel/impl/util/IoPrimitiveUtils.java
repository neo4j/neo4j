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
package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

public abstract class IoPrimitiveUtils
{
    public static String readString( ReadableLogChannel channel, int length ) throws IOException
    {
        assert length >= 0 : "invalid array length " + length;
        byte[] chars = new byte[length];
        channel.get( chars, length );
        return new String(chars, "UTF-8");
    }

    public static void write3bLengthAndString( WritableLogChannel channel, String string ) throws IOException
    {
        byte[] chars = string.getBytes( "UTF-8" );
        // 3 bytes to represent the length (4 is a bit overkill)... maybe
        // this space optimization is a bit overkill also :)
        channel.putShort( (short)chars.length );
        channel.put( (byte)(chars.length >> 16) );
        channel.put(chars, chars.length);
    }

    public static String read3bLengthAndString( ReadableLogChannel channel ) throws IOException
    {
        short lengthShort = channel.getShort();
        byte lengthByte = channel.get();
        int length = (lengthByte << 16) | (lengthShort & 0xFFFF);
        byte[] chars = new byte[length];
        channel.get( chars, length );
        return new String(chars, "UTF-8");
    }

    public static void write2bLengthAndString( WritableLogChannel channel, String string ) throws IOException
    {
        byte[] chars = string.getBytes( "UTF-8" );
        channel.putShort( (short)chars.length );
        channel.put(chars, chars.length);
    }

    public static String read2bLengthAndString( ReadableLogChannel channel ) throws IOException
    {
        short length = channel.getShort();
        return readString( channel, length );
    }

    private static char[] readCharArray( ReadableByteChannel channel,
            ByteBuffer buffer, char[] charArray ) throws IOException
    {
        buffer.clear();
        int charsLeft = charArray.length;
        int maxSize = buffer.capacity() / 2;
        int offset = 0; // offset in chars
        while ( charsLeft > 0 )
        {
            if ( charsLeft > maxSize )
            {
                buffer.limit( maxSize * 2 );
                charsLeft -= maxSize;
            }
            else
            {
                buffer.limit( charsLeft * 2 );
                charsLeft = 0;
            }
            if ( channel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            int length = buffer.limit() / 2;
            buffer.asCharBuffer().get( charArray, offset, length );
            offset += length;
            buffer.clear();
        }
        return charArray;
    }

    public static boolean readAndFlip( ReadableByteChannel channel, ByteBuffer buffer, int bytes )
            throws IOException
    {
        buffer.clear();
        buffer.limit( bytes );
        while ( buffer.hasRemaining())
        {
            int read = channel.read( buffer );

            if ( read == -1 )
            {
                return false;
            }
        }
        buffer.flip();
        return true;
    }

    public static Integer readInt( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
    {
        return readAndFlip( channel, buffer, 4 ) ? buffer.getInt() : null;
    }

    public static byte[] readBytes( ReadableByteChannel channel, byte[] array ) throws IOException
    {
        return readBytes( channel, array, array.length );
    }

    public static byte[] readBytes( ReadableByteChannel channel, byte[] array, int length ) throws IOException
    {
        return readAndFlip( channel, ByteBuffer.wrap( array ), length ) ? array : null;
    }

    public static Map<String, String> read2bMap( ReadableLogChannel channel ) throws IOException
    {
        short size = channel.getShort();
        Map<String, String> map = new HashMap<>();
        for ( int i = 0; i < size; i++ )
        {
            String key = read2bLengthAndString( channel );
            String value = read2bLengthAndString( channel );
            map.put( key, value );
        }
        return map;
    }

    public static String readLengthAndString( ReadableByteChannel channel,
            ByteBuffer buffer ) throws IOException
    {
        Integer length = readInt( channel, buffer );
        if ( length != null )
        {
            char[] chars = new char[length];
            chars = readCharArray( channel, buffer, chars );
            return chars == null ? null : new String( chars );
        }
        return null;
    }

    public static void writeLengthAndString( StoreChannel channel, ByteBuffer buffer, String value )
            throws IOException
    {
        char[] chars = value.toCharArray();
        int length = chars.length;
        writeInt( channel, buffer, length );
        writeChars( channel, buffer, chars );
    }

    private static void writeChars( StoreChannel channel, ByteBuffer buffer, char[] chars )
            throws IOException
    {
        int position = 0;
        do
        {
            buffer.clear();
            int leftToWrite = chars.length - position;
            if ( leftToWrite * 2 < buffer.capacity() )
            {
                buffer.asCharBuffer().put( chars, position, leftToWrite );
                buffer.limit( leftToWrite * 2);
                channel.write( buffer );
                position += leftToWrite;
            }
            else
            {
                int length = buffer.capacity() / 2;
                buffer.asCharBuffer().put( chars, position, length );
                buffer.limit( length * 2 );
                channel.write( buffer );
                position += length;
            }
        } while ( position < chars.length );
    }

    public static void writeInt( StoreChannel channel, ByteBuffer buffer, int value )
            throws IOException
    {
        buffer.clear();
        buffer.putInt( value );
        buffer.flip();
        channel.write( buffer );
    }

    public static Object[] asArray( Object propertyValue )
    {
        if ( propertyValue.getClass().isArray() )
        {
            int length = Array.getLength( propertyValue );
            Object[] result = new Object[ length ];
            for ( int i = 0; i < length; i++ )
            {
                result[ i ] = Array.get( propertyValue, i );
            }
            return result;
        }
        return new Object[] { propertyValue };
    }

    public static int safeCastLongToInt( long value )
    {
        if ( value >= Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( "Casting long value " + value + " to an int would wrap around" );
        }
        return (int) value;
    }

    public static int shortToUnsignedInt( short value )
    {
        return value & 0xFFFF;
    }
}
