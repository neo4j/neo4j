package org.neo4j.index.impl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public abstract class NioUtils
{
    public static String readLengthAndString( ReadableByteChannel channel,
            ByteBuffer buffer ) throws IOException
    {
        Integer length = readInt( channel, buffer );
        String result = length != null ? readString( channel, buffer, length ) : null;
        return result;
    }
    
    public static String readString( ReadableByteChannel channel, ByteBuffer buffer,
            int length ) throws IOException
    {
        char[] chars = new char[length];
        chars = readCharArray( channel, buffer, chars );
        return chars == null ? null : new String( chars );
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

    public static Integer readInt( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
    {
        buffer.clear();
        buffer.limit( 4 );
        int read = channel.read( buffer );
        if ( read < 4 )
        {
            return null;
        }
        buffer.flip();
        int result = buffer.getInt();
        return result;
    }

    public static void writeLengthAndString( FileChannel channel, ByteBuffer buffer, String value )
            throws IOException
    {
        char[] chars = value.toCharArray();
        int length = chars.length;
        writeInt( channel, buffer, length );
        writeChars( channel, buffer, chars );
    }
    
    private static void writeChars( FileChannel channel, ByteBuffer buffer, char[] chars )
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
    
    static void writeInt( FileChannel channel, ByteBuffer buffer, int value ) throws IOException
    {
        buffer.clear();
        buffer.putInt( value );
        buffer.flip();
        channel.write( buffer );
    }
}
