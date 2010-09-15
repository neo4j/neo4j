package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public class InMemoryLogBuffer implements LogBuffer, ReadableByteChannel
{
    private byte[] bytes = new byte[1000];
    private int writeIndex;
    private int readIndex;
    private ByteBuffer bufferForConversions = ByteBuffer.wrap( new byte[100] );
    
    public InMemoryLogBuffer()
    {
    }
    
    private void ensureArrayCapacityPlus( int plus )
    {
        while ( writeIndex+plus > bytes.length ) 
        {
            byte[] tmp = bytes;
            bytes = new byte[bytes.length*2];
            System.arraycopy( tmp, 0, bytes, 0, tmp.length );
        }
    }

    private LogBuffer flipAndPut()
    {
        ensureArrayCapacityPlus( bufferForConversions.limit() );
        System.arraycopy( bufferForConversions.flip().array(), 0, bytes, writeIndex,
                bufferForConversions.limit() );
        writeIndex += bufferForConversions.limit();
        return this;
    }
    
    public LogBuffer put( byte b ) throws IOException
    {
        ensureArrayCapacityPlus( 1 );
        bytes[writeIndex++] = b;
        return this;
    }

    public LogBuffer putInt( int i ) throws IOException
    {
        ((ByteBuffer) bufferForConversions.clear()).putInt( i );
        return flipAndPut();
    }

    public LogBuffer putLong( long l ) throws IOException
    {
        ((ByteBuffer) bufferForConversions.clear()).putLong( l );
        return flipAndPut();
    }

    public LogBuffer put( byte[] bytes ) throws IOException
    {
        ensureArrayCapacityPlus( bytes.length );
        System.arraycopy( bytes, 0, this.bytes, writeIndex, bytes.length );
        writeIndex += bytes.length;
        return this;
    }

    public LogBuffer put( char[] chars ) throws IOException
    {
        ensureConversionBufferCapacity( chars.length*2 );
        bufferForConversions.clear();
        for ( char ch : chars )
        {
            bufferForConversions.putChar( ch );
        }
        return flipAndPut();
    }

    private void ensureConversionBufferCapacity( int length )
    {
        if ( bufferForConversions.capacity() < length )
        {
            bufferForConversions = ByteBuffer.wrap( new byte[length*2] );
        }
    }

    public void force() throws IOException
    {
    }

    public long getFileChannelPosition() throws IOException
    {
        return this.readIndex;
    }

    public FileChannel getFileChannel()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen()
    {
        return true;
    }

    public void close() throws IOException
    {
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        if ( readIndex >= writeIndex )
        {
            return -1;
        }
        
        int actualLengthToRead = Math.min( dst.limit(), writeIndex-readIndex );
        try
        {
            dst.put( bytes, readIndex, actualLengthToRead );
            return actualLengthToRead;
        }
        finally
        {
            readIndex += actualLengthToRead;
        }
    }
}
