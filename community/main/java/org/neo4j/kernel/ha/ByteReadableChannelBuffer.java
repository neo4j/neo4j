package org.neo4j.kernel.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;

class ByteReadableChannelBuffer implements ReadableByteChannel
{
    private ChannelBuffer buffer;

    ByteReadableChannelBuffer( ChannelBuffer buffer )
    {
        this.buffer = buffer;
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        if ( !buffer.readable() ) return -1;
        int size = Math.min( buffer.readableBytes(), dst.limit() - dst.position() );
        ByteBufferBackedChannelBuffer wrapper = new ByteBufferBackedChannelBuffer( dst );
        wrapper.clear(); // Reset the writer position
        buffer.readBytes( wrapper, size );
        dst.position( dst.position() + size );
        return size;
    }

    public void close()
    {
        buffer = null;
    }

    public boolean isOpen()
    {
        return buffer != null;
    }
}
