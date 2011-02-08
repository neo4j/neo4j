package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

public class ToNetworkStoreWriter implements StoreWriter
{
    private final ChannelBuffer targetBuffer;

    public ToNetworkStoreWriter( ChannelBuffer targetBuffer )
    {
        this.targetBuffer = targetBuffer;
    }
    
    public void write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
            boolean hasData ) throws IOException
    {
        char[] chars = path.toCharArray();
        targetBuffer.writeShort( chars.length );
        Protocol.writeChars( targetBuffer, chars );
        targetBuffer.writeByte( hasData ? 1 : 0 );
        // TODO Make use of temporaryBuffer?
        BlockLogBuffer buffer = new BlockLogBuffer( targetBuffer );
        if ( hasData )
        {
            buffer.write( data );
            buffer.done();
        }
    }

    public void done()
    {
        targetBuffer.writeShort( 0 );
    }
}
