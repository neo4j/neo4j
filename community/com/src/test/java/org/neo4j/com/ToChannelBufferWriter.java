package org.neo4j.com;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

public class ToChannelBufferWriter implements MadeUpWriter
{
    private final ChannelBuffer target;

    public ToChannelBufferWriter( ChannelBuffer target )
    {
        this.target = target;
    }

    @Override
    public void write( ReadableByteChannel data )
    {
        BlockLogBuffer blockBuffer = new BlockLogBuffer( target );
        try
        {
            blockBuffer.write( data );
            blockBuffer.done();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
