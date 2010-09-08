package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class ResponseDecoder extends FrameDecoder
{
    private static final int HEADER_SIZE = TransactionDataReader.HEADER_SIZE;
    private TransactionDataReader data = null;

    @Override
    protected Object decode( ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer )
            throws Exception
    {
        if ( data != null )
        {
            Object result = data.read( buffer );
            if ( result != null ) data = null; // Done reading this transaction
            return result;
        }
        if ( buffer.readableBytes() >= HEADER_SIZE )
        {
            int header = buffer.getShort( buffer.readerIndex() );
            if ( header < 0 )
            {
                header = -header;
                data = TransactionDataReader.tryInitStream( header, buffer );
            }
            else
            {
                if ( buffer.readableBytes() < HEADER_SIZE + header )
                {
                    return null;
                }
                else
                {
                    ChannelBuffer frame = buffer.factory().getBuffer( header );
                    int index = buffer.readerIndex() + HEADER_SIZE;
                    frame.writeBytes( buffer, index, header );
                    buffer.readerIndex( index + header );
                    return frame;
                }
            }
        }
        return null;
    }
}
