package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class RequestDecoder extends FrameDecoder
{
    private TransactionDataReader data = null;

    @Override
    protected Object decode( ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer )
            throws Exception
    {
        if ( data != null )
        {
            Object result = data.read( buffer );
            if ( result != null ) data = null; // Done reading transaction data
            return result;
        }
        int pos = buffer.readerIndex();
        Object result = null;
        try
        {
            RequestType requestType = RequestType.get( buffer.readUnsignedByte() );
            result = requestType.readRequest( buffer );
        }
        finally
        {
            if ( result == null ) /*reset reader*/buffer.readerIndex( pos );
        }
        if ( result instanceof TransactionDataReader )
        {
            data = (TransactionDataReader) result;
            return null;
        }
        else
        {
            return result;
        }
    }
}
