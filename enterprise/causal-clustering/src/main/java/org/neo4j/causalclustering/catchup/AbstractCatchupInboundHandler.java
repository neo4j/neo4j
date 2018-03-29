package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractCatchupInboundHandler<T> extends SimpleChannelInboundHandler<T>
{
    protected final CatchUpResponseHandler handler;
    protected final CatchupClientProtocol protocol;

    public AbstractCatchupInboundHandler( CatchUpResponseHandler catchUpResponseHandler, CatchupClientProtocol catchupClientProtocol )
    {
        this.handler = catchUpResponseHandler;
        this.protocol = catchupClientProtocol;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        handler.onChannelInactive();
        protocol.expect( CatchupClientProtocol.State.MESSAGE_TYPE );
    }
}
