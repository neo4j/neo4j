package org.neo4j.causalclustering.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

public class EventLoopContext<C extends Channel>
{
    private final EventLoopGroup eventExecutors;
    private final Class<C> channelClass;

    public EventLoopContext( EventLoopGroup eventExecutors, Class<C> channelClass )
    {
        this.eventExecutors = eventExecutors;
        this.channelClass = channelClass;
    }

    public EventLoopGroup eventExecutors()
    {
        return eventExecutors;
    }

    public Class<C> channelClass()
    {
        return channelClass;
    }
}
