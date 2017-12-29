package org.neo4j.causalclustering.common;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;

public interface ChannelService<T extends AbstractBootstrap, C extends Channel>
{
    T  bootstrap( EventLoopContext<C>  eventLoopContext );

    void start() throws Throwable;

    void closeChannels() throws Throwable;
}
