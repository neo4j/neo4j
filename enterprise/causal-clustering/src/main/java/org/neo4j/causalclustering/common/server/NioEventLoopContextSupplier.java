package org.neo4j.causalclustering.common.server;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.EventLoopContext;

public class NioEventLoopContextSupplier implements Supplier<EventLoopContext<NioServerSocketChannel>>
{
    private final ThreadFactory threadFactory;
    private final int threads;

    public NioEventLoopContextSupplier( ThreadFactory threadFactory, int threads  )
    {
        this.threadFactory = threadFactory;
        this.threads = threads;
    }

    public NioEventLoopContextSupplier( ThreadFactory threadFactory  )
    {
        this(threadFactory, 0);
    }

    @Override
    public EventLoopContext<NioServerSocketChannel> get()
    {
        return new EventLoopContext<>( new NioEventLoopGroup( threads, threadFactory ), NioServerSocketChannel.class );
    }
}
