package org.neo4j.causalclustering.common.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ServerChannel;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.common.ChannelService;
import org.neo4j.causalclustering.common.EventLoopContext;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ServerBindToChannel<C extends ServerChannel> implements ChannelService<ServerBootstrap,C>
{
    private final Supplier<InetSocketAddress> addressSuppler;
    private final Log log;
    private final Log userLog;
    private final Function<EventLoopContext<C>,ServerBootstrap> bootstrapper;
    private ServerBootstrap serverBootstrap;
    private Channel channel;

    public ServerBindToChannel( Supplier<InetSocketAddress> addressSuppler, LogProvider logProvider, LogProvider
            userLog, Function<EventLoopContext<C>,ServerBootstrap> bootstrapper )
    {
        this.addressSuppler = addressSuppler;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLog.getLog( getClass() );
        this.bootstrapper = bootstrapper;
    }

    private void bind() throws Throwable
    {
        if ( serverBootstrap == null )
        {
            throw new IllegalStateException( "The Channel Manager has not been bootstrapped." );
        }
        InetSocketAddress localAddress = addressSuppler.get();
        ChannelFuture channelFuture =
                serverBootstrap.bind( localAddress ).awaitUninterruptibly();
        if ( channelFuture.isSuccess() )
        {
            channel = channelFuture.channel();
        }
        else
        {
            handleUnsuccessfulBindCause( channelFuture.cause(), localAddress );
        }
    }

    @Override
    public void bootstrap( EventLoopContext<C> eventLoopContext )
    {
        serverBootstrap = bootstrapper.apply( eventLoopContext );
    }

    @Override
    public synchronized void start() throws Throwable
    {
        if ( channel != null )
        {
            throw new IllegalStateException( "Already running" );
        }
        bind();
    }

    @Override
    public synchronized void closeChannels() throws Throwable
    {
        if ( channel == null )
        {
            log.info( "Already stopped" );
            return;
        }
        try
        {
            channel.close().syncUninterruptibly();
        }
        finally
        {
            channel = null;
        }
    }

    private void handleUnsuccessfulBindCause( Throwable cause, InetSocketAddress localAddress ) throws Exception
    {
        if ( cause == null )
        {
            throw new IllegalArgumentException( "Cause cannot be null" );
        }
        if ( cause instanceof BindException )
        {
            userLog.error(
                    "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                    " with value: " + localAddress );
            log.error(
                    "Address is already bound for setting: " + CausalClusteringSettings.transaction_listen_address +
                    " with value: " + localAddress, cause );
            throw (BindException) cause;
        }
        else
        {
            log.error( "Failed to start application", cause );
            throw new RuntimeException( cause );
        }
    }
}