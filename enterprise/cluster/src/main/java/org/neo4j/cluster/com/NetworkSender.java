/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.com;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.cluster.com.NetworkReceiver.CLUSTER_SCHEME;
import static org.neo4j.helpers.NamedThreadFactory.daemon;

/**
 * TCP version of sending messages. This handles sending messages from state machines to other instances
 * in the cluster.
 */
public class NetworkSender
        implements MessageSender, Lifecycle
{
    public interface Monitor
        extends NamedThreadFactory.Monitor
    {
        void queuedMessage( Message message );

        void sentMessage( Message message );
    }

    public interface Configuration
    {
        int defaultPort(); // This is the default port to try to connect to

        int port(); // This is the port we are listening on
    }

    public interface NetworkChannelsListener
    {
        void channelOpened( URI to );

        void channelClosed( URI to );
    }

    private ChannelGroup channels;

    // Sending
    // One executor for each receiving instance, so that one blocking instance cannot block others receiving messages
    private final Map<URI, ExecutorService> senderExecutors = new HashMap<>();
    private final Set<URI> failedInstances = new HashSet<>(); // Keeps track of what instances we have failed to open
    // connections to
    private ClientBootstrap clientBootstrap;

    private final Monitor monitor;
    private final Configuration config;
    private final NetworkReceiver receiver;
    private final Log msgLog;
    private URI me;

    private final Map<URI, Channel> connections = new ConcurrentHashMap<>();
    private final Listeners<NetworkChannelsListener> listeners = new Listeners<>();

    private volatile boolean paused;

    public NetworkSender( Monitor monitor, Configuration config, NetworkReceiver receiver, LogProvider logProvider )
    {
        this.monitor = monitor;
        this.config = config;
        this.receiver = receiver;
        this.msgLog = logProvider.getLog( getClass() );
        me = URI.create( CLUSTER_SCHEME + "://0.0.0.0:" + config.port() );
        receiver.addNetworkChannelsListener( new NetworkReceiver.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                NetworkSender.this.me = me;
            }

            @Override
            public void channelOpened( URI to )
            {
            }

            @Override
            public void channelClosed( URI to )
            {
            }
        } );
    }

    @Override
    public void init()
    {
        ThreadRenamingRunnable.setThreadNameDeterminer( ThreadNameDeterminer.CURRENT );
    }

    @Override
    public void start()
    {
        channels = new DefaultChannelGroup();

        // Start client bootstrap
        clientBootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                Executors.newSingleThreadExecutor( daemon( "Cluster client boss", monitor ) ),
                Executors.newFixedThreadPool( 2, daemon( "Cluster client worker", monitor ) ), 2 ) );
        clientBootstrap.setOption( "tcpNoDelay", Boolean.TRUE );
        clientBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        msgLog.debug( "Started NetworkSender for " + toString( config ) );
    }

    private String toString( Configuration config )
    {
        return "defaultPort:" + config.defaultPort() + ", port:" + config.port();
    }

    @Override
    public void stop()
            throws Throwable
    {
        msgLog.debug( "Shutting down NetworkSender" );
        for ( ExecutorService executorService : senderExecutors.values() )
        {
            executorService.shutdown();
        }
        long totalWaitTime = 0;
        long maxWaitTime = SECONDS.toMillis( 5 );
        for ( Map.Entry<URI, ExecutorService> entry : senderExecutors.entrySet() )
        {
            URI targetAddress = entry.getKey();
            ExecutorService executorService = entry.getValue();

            long start = currentTimeMillis();
            if ( !executorService.awaitTermination( maxWaitTime - totalWaitTime, MILLISECONDS ) )
            {
                msgLog.warn( "Could not shut down send executor towards: " + targetAddress );
                break;
            }
            totalWaitTime += currentTimeMillis() - start;
        }
        senderExecutors.clear();

        channels.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        msgLog.debug( "Shutting down NetworkSender for " + toString( config ) + " complete" );
    }

    @Override
    public void shutdown()
    {
    }

    // MessageSender implementation
    @Override
    public void process( final List<Message<? extends MessageType>> messages )
    {
        for ( Message<? extends MessageType> message : messages )
        {
            try
            {
                process( message );
            }
            catch ( Exception e )
            {
                msgLog.warn( "Error sending message " + message + "(" + e.getMessage() + ")" );
            }
        }
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        if ( !paused )
        {
            if ( message.hasHeader( Message.HEADER_TO ) )
            {
                send( message );
            }
            else
            {
                // Internal message
                receiver.receive( message );
            }
        }
        return true;
    }

    public void setPaused( boolean paused )
    {
        this.paused = paused;
    }

    private URI getURI( InetSocketAddress address ) throws URISyntaxException
    {
        return new URI( CLUSTER_SCHEME + ":/" + address ); // Socket.toString() already prepends a /
    }

    private synchronized void send( final Message message )
    {
        monitor.queuedMessage( message );

        final URI to = URI.create( message.getHeader( Message.HEADER_TO ) );

        ExecutorService senderExecutor = senderExecutors.computeIfAbsent( to, t -> Executors
                .newSingleThreadExecutor( new NamedThreadFactory( "Cluster Sender " + t.toASCIIString(), monitor ) ) );

        senderExecutor.submit( () ->
        {
            Channel channel = getChannel( to );

            try
            {
                if ( channel == null )
                {
                    channel = openChannel( to );
                    openedChannel( to, channel );

                    // Instance could be connected to, remove any marker of it being failed
                    failedInstances.remove( to );
                }
            }
            catch ( Exception e )
            {
                // Only print out failure message on first fail
                if ( !failedInstances.contains( to ) )
                {
                    msgLog.warn( e.getMessage() );
                    failedInstances.add( to );
                }

                return;
            }

            try
            {
                // Set HEADER_FROM header
                message.setHeader( Message.HEADER_FROM, me.toASCIIString() );

                msgLog.debug( "Sending to " + to + ": " + message );

                ChannelFuture future = channel.write( message );
                future.addListener( future1 ->
                {
                    monitor.sentMessage( message );

                    if ( !future1.isSuccess() )
                    {
                        msgLog.debug( "Unable to write " + message + " to " + future1.getChannel(),
                                future1.getCause() );
                        closedChannel( future1.getChannel() );

                        // Try again
                        send( message );
                    }
                } );
            }
            catch ( Exception e )
            {
                if ( Exceptions.contains( e, ClosedChannelException.class ) )
                {
                    msgLog.warn( "Could not send message, because the connection has been closed." );
                }
                else
                {
                    msgLog.warn( "Could not send message", e );
                }
                channel.close();
            }
        } );
    }

    protected void openedChannel( URI uri, Channel ctxChannel )
    {
        connections.put( uri, ctxChannel );

        listeners.notify( listener -> listener.channelOpened( uri ) );
    }

    protected void closedChannel( Channel channelClosed )
    {
        /*
         * Netty channels do not have the remote address set when closed (technically, when not connected). So
         * we need to do a reverse lookup
         */
        URI to = null;
        for ( Map.Entry<URI, Channel> uriChannelEntry : connections.entrySet() )
        {
            if ( uriChannelEntry.getValue().equals( channelClosed ) )
            {
                to = uriChannelEntry.getKey();
                break;
            }
        }

        if ( to == null )
        {
            /*
             * This is normal to happen if a channel fails to open - channelOpened() will not be called and the
             * association with the URI will not exist, but channelClosed() will be called anyway.
             */
            return;
        }

        connections.remove( to );

        URI uri = to;

        listeners.notify( listener -> listener.channelClosed( uri ) );
    }

    public Channel getChannel( URI uri )
    {
        return connections.get( uri );
    }

    public void addNetworkChannelsListener( NetworkChannelsListener listener )
    {
        listeners.add( listener );
    }

    private Channel openChannel( URI clusterUri )
    {
        SocketAddress destination = new InetSocketAddress( clusterUri.getHost(),
                clusterUri.getPort() == -1 ? config.defaultPort() : clusterUri.getPort() );
        // We must specify the origin address in case the server has multiple IPs per interface
        SocketAddress origin = new InetSocketAddress( me.getHost(), 0 );

        msgLog.info( "Attempting to connect from " + origin + " to " + destination );
        ChannelFuture channelFuture = clientBootstrap.connect( destination, origin );
        channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );

        if ( channelFuture.isSuccess() )
        {
            Channel channel = channelFuture.getChannel();
            msgLog.info( "Connected from " + channel.getLocalAddress() + " to " + channel.getRemoteAddress() );
            return channel;

        }

        Throwable cause = channelFuture.getCause();
        msgLog.info( "Failed to connect to " + destination + " due to: " + cause );

        throw new ChannelOpenFailedException( cause );
    }

    private class NetworkNodePipelineFactory
            implements ChannelPipelineFactory
    {
        @Override
        public ChannelPipeline getPipeline()
        {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast( "frameEncoder", new ObjectEncoder( 2048 ) );
            pipeline.addLast( "sender", new NetworkMessageSender() );
            return pipeline;
        }
    }

    private class NetworkMessageSender
            extends SimpleChannelHandler
    {
        private Throwable lastException;

        @Override
        public void channelConnected( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception
        {
            Channel ctxChannel = ctx.getChannel();
            openedChannel( getURI( (InetSocketAddress) ctxChannel.getRemoteAddress() ), ctxChannel );
            channels.add( ctxChannel );
        }

        @Override
        public void channelClosed( ChannelHandlerContext ctx, ChannelStateEvent e )
        {
            closedChannel( ctx.getChannel() );
            channels.remove( ctx.getChannel() );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e )
        {
            Throwable cause = e.getCause();
            if ( !(cause instanceof ConnectException || cause instanceof RejectedExecutionException) )
            {
                // If we keep getting the same exception, only output the first one
                if ( lastException != null && !lastException.getClass().equals( cause.getClass() ) )
                {
                    msgLog.error( "Receive exception:", cause );
                    lastException = cause;
                }
            }
        }

        @Override
        public void writeComplete( ChannelHandlerContext ctx, WriteCompletionEvent e ) throws Exception
        {
            if ( lastException != null )
            {
                msgLog.error( "Recovered from:", lastException );
                lastException = null;
            }
            super.writeComplete( ctx, e );
        }
    }
}
