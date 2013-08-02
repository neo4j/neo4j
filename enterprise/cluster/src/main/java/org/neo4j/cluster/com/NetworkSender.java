/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.com;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.cluster.com.NetworkReceiver.URI_PROTOCOL;

/**
 * TCP version of sending messages. This handles sending messages from state machines to other instances
 * in the cluster.
 */
public class NetworkSender
        implements MessageSender, Lifecycle
{
    public interface Configuration
    {
        int defaultPort();
    }

    public interface NetworkChannelsListener
    {
        void channelOpened( URI to );

        void channelClosed( URI to );
    }

    private ChannelGroup channels;

    // Sending
    private ExecutorService sendExecutor;
    private ClientBootstrap clientBootstrap;

    private Configuration config;
    private final NetworkReceiver receiver;
    private StringLogger msgLog;
    private URI me;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();
    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    public NetworkSender( Configuration config, NetworkReceiver receiver, Logging logging )
    {
        this.config = config;
        this.receiver = receiver;
        this.msgLog = logging.getMessagesLog( getClass() );
        receiver.addNetworkChannelsListener( new NetworkReceiver.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me)
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
        });
    }

    @Override
    public void init()
            throws Throwable
    {
        ThreadRenamingRunnable.setThreadNameDeterminer( ThreadNameDeterminer.CURRENT );
    }

    @Override
    public void start()
            throws Throwable
    {
        sendExecutor = Executors.newSingleThreadExecutor( new NamedThreadFactory( "Cluster Sender" ) );
        channels = new DefaultChannelGroup();

        // Start client bootstrap
        clientBootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                Executors.newSingleThreadExecutor( new NamedThreadFactory( "Cluster client boss" ) ),
                Executors.newFixedThreadPool( 2, new NamedThreadFactory( "Cluster client worker" ) ), 2 ) );
        clientBootstrap.setOption( "tcpNoDelay", true );
        clientBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );
        clientBootstrap.setOption("tcpNoDelay", true);
    }

    @Override
    public void stop()
            throws Throwable
    {
        msgLog.debug( "Shutting down NetworkSender" );
        sendExecutor.shutdown();
        if ( !sendExecutor.awaitTermination( 10, TimeUnit.SECONDS ) )
        {
            msgLog.warn( "Could not shut down send executor" );
        }

        channels.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        msgLog.debug( "Shutting down NetworkSender complete" );
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    // MessageSender implementation
    @Override
    public void process( final List<Message<? extends MessageType>> messages )
    {
        sendExecutor.submit( new Runnable()
        {
            @Override
            public void run()
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
        } );
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        if ( message.hasHeader( Message.TO ) )
        {
            String to = message.getHeader( Message.TO );

            if ( to.equals( Message.BROADCAST ) )
            {
                broadcast( message );
            }
            else if ( to.equals( me.toString() ) )
            {
                receiver.receive( message );
            }
            else
            {
                send( message );
            }
        }
        else
        {
            // Internal message
            receiver.receive( message );
        }
        return true;
    }


    private URI getURI( InetSocketAddress address ) throws URISyntaxException
    {
        return new URI( URI_PROTOCOL + ":/" + address ); // Socket.toString() already prepends a /
    }

    private void broadcast( Message message )
    {
        for ( int i = 1234; i < 1234 + 2; i++ )
        {
            String to = URI_PROTOCOL + "://127.0.0.1:" + i;

            if ( !to.equals( me.toString() ) )
            {
                message.setHeader( Message.TO, to );
                send( message );
            }
        }
    }

    private synchronized void send( final Message message )
    {
        URI to;
        try
        {
            to = new URI( message.getHeader( Message.TO ) );
        }
        catch ( URISyntaxException e )
        {
            msgLog.error( "Not valid URI:" + message.getHeader( Message.TO ) );
            return;
        }

        Channel channel = getChannel( to );

        try
        {
            if ( channel == null )
            {
                channel = openChannel( to );
                openedChannel( to, channel );
            }
        }
        catch ( Exception e )
        {
            msgLog.debug( "Could not connect to:" + to );
            return;
        }

        try
        {
            msgLog.debug( "Sending to " + to + ": " + message );
            ChannelFuture future = channel.write( message );
            future.addListener( new ChannelFutureListener()
            {
                @Override
                public void operationComplete( ChannelFuture future ) throws Exception
                {
                    if ( !future.isSuccess() )
                    {
                        msgLog.debug( "Unable to write " + message + " to " + future.getChannel(), future.getCause() );
                    }
                }
            } );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            channel.close();
            closedChannel( to );
        }
    }

    protected void openedChannel( final URI uri, Channel ctxChannel )
    {
        connections.put( uri, ctxChannel );

        Listeners.notifyListeners( listeners, new Listeners.Notification<NetworkChannelsListener>()
        {
            @Override
            public void notify( NetworkChannelsListener listener )
            {
                listener.channelOpened( uri );
            }
        } );
    }

    protected void closedChannel( final URI uri )
    {
        Channel channel = connections.remove( uri );
        if ( channel != null )
        {
            channel.close();
        }

        Listeners.notifyListeners( listeners, new Listeners.Notification<NetworkChannelsListener>()
        {
            @Override
            public void notify( NetworkChannelsListener listener )
            {
                listener.channelClosed( uri );
            }
        } );
    }

    public Channel getChannel( URI uri )
    {
        return connections.get( uri );
    }

    public void addNetworkChannelsListener( NetworkChannelsListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    private Channel openChannel( URI clusterUri )
    {
        SocketAddress address = new InetSocketAddress( clusterUri.getHost(), clusterUri.getPort() == -1 ? config
                .defaultPort() : clusterUri.getPort() );

        ChannelFuture channelFuture = clientBootstrap.connect( address );

        try
        {
            if ( channelFuture.await( 5, TimeUnit.SECONDS ) && channelFuture.getChannel().isConnected() )
            {
                msgLog.info( me + " opened a new channel to " + address );
                return channelFuture.getChannel();
            }

            String msg = "Client could not connect to " + address;
            throw new ChannelOpenFailedException( msg );
        }
        catch ( InterruptedException e )
        {
            msgLog.warn( "Interrupted", e );
            // Restore the interrupt status since we are not rethrowing InterruptedException
            // We may be running in an executor and we could fail to be terminated
            Thread.currentThread().interrupt();
            throw new ChannelOpenFailedException( e );
        }
    }

    private class NetworkNodePipelineFactory
            implements ChannelPipelineFactory
    {
        @Override
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addFirst( "log", new LoggingHandler() );
            pipeline.addLast( "frameEncoder", new ObjectEncoder( 2048 ) );
            pipeline.addLast( "sender", new NetworkSender.MessageSender() );
            return pipeline;
        }
    }

    private class MessageSender
            extends SimpleChannelHandler
    {
        @Override
        public void channelOpen( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception
        {
            Channel ctxChannel = ctx.getChannel();
            openedChannel( getURI( (InetSocketAddress) ctxChannel.getRemoteAddress() ), ctxChannel );
            channels.add( ctxChannel );
        }

        @Override
        public void messageReceived( ChannelHandlerContext ctx, MessageEvent event ) throws Exception
        {
            final Message message = (Message) event.getMessage();
            msgLog.debug( "Received:" + message );
            receiver.receive( message );
        }

        @Override
        public void channelDisconnected( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception
        {
            closedChannel( getURI( (InetSocketAddress) ctx.getChannel().getRemoteAddress() ) );
        }

        @Override
        public void channelClosed( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception
        {
            closedChannel( getURI( (InetSocketAddress) ctx.getChannel().getRemoteAddress() ) );
            channels.remove( ctx.getChannel() );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            Throwable cause = e.getCause();
            if ( !(cause instanceof ConnectException) )
            {
                msgLog.error( "Receive exception:", cause );
            }
        }
    }
}
