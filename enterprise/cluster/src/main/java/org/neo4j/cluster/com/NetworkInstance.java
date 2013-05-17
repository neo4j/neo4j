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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
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
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

/**
 * TCP version of a Networked Instance. This handles receiving messages to be consumed by local statemachines and
 * sending
 * outgoing messages
 */
public class NetworkInstance
        implements MessageSource, MessageSender, Lifecycle
{
    public interface Configuration
    {
        HostnamePort clusterServer();

        int defaultPort();
    }

    public interface NetworkChannelsListener
    {
        void listeningAt( URI me );

        void channelOpened( URI to );

        void channelClosed( URI to );
    }

    public static final String URI_PROTOCOL = "cluster";

    private ChannelGroup channels;

    // Receiving
    private ExecutorService sendExecutor;
    private NioServerSocketChannelFactory nioChannelFactory;
    private ServerBootstrap serverBootstrap;
    //    private Channel channel;
    private Iterable<MessageProcessor> processors = Listeners.newListeners();

    // Sending
    private ClientBootstrap clientBootstrap;

    private Configuration config;
    private StringLogger msgLog;
    private URI me;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();
    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    public NetworkInstance( Configuration config, Logging logging )
    {
        this.config = config;
        this.msgLog = logging.getMessagesLog( getClass() );
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

        // Listen for incoming connections
        nioChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool( new NamedThreadFactory( "Cluster boss" ) ),
                Executors.newFixedThreadPool( 2, new NamedThreadFactory( "Cluster worker" ) ), 2 );
        serverBootstrap = new ServerBootstrap( nioChannelFactory );
        serverBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        int[] ports = config.clusterServer().getPorts();

        int minPort = ports[0];
        int maxPort = ports.length == 2 ? ports[1] : minPort;

        // Start client bootstrap
        clientBootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                Executors.newSingleThreadExecutor( new NamedThreadFactory( "Cluster client boss" ) ),
                Executors.newFixedThreadPool( 2, new NamedThreadFactory( "Cluster client worker" ) ), 2 ) );
        clientBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        // Try all ports in the given range
        listen( minPort, maxPort );
    }

    @Override
    public void stop()
            throws Throwable
    {
        msgLog.debug( "Shutting down NetworkInstance" );
        sendExecutor.shutdown();
        if ( !sendExecutor.awaitTermination( 10, TimeUnit.SECONDS ) )
        {
            msgLog.warn( "Could not shut down send executor" );
        }

        channels.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
        msgLog.debug( "Shutting down NetworkInstance complete" );
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    private void listen( int minPort, int maxPort )
            throws URISyntaxException, ChannelException, UnknownHostException
    {
        ChannelException ex = null;
        for ( int checkPort = minPort; checkPort <= maxPort; checkPort++ )
        {
            try
            {
                InetAddress host;
                String address = config.clusterServer().getHost();
                if ( address == null )
                {
                    host = InetAddress.getLocalHost();
                }
                else
                {
                    host = InetAddress.getByName( address );
                }

                InetSocketAddress localAddress = new InetSocketAddress( host, checkPort );

                Channel listenChannel = serverBootstrap.bind( localAddress );
                listeningAt( (getURI( (InetSocketAddress) listenChannel.getLocalAddress() )) );

                channels.add( listenChannel );
                return;
            }
            catch ( ChannelException e )
            {
                ex = e;
            }
        }

        nioChannelFactory.releaseExternalResources();
        throw ex;
    }

    // MessageSource implementation
    public void addMessageProcessor( MessageProcessor processor )
    {
        processors = Listeners.addListener( processor, processors );
    }

    @SuppressWarnings("unchecked")
    public void receive( Message message )
    {
        for ( MessageProcessor processor : processors )
        {
            try
            {
                if ( !processor.process( message ) )
                {
                    break;
                }
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }
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
        });
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
                receive( message );
            }
            else
            {
                send( message );
            }
        }
        else
        {
            // Internal message
            receive( message );
        }
        return true;
    }


    private URI getURI( InetSocketAddress address ) throws URISyntaxException
    {
        return new URI( URI_PROTOCOL + ":/" + address ); // Socket.toString() already prepends a /
    }

    public void listeningAt( final URI me )
    {
        this.me = me;

        Listeners.notifyListeners( listeners, new Listeners.Notification<NetworkChannelsListener>()
        {
            @Override
            public void notify( NetworkChannelsListener listener )
            {
                listener.listeningAt( me );
            }
        } );
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
                        msgLog.debug( "Unable to write " + message + " to " + future.getChannel(), future.getCause() );
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
        SocketAddress address = new InetSocketAddress( clusterUri.getHost(), clusterUri.getPort() == -1 ? config.defaultPort() : clusterUri.getPort() );

        ChannelFuture channelFuture = clientBootstrap.connect( address );
//            channelFuture.awaitUninterruptibly( 5, TimeUnit.SECONDS );

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
            addSerialization( pipeline, 1024 * 1000 );
            pipeline.addLast( "serverHandler", new MessageReceiver() );
            return pipeline;
        }

        private void addSerialization( ChannelPipeline pipeline, int frameLength )
        {
            pipeline.addLast( "frameDecoder",
                              new ObjectDecoder( frameLength, ClassResolvers.cacheDisabled(
                                      NetworkNodePipelineFactory.this.getClass().getClassLoader() ) ) );
            pipeline.addLast( "frameEncoder", new ObjectEncoder( 2048 ) );
        }
    }

    private class MessageReceiver
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
            msgLog.debug("Received:" + message);
//            StringBuilder uri = new StringBuilder( "cluster://" ).append( ((InetSocketAddress) event.getRemoteAddress()).getAddress() )
//                    .append( ":" ).append( ((InetSocketAddress) event.getRemoteAddress()).getPort() );
//            message.setHeader( Message.FROM, uri.toString() );
              receive( message );

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
