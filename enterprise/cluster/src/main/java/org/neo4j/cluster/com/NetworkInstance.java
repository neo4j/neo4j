/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * TCP version of a Networked Instance. This handles receiving messages to be consumed by local statemachines and
 * sending
 * outgoing messages
 */
public class NetworkInstance
        implements MessageProcessor, MessageSource, Lifecycle
{
    public interface Configuration
    {
        HostnamePort clusterServer();
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
    private ExecutorService executor;
    private ServerBootstrap serverBootstrap;
    private ServerSocketChannelFactory nioChannelFactory;
    //    private Channel channel;
    private Iterable<MessageProcessor> processors = Listeners.newListeners();

    // Sending
    private ClientBootstrap clientBootstrap;

    private Configuration config;
    private StringLogger msgLog;
    private URI me;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();
    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    public NetworkInstance( Configuration config, StringLogger logger )
    {
        this.config = config;
        this.msgLog = logger;
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
        executor = Executors.newSingleThreadExecutor( new NamedThreadFactory( "Cluster messenger" ) );
        channels = new DefaultChannelGroup();

        // Listen for incoming connections
        nioChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool( new NamedThreadFactory( "Cluster boss" ) ),
                Executors.newFixedThreadPool( 10, new NamedThreadFactory( "Cluster worker" ) ) );
        serverBootstrap = new ServerBootstrap( nioChannelFactory );
        serverBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        int[] ports = config.clusterServer().getPorts();

        int minPort = ports[0];
        int maxPort = ports.length == 2 ? ports[1] : minPort;

        // Start client bootstrap
        clientBootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
                Executors.newSingleThreadExecutor( new NamedThreadFactory( "Cluster client boss" ) ),
                Executors.newFixedThreadPool( 10, new NamedThreadFactory( "Cluster client worker" ) ) ) );
        clientBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        // Try all ports in the given range
        listen( minPort, maxPort );
    }

    @Override
    public void stop()
            throws Throwable
    {
        channels.close().awaitUninterruptibly();
        nioChannelFactory.releaseExternalResources();
        clientBootstrap.releaseExternalResources();

        executor.shutdownNow();
        if ( !executor.awaitTermination( 10, TimeUnit.SECONDS ) )
        {
            msgLog.warn( "Could not shut down executor" );
        }
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

    public void receive( Message message )
    {
        for ( MessageProcessor listener : processors )
        {
            try
            {
                listener.process( message );
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }
    }

    // MessageProcessor implementation
    @Override
    public void process( Message<? extends MessageType> message )
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

    private synchronized void send( Message message )
    {
        URI to = null;
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
//            msgLog.error("Could not connect to:" + to, true);
            return;
        }

        try
        {
            if ( msgLog.isDebugEnabled() )
            {
                msgLog.debug( "Sending to " + to + ": " + message );
            }
            channel.write( message );
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

    public URI getMe()
    {
        return me;
    }

    public Channel getChannel( URI uri )
    {
        return connections.get( uri );
    }

    public void addNetworkChannelsListener( NetworkChannelsListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    public void removeNetworkChannelsListener( NetworkChannelsListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    private Channel openChannel( URI clusterUri )
    {
        SocketAddress address = new InetSocketAddress( clusterUri.getHost(), clusterUri.getPort() );

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
                    new ObjectDecoder( 1024 * 1000, NetworkNodePipelineFactory.this.getClass().getClassLoader() ) );
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
//            msgLog.logMessage("Received:" + message, true);
//            receive( message );
            executor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    receive( message );
                }
            } );

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
            if ( !(e.getCause() instanceof ConnectException) )
            {
                msgLog.error( "Receive exception:", e.getCause() );
            }
        }
    }
}
