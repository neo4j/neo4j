/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
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
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

/**
 * TCP version of a Networked Instance. This handles receiving messages to be consumed by local state-machines and
 * sending outgoing messages
 */
public class NetworkReceiver
        implements MessageSource, Lifecycle
{


    public interface Configuration
    {
        HostnamePort clusterServer();

        int defaultPort();

        String name(); // Name of this cluster instance. Null in most cases, but tools may use e.g. "Backup"
    }

    public interface NetworkChannelsListener
    {
        void listeningAt( URI me );

        void channelOpened( URI to );

        void channelClosed( URI to );
    }

    public static final String CLUSTER_SCHEME = "cluster";
    public static final String INADDR_ANY = "0.0.0.0";

    private ChannelGroup channels;

    // Receiving
    private NioServerSocketChannelFactory nioChannelFactory;
    private ServerBootstrap serverBootstrap;
    private Iterable<MessageProcessor> processors = Listeners.newListeners();

    private Configuration config;
    private StringLogger msgLog;

    private Map<URI, Channel> connections = new ConcurrentHashMap<URI, Channel>();
    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    volatile boolean bindingDetected = false;

    public NetworkReceiver( Configuration config, Logging logging )
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
        channels = new DefaultChannelGroup();

        // Listen for incoming connections
        nioChannelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool( new NamedThreadFactory( "Cluster boss" ) ),
                Executors.newFixedThreadPool( 2, new NamedThreadFactory( "Cluster worker" ) ), 2 );
        serverBootstrap = new ServerBootstrap( nioChannelFactory );
        serverBootstrap.setOption("child.tcpNoDelay", true);
        serverBootstrap.setPipelineFactory( new NetworkNodePipelineFactory() );

        int[] ports = config.clusterServer().getPorts();

        int minPort = ports[0];
        int maxPort = ports.length == 2 ? ports[1] : minPort;

        // Try all ports in the given range
        listen( minPort, maxPort );
    }

    @Override
    public void stop()
            throws Throwable
    {
        msgLog.debug( "Shutting down NetworkReceiver" );

        channels.close().awaitUninterruptibly();
        serverBootstrap.releaseExternalResources();
        msgLog.debug( "Shutting down NetworkReceiver complete" );
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
                InetSocketAddress localAddress;
                if ( address == null || address.equals( INADDR_ANY ))
                {
                    localAddress = new InetSocketAddress( checkPort );
                }
                else
                {
                    host = InetAddress.getByName( address );
                    localAddress = new InetSocketAddress( host, checkPort );
                }

                Channel listenChannel = serverBootstrap.bind( localAddress );

                listeningAt( getURI( localAddress ) );

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

    private URI getURI( InetSocketAddress address ) throws URISyntaxException
    {
        String uri;

        if (address.getAddress().getHostAddress().startsWith( "0" ))
            uri =  CLUSTER_SCHEME + "://0.0.0.0:"+address.getPort(); // Socket.toString() already prepends a /
        else
            uri = CLUSTER_SCHEME + "://" + address.getAddress().getHostAddress()+":"+address.getPort(); // Socket.toString() already prepends a /

        // Add name if given
        if (config.name() != null)
            uri += "/?name="+config.name();

        return URI.create( uri );
    }

    public void listeningAt( final URI me )
    {
        Listeners.notifyListeners( listeners, new Listeners.Notification<NetworkChannelsListener>()
        {
            @Override
            public void notify( NetworkChannelsListener listener )
            {
                listener.listeningAt( me );
            }
        } );
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

    public void addNetworkChannelsListener( NetworkChannelsListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    private class NetworkNodePipelineFactory
            implements ChannelPipelineFactory
    {
        @Override
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast( "frameDecoder",new ObjectDecoder( 1024 * 1000, NetworkNodePipelineFactory.this.getClass().getClassLoader() ) );
            pipeline.addLast( "serverHandler", new MessageReceiver() );
            return pipeline;
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
            if (!bindingDetected)
            {
                InetSocketAddress local = ((InetSocketAddress)event.getChannel().getLocalAddress());
                bindingDetected = true;
                listeningAt( getURI( local ) );
            }

            final Message message = (Message) event.getMessage();

            // Fix FROM header since sender cannot know it's correct IP/hostname
            InetSocketAddress remote = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
            String remoteAddress = remote.getAddress().getHostAddress();
            URI fromHeader = URI.create( message.getHeader( Message.FROM ) );
            fromHeader = URI.create(fromHeader.getScheme()+"://"+remoteAddress + ":" + fromHeader.getPort());
            message.setHeader( Message.FROM, fromHeader.toASCIIString() );

            msgLog.debug( "Received:" + message );
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
            if ( !(e.getCause() instanceof ConnectException) )
            {
                msgLog.error( "Receive exception:", e.getCause() );
            }
        }
    }
}
