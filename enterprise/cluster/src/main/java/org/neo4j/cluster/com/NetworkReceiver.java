/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.io.net.Ports;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.NamedThreadFactory.daemon;

/**
 * TCP version of a Networked Instance. This handles receiving messages to be consumed by local state-machines and
 * sending outgoing messages
 */
public class NetworkReceiver
        implements MessageSource, Lifecycle
{
    public interface Monitor
        extends NamedThreadFactory.Monitor
    {
        void receivedMessage( Message message );

        void processedMessage( Message message );
    }


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

    private ChannelGroup channels;
    private NioEventLoopGroup workerGroup;
    private NioEventLoopGroup bossGroup;

    // Receiving
    private Iterable<MessageProcessor> processors = Listeners.newListeners();

    private Monitor monitor;
    private Configuration config;
    private StringLogger msgLog;

    private Map<URI, Channel> connections = new ConcurrentHashMap<>();
    private Iterable<NetworkChannelsListener> listeners = Listeners.newListeners();

    volatile boolean bindingDetected = false;

    public NetworkReceiver( Monitor monitor, Configuration config, Logging logging )
    {
        this.monitor = monitor;
        this.config = config;
        this.msgLog = logging.getMessagesLog( getClass() );
    }

    @Override
    public void init()
            throws Throwable
    {
        // TODO This is not present in Netty 4 - what was the point of this?
//        ThreadRenamingRunnable.setThreadNameDeterminer( ThreadNameDeterminer.CURRENT );
    }

    @Override
    public void start()
            throws Throwable
    {
        // Try binding to any port in the port range
        HostnamePort targetAddress = config.clusterServer();
        try
        {
            bossGroup = new NioEventLoopGroup( 0, daemon( "Cluster boss", monitor ) );
            workerGroup = new NioEventLoopGroup( 2, daemon( "Cluster worker", monitor ));

            ServerBootstrap b = new ServerBootstrap();
            b.group( bossGroup, workerGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_BACKLOG, 100 )
                    .option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT )
                    .childOption( ChannelOption.TCP_NODELAY, true )
                    .childHandler( new NetworkNodePipelineFactory() );

            channels = new DefaultChannelGroup( GlobalEventExecutor.INSTANCE );
            listen( targetAddress.getPorts()[0], targetAddress.getPorts()[1], b );
        }
        catch(Exception e)
        {
            msgLog.logMessage( "Failed to bind server to " + targetAddress, e );
            stop();
            throw e;
        }
    }

    private void listen( int minPort, int maxPort, ServerBootstrap serverBootstrap )
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
                if ( address == null || address.equals( Ports.INADDR_ANY ))
                {
                    localAddress = new InetSocketAddress( checkPort );
                }
                else
                {
                    host = InetAddress.getByName( address );
                    localAddress = new InetSocketAddress( host, checkPort );
                }

                Channel channel = serverBootstrap.bind( localAddress ).sync().channel();
                listeningAt( getURI( localAddress ) );

                channels.add( channel );
                return;
            }
            catch ( ChannelException e )
            {
                ex = e;
            }
            catch ( InterruptedException e )
            {
                throw new ChannelException( "Interrupted while setting up network listener." );
            }
        }
        throw ex;
    }

    @Override
    public void stop()
            throws Throwable
    {
        msgLog.debug( "Shutting down NetworkReceiver" );

        if(channels != null)
        {
            channels.close().awaitUninterruptibly();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        bossGroup.terminationFuture().sync();
        workerGroup.terminationFuture().sync();
        msgLog.debug( "Shutting down NetworkReceiver complete" );
    }

    @Override
    public void shutdown()
            throws Throwable
    {
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

        monitor.processedMessage( message );
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
            extends ChannelInitializer<SocketChannel>
    {
        @Override
        protected void initChannel( SocketChannel ch ) throws Exception
        {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast( "frameDecoder", new ObjectDecoder( 1024 * 1000, ClassResolvers.cacheDisabled(NetworkNodePipelineFactory.this.getClass().getClassLoader()) ) );
            pipeline.addLast( "serverHandler", new MessageReceiver() );
        }
    }

    private class MessageReceiver
            extends ChannelDuplexHandler
    {
        @Override
        public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
        {
            if (!bindingDetected)
            {
                InetSocketAddress local = ((InetSocketAddress)ctx.channel().localAddress());
                bindingDetected = true;
                listeningAt( getURI( local ) );
            }

            final Message message = (Message) msg;

            // Fix FROM header since sender cannot know it's correct IP/hostname
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            String remoteAddress = remote.getAddress().getHostAddress();
            URI fromHeader = URI.create( message.getHeader( Message.FROM ) );
            fromHeader = URI.create(fromHeader.getScheme()+"://"+remoteAddress + ":" + fromHeader.getPort());
            message.setHeader( Message.FROM, fromHeader.toASCIIString() );

            msgLog.debug( "Received:" + message );
            monitor.receivedMessage( message );
            receive( message );
        }

        @Override
        public void channelActive( ChannelHandlerContext ctx ) throws Exception
        {
            Channel ctxChannel = ctx.channel();
            openedChannel( getURI( (InetSocketAddress) ctxChannel.remoteAddress() ), ctxChannel );
            channels.add( ctxChannel );
        }

        @Override
        public void channelInactive( ChannelHandlerContext ctx ) throws Exception
        {
            closedChannel( getURI( (InetSocketAddress) ctx.channel().remoteAddress() ) );
            channels.remove( ctx.channel() );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable e ) throws Exception
        {
            if ( !(e instanceof ConnectException) )
            {
                msgLog.error( "Receive exception:", e );
            }
        }
    }
}
