/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.neo4j.bolt.transport.configuration.EpollConfigurationProvider;
import org.neo4j.bolt.transport.configuration.NioConfigurationProvider;
import org.neo4j.bolt.transport.configuration.ServerConfigurationProvider;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.PortBindException;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;

/**
 * Simple wrapper around Netty boss and selector threads, which allows multiple ports and protocols to be handled
 * by the same set of common worker threads.
 */
public class NettyServer extends LifecycleAdapter
{

    private static final boolean USE_EPOLL = FeatureToggles.flag( NettyServer.class, "useEpoll", true  );
    // Not officially configurable, but leave it modifiable via system properties in case we find we need to
    // change it.
    private static final int NUM_SELECTOR_THREADS = Math.max( 1, Integer.getInteger(
            "org.neo4j.selectorThreads", Runtime.getRuntime().availableProcessors() * 2 ) );

    private final Map<BoltConnector, ProtocolInitializer> bootstrappersMap;
    private final ThreadFactory tf;
    private final ConnectorPortRegister connectionRegister;
    private final Log log;
    private EventLoopGroup bossGroup;
    private EventLoopGroup selectorGroup;

    /**
     * Describes how to initialize new channels for a protocol, and which address the protocol should be bolted into.
     */
    public interface ProtocolInitializer
    {
        ChannelInitializer<io.netty.channel.socket.SocketChannel> channelInitializer();
        ListenSocketAddress address();
    }

    /**
     * @param tf used to create IO threads to listen and handle network events
     * @param initializersMap  function per bolt connector map to bootstrap configured protocols
     * @param connectorRegister register to keep local address information on all configured connectors
     */
    public NettyServer( ThreadFactory tf, Map<BoltConnector, ProtocolInitializer> initializersMap,
                        ConnectorPortRegister connectorRegister, Log log )
    {
        this.bootstrappersMap = initializersMap;
        this.tf = tf;
        this.connectionRegister = connectorRegister;
        this.log = log;
    }

    @Override
    public void start() throws Throwable
    {
        boolean useEpoll = USE_EPOLL && Epoll.isAvailable();
        ServerConfigurationProvider configurationProvider = useEpoll ? EpollConfigurationProvider.INSTANCE :
                                                            NioConfigurationProvider.INSTANCE;
        bossGroup = configurationProvider.createEventLoopGroup(1, tf);

        // These threads handle live channels. Each thread has a set of channels it is responsible for, and it will
        // continuously run a #select() loop to react to new events on these channels.
        selectorGroup = configurationProvider.createEventLoopGroup( NUM_SELECTOR_THREADS, tf );

        // Bootstrap the various ports and protocols we want to handle

        for ( Map.Entry<BoltConnector, ProtocolInitializer> bootstrapEntry : bootstrappersMap.entrySet() )
        {
            try
            {
                ProtocolInitializer protocolInitializer = bootstrapEntry.getValue();
                BoltConnector boltConnector = bootstrapEntry.getKey();
                ChannelFuture channelFuture =
                        new ServerBootstrap().option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT )
                                .group( bossGroup, selectorGroup ).channel( configurationProvider.getChannelClass() )
                                .childHandler( protocolInitializer.channelInitializer() )
                                .bind( protocolInitializer.address().socketAddress() ).sync();
                InetSocketAddress localAddress = (InetSocketAddress) channelFuture.channel().localAddress();
                connectionRegister.register( boltConnector.key(), localAddress );
                String host = protocolInitializer.address().getHostname();
                int port = localAddress.getPort();
                if ( host.contains( ":" ) )
                {
                    // IPv6
                    log.info( "Bolt enabled on [%s]:%s.", host, port );
                }
                else
                {
                    // IPv4
                    log.info( "Bolt enabled on %s:%s.", host, port );
                }
            }
            catch ( Throwable e )
            {
                // We catch throwable here because netty uses clever tricks to have method signatures that look like they do not
                // throw checked exceptions, but they actually do. The compiler won't let us catch them explicitly because in theory
                // they shouldn't be possible, so we have to catch Throwable and do our own checks to grab them
                throw new PortBindException( bootstrapEntry.getValue().address(), e );
            }
        }
    }

    @Override
    public void stop()
    {
        bossGroup.shutdownGracefully();
        selectorGroup.shutdownGracefully();
    }
}
