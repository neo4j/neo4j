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
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.neo4j.bolt.transport.configuration.EpollConfigurationProvider;
import org.neo4j.bolt.transport.configuration.NioConfigurationProvider;
import org.neo4j.bolt.transport.configuration.ServerConfigurationProvider;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.helpers.ListenSocketAddress;
import org.neo4j.internal.helpers.PortBindException;
import org.neo4j.internal.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.function.ThrowingAction.executeAll;

/**
 * Simple wrapper around Netty boss and selector threads, which allows multiple ports and protocols to be handled
 * by the same set of common worker threads.
 */
public class NettyServer extends LifecycleAdapter
{
    private static final boolean USE_EPOLL = FeatureToggles.flag( NettyServer.class, "useEpoll", true  );

    private final Map<BoltConnector, ProtocolInitializer> bootstrappersMap;
    private final ThreadFactory tf;
    private final ConnectorPortRegister portRegister;
    private final Log log;

    private EventLoopGroup eventLoopGroup;
    private List<Channel> channels;

    /**
     * Describes how to initialize new channels for a protocol, and which address the protocol should be bolted into.
     */
    public interface ProtocolInitializer
    {
        ChannelInitializer<Channel> channelInitializer();
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
        this.portRegister = connectorRegister;
        this.log = log;
    }

    @Override
    public void start() throws Exception
    {
        var configurationProvider = createConfigurationProvider();

        eventLoopGroup = configurationProvider.createEventLoopGroup( tf );
        channels = new ArrayList<>();

        for ( var bootstrapEntry : bootstrappersMap.entrySet() )
        {
            try
            {
                var protocolInitializer = bootstrapEntry.getValue();
                var boltConnector = bootstrapEntry.getKey();

                var channel = bind( configurationProvider, protocolInitializer );
                channels.add( channel );

                var localAddress = (InetSocketAddress) channel.localAddress();
                portRegister.register( boltConnector.key(), localAddress );

                var host = protocolInitializer.address().getHostname();
                var port = localAddress.getPort();
                log.info( "Bolt enabled on %s.", SocketAddress.format( host, port ) );
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
    public void stop() throws Exception
    {
        executeAll(
                this::deregisterListenAddresses,
                this::closeChannels,
                this::shutdownEventLoopGroup );
    }

    private Channel bind( ServerConfigurationProvider configurationProvider, ProtocolInitializer protocolInitializer ) throws InterruptedException
    {
        var serverBootstrap = createServerBootstrap( configurationProvider, protocolInitializer );
        var address = protocolInitializer.address().socketAddress();
        return serverBootstrap.bind( address ).sync().channel();
    }

    private ServerBootstrap createServerBootstrap( ServerConfigurationProvider configurationProvider, ProtocolInitializer protocolInitializer )
    {
        return new ServerBootstrap()
                .group( eventLoopGroup )
                .channel( configurationProvider.getChannelClass() )
                .option( ChannelOption.SO_REUSEADDR, true )
                .childOption( ChannelOption.SO_KEEPALIVE, true )
                .childHandler( protocolInitializer.channelInitializer() );
    }

    private static ServerConfigurationProvider createConfigurationProvider()
    {
        var useEpoll = USE_EPOLL && Epoll.isAvailable();
        return useEpoll ? EpollConfigurationProvider.INSTANCE : NioConfigurationProvider.INSTANCE;
    }

    private void deregisterListenAddresses()
    {
        for ( var connector : bootstrappersMap.keySet() )
        {
            portRegister.deregister( connector.key() );
        }
    }

    private void closeChannels()
    {
        if ( channels != null )
        {
            for ( var channel : channels )
            {
                try
                {
                    channel.close().syncUninterruptibly();
                }
                catch ( Throwable t )
                {
                    log.warn( "Failed to close a channel " + channel, t );
                }
            }
            channels = null;
        }
    }

    private void shutdownEventLoopGroup()
    {
        if ( eventLoopGroup != null )
        {
            try
            {
                eventLoopGroup.shutdownGracefully( 500, 2000, MILLISECONDS ).syncUninterruptibly();
            }
            catch ( Throwable t )
            {
                log.warn( "Failed to shutdown the event loop group", t );
            }
            eventLoopGroup = null;
        }
    }
}
