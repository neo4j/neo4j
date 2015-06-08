/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.modules;

import io.netty.channel.Channel;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.internal.StandardSessions;
import org.neo4j.ndp.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.ndp.transport.socket.NettyServer;
import org.neo4j.ndp.transport.socket.SocketProtocol;
import org.neo4j.ndp.transport.socket.SocketProtocolV1;
import org.neo4j.ndp.transport.socket.SocketTransport;
import org.neo4j.ndp.transport.socket.WebSocketTransport;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.logging.Netty4LoggerFactory;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;

/**
 * Experimental feature support for Neo4j Data Protocol, must be explicitly enabled to start up.
 */
public class NDPModule implements ServerModule
{
    private final Config config;
    private final DependencyResolver dependencyResolver;
    private final LifeSupport life = new LifeSupport();

    public NDPModule( Config config, DependencyResolver dependencyResolver )
    {
        this.config = config;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void start()
    {
        // These three pulled out dynamically due to initialization ordering issue where this module is
        // created before the below services are created. A bigger piece of work is pending to
        // organize this module life cycle better.
        final GraphDatabaseAPI api = dependencyResolver.resolveDependency( GraphDatabaseAPI.class );
        final LogService logging = dependencyResolver.resolveDependency( LogService.class );
        final UsageData usageData = dependencyResolver.resolveDependency( UsageData.class );
        final JobScheduler scheduler = dependencyResolver.resolveDependency( JobScheduler.class );

        final Log internalLog = logging.getInternalLog( Sessions.class );
        final Log userLog = logging.getUserLog( Sessions.class );

        final HostnamePort socketAddress = config.get( ServerSettings.ndp_socket_address );
        final HostnamePort webSocketAddress = config.get( ServerSettings.ndp_ws_address );

        if ( config.get( ServerSettings.ndp_enabled ) )
        {
            final Sessions sessions = life.add( new ThreadedSessions(
                    life.add( new StandardSessions( api, usageData, logging ) ),
                    scheduler,
                    logging ) );

            PrimitiveLongObjectMap<Function<Channel,SocketProtocol>> availableVersions = longObjectMap();
            availableVersions.put( SocketProtocolV1.VERSION, new Function<Channel,SocketProtocol>()
            {
                @Override
                public SocketProtocol apply( Channel channel )
                {
                    return new SocketProtocolV1( logging, sessions.newSession(), channel );
                }
            } );

            InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logging.getInternalLogProvider() ) );
            life.add( new NettyServer( asList(
                    new SocketTransport( socketAddress, availableVersions ),
                    new WebSocketTransport( webSocketAddress, availableVersions ) ) ) );
            internalLog.info( "NDP Server extension loaded." );
            userLog.info( "Experimental NDP support enabled! Listening for socket connections on " + socketAddress +
                          " and for websocket connections on " + webSocketAddress + ".");
        }

        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }
}
