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
package org.neo4j.ext;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.bolt.runtime.Sessions;
import org.neo4j.bolt.runtime.internal.StandardSessions;
import org.neo4j.bolt.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.bolt.transport.socket.NettyServer;
import org.neo4j.bolt.transport.socket.SocketProtocol;
import org.neo4j.bolt.transport.socket.SocketProtocolV1;
import org.neo4j.bolt.transport.socket.SocketTransport;
import org.neo4j.bolt.transport.socket.WebSocketTransport;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.boltNetworkIO;

/**
 * Wraps Bolt and exposes it as a Kernel Extension.
 */
@Service.Implementation(KernelExtensionFactory.class)
public class BoltKernelExtension extends KernelExtensionFactory<BoltKernelExtension.Dependencies>
{
    public static class Settings
    {
        @Description("Enable Neo4j Bolt")
        public static final Setting<Boolean> bolt_enabled = setting( "experimental.bolt.enabled", BOOLEAN,
                "false" );

        @Description("Host and port for the Neo4j Bolt Protocol")
        public static final Setting<HostnamePort> bolt_socket_address =
                setting( "dbms.bolt.address", HOSTNAME_PORT, "localhost:7687" );

        @Description("Host and port for the Neo4j Bolt Protocol Websocket")
        public static final Setting<HostnamePort> bolt_ws_address =
                setting( "dbms.bolt.ws.address", HOSTNAME_PORT, "localhost:7688" );
    }

    public interface Dependencies
    {
        LogService logService();

        Config config();

        GraphDatabaseService db();

        JobScheduler scheduler();

        UsageData usageData();
    }

    public BoltKernelExtension()
    {
        super( "bolt-server" );
    }

    @Override
    public Lifecycle newKernelExtension( final Dependencies dependencies ) throws Throwable
    {
        final Config config = dependencies.config();
        final GraphDatabaseService gdb = dependencies.db();
        final GraphDatabaseAPI api = (GraphDatabaseAPI) gdb;
        final LogService logging = dependencies.logService();
        final Log log = logging.getInternalLog( Sessions.class );

        final HostnamePort socketAddress = config.get( Settings.bolt_socket_address );
        final HostnamePort webSocketAddress = config.get( Settings.bolt_ws_address );

        final LifeSupport life = new LifeSupport();

        if ( config.get( Settings.bolt_enabled ) )
        {
            final JobScheduler scheduler = dependencies.scheduler();

            final Sessions sessions = new ThreadedSessions(
                    life.add( new StandardSessions( api, dependencies.usageData(), logging ) ),
                    scheduler, logging );

            SelfSignedCertificate ssc = new SelfSignedCertificate();
            SslContext sslCtx = SslContextBuilder.forServer( ssc.certificate(), ssc.privateKey() ).build();

            PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> availableVersions = longObjectMap();
            availableVersions.put( SocketProtocolV1.VERSION, new Function<Channel, SocketProtocol>()
            {
                @Override
                public SocketProtocol apply( Channel channel )
                {
                    return new SocketProtocolV1( logging, sessions.newSession(), channel, dependencies.usageData() );
                }
            } );

            // Start services
            life.add( new NettyServer( scheduler.threadFactory( boltNetworkIO ), asList(
                    new SocketTransport( socketAddress, sslCtx, logging.getInternalLogProvider(), availableVersions ),
                    new WebSocketTransport( webSocketAddress, sslCtx, logging.getInternalLogProvider(), availableVersions ) ) ) );
            log.info( "Bolt Server extension loaded." );
        }

        return life;
    }
}
