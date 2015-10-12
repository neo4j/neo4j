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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;

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
import org.neo4j.bolt.runtime.Sessions;
import org.neo4j.bolt.runtime.internal.StandardSessions;
import org.neo4j.bolt.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.bolt.transport.socket.NettyServer;
import org.neo4j.bolt.transport.socket.SocketProtocol;
import org.neo4j.bolt.transport.socket.SocketProtocolV1;
import org.neo4j.bolt.transport.socket.SocketTransport;
import org.neo4j.bolt.transport.socket.WebSocketTransport;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.logging.Netty4LoggerFactory;
import org.neo4j.server.security.ssl.KeyStoreInformation;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;

/**
 * Experimental feature support for Neo4j Data Protocol, must be explicitly enabled to start up.
 */
public class BoltModule implements ServerModule
{
    private final Config config;
    private final DependencyResolver dependencyResolver;
    private final KeyStoreInformation ksi;
    private final LifeSupport life = new LifeSupport();

    public BoltModule( Config config, DependencyResolver dependencyResolver, KeyStoreInformation ksi )
    {
        this.config = config;
        this.dependencyResolver = dependencyResolver;
        this.ksi = ksi;
    }

    @Override
    public void start()
    {
        try
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

            final File certificateFile = config.get( ServerSettings.tls_certificate_file );
            final File keyFile = config.get( ServerSettings.tls_key_file );

            final HostnamePort socketAddress = config.get( ServerSettings.bolt_socket_address );
            final HostnamePort webSocketAddress = config.get( ServerSettings.bolt_ws_address );

            if ( config.get( ServerSettings.bolt_enabled ) )
            {
                final Sessions sessions = new ThreadedSessions(
                        life.add( new StandardSessions( api, usageData, logging ) ),
                        scheduler,
                        logging );

                PrimitiveLongObjectMap<Function<Channel,SocketProtocol>> availableVersions = longObjectMap();
                availableVersions.put( SocketProtocolV1.VERSION, new Function<Channel,SocketProtocol>()
                {
                    @Override
                    public SocketProtocol apply( Channel channel )
                    {
                        return new SocketProtocolV1( logging, sessions.newSession(), channel, usageData);
                    }
                } );


                SslContext sslCtx = createSSLContext(certificateFile, keyFile);

                InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logging.getInternalLogProvider() ) );
                life.add( new NettyServer( scheduler.threadFactory( JobScheduler.Groups.boltNetworkIO ), asList(
                        new SocketTransport( socketAddress, sslCtx, logging.getInternalLogProvider(), availableVersions ),
                        new WebSocketTransport( webSocketAddress, sslCtx, logging.getInternalLogProvider(), availableVersions ) ) ) );
                internalLog.info( "NDP Server extension loaded." );
                userLog.info( "Experimental NDP support enabled! Listening for socket connections on " + socketAddress +
                              " and for websocket connections on " + webSocketAddress + "." );
            }

            life.start();
        }
        catch( SSLException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e )
        {
            throw new RuntimeException( "Failed to configure TLS during NDP startup: " + e.getMessage(), e );
        }
    }

    private SslContext createSSLContext( File certificateFile, File keyFile )
            throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, SSLException
    {
        SslContext sslCtx = null;

        // Configure SSL context if enabled
        if ( config.get( ServerSettings.bolt_tls_enabled ) )
        {
            KeyManagerFactory keyManager = KeyManagerFactory.getInstance( "SunX509" );
            keyManager.init( ksi.getKeyStore(), ksi.getKeyPassword() );

            sslCtx = SslContextBuilder
                    .forServer( certificateFile, keyFile     )
                    .sslProvider( SslProvider.JDK )
                    .keyManager( keyManager )
                    .build();
        }
        return sslCtx;
    }

    @Override
    public void stop()
    {
        life.stop();
    }
}
