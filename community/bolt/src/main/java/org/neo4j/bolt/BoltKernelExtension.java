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
package org.neo4j.bolt;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.bolt.v1.runtime.internal.EncryptionRequiredSessions;
import org.neo4j.bolt.v1.runtime.internal.StandardSessions;
import org.neo4j.bolt.v1.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigValues;
import org.neo4j.kernel.configuration.ConfigView;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;

import static org.neo4j.bolt.BoltKernelExtension.EncryptionLevel.OPTIONAL;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.HOSTNAME_PORT;
import static org.neo4j.helpers.Settings.options;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.boltNetworkIO;

/**
 * Wraps Bolt and exposes it as a Kernel Extension.
 */
@Service.Implementation( KernelExtensionFactory.class )
public class BoltKernelExtension extends KernelExtensionFactory<BoltKernelExtension.Dependencies>
{
    public static class Settings
    {
        public static final Function<ConfigValues,List<ConfigView>> connector_group = Config.groups( "dbms.connector" );

        @Description( "Enable Neo4j Bolt" )
        public static final Setting<Boolean> enabled =
                setting( "enabled", BOOLEAN, "false" );

        @Description( "Set the encryption level for Neo4j Bolt protocol ports" )
        public static final Setting<EncryptionLevel> tls_level =
                setting( "tls.level", options( EncryptionLevel.class ),
                        OPTIONAL.name() );

        @Description( "Host and port for the Neo4j Bolt Protocol" )
        public static final Setting<HostnamePort> socket_address =
                setting( "address", HOSTNAME_PORT, "localhost:7687" );

        public static <T> Setting<T> connector( int i, Setting<T> setting )
        {
            String name = String.format( "dbms.connector.%s", i );
            return new Setting<T>()
            {
                @Override
                public String name()
                {
                    return String.format( "%s.%s", name, setting.name() );
                }

                @Override
                public String getDefaultValue()
                {
                    return setting.getDefaultValue();
                }

                @Override
                public T apply( Function<String,String> settings )
                {
                    return setting.apply( settings );
                }

                @Override
                public int hashCode()
                {
                    return name().hashCode();
                }

                @Override
                public boolean equals( Object obj )
                {
                    if ( obj instanceof Setting<?> && ((Setting<?>) obj).name().equals( name() ) )
                    {
                        return true;

                    }
                    return false;
                }
            };
        }
    }

    public enum EncryptionLevel
    {
        REQUIRED, OPTIONAL, DISABLED
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

        final LifeSupport life = new LifeSupport();

        final JobScheduler scheduler = dependencies.scheduler();

        final Sessions sessions = new ThreadedSessions(
                life.add( new StandardSessions( api, dependencies.usageData(), logging ) ),
                scheduler, logging );



        List<NettyServer.ProtocolInitializer> connectors = new ArrayList<>();

        List<ConfigView> view = config.view( Settings.connector_group );
        for( ConfigView connector: view )
        {
            final HostnamePort socketAddress = connector.get( Settings.socket_address );

            if ( connector.get( Settings.enabled ) )
            {
                SslContext sslCtx;
                boolean requireEncryption = false;
                switch ( connector.get( Settings.tls_level ) )
                {
                // self signed cert should be generated when encryption is REQUIRED or OPTIONAL on the server
                // while no cert is generated if encryption is DISABLED
                case REQUIRED:
                    requireEncryption = true;
                    // no break here
                case OPTIONAL:
                    SelfSignedCertificate ssc = new SelfSignedCertificate();
                    sslCtx = SslContextBuilder.forServer( ssc.certificate(), ssc.privateKey() ).build();
                    break;
                default:
                    // case DISABLED:
                    sslCtx = null;
                    break;
                }

                PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> availableVersions;
                if( requireEncryption )
                {
                    availableVersions = newVersions( logging, new EncryptionRequiredSessions( sessions ) );
                }
                else
                {
                    availableVersions = newVersions( logging, sessions );
                }

                connectors.add( new SocketTransport( socketAddress, sslCtx, logging.getInternalLogProvider(),
                    availableVersions ) );
            }
        }

        if( connectors.size() > 0 )
        {
            life.add( new NettyServer( scheduler.threadFactory( boltNetworkIO ), connectors ) );
            log.info( "Bolt Server extension loaded." );
        }

        return life;
    }

    private PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> newVersions( LogService logging,
            Sessions sessions )
    {
        PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> availableVersions = longObjectMap();
        availableVersions.put(
                BoltProtocolV1.VERSION,
                ( channel, isEncrypted ) -> new BoltProtocolV1( logging, sessions.newSession( isEncrypted ), channel )
        );
        return availableVersions;
    }
}
