/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.discovery;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Level;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.AdvertisedSocketAddress.advertisedAddress;
import static org.neo4j.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ReadReplica implements ClusterMember
{
    protected final Map<String,String> config = stringMap();
    protected final DiscoveryServiceFactory discoveryServiceFactory;
    private final File neo4jHome;
    protected final File storeDir;
    private final int serverId;
    private final String boltAdvertisedSocketAddress;
    protected ReadReplicaGraphDatabase database;
    protected Monitors monitors;

    public ReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort,
                        DiscoveryServiceFactory discoveryServiceFactory,
                        List<AdvertisedSocketAddress> coreMemberHazelcastAddresses, Map<String, String> extraParams,
                        Map<String, IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
                        String advertisedAddress, String listenAddress )
    {
        this.serverId = serverId;

        String initialHosts = coreMemberHazelcastAddresses.stream().map( AdvertisedSocketAddress::toString )
                .collect( joining( "," ) );
        boltAdvertisedSocketAddress = advertisedAddress( advertisedAddress, boltPort );

        config.put( "dbms.mode", "READ_REPLICA" );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialHosts );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String,IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        config.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new BoltConnector( "bolt" ).listen_address.name(), listenAddress( listenAddress, boltPort ) );
        config.put( new BoltConnector( "bolt" ).advertised_address.name(), boltAdvertisedSocketAddress );
        config.put( new HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP" );
        config.put( new HttpConnector( "http", Encryption.NONE ).enabled.name(), "true" );
        config.put( new HttpConnector( "http", Encryption.NONE ).listen_address.name(), listenAddress( listenAddress, httpPort ) );
        config.put( new HttpConnector( "http", Encryption.NONE ).advertised_address.name(), advertisedAddress( advertisedAddress, httpPort ) );

        this.neo4jHome = new File( parentDir, "read-replica-" + serverId );
        config.put( GraphDatabaseSettings.neo4j_home.name(), neo4jHome.getAbsolutePath() );

        config.put( CausalClusteringSettings.transaction_listen_address.name(), listenAddress( listenAddress, txPort ) );
        config.put( OnlineBackupSettings.online_backup_server.name(), listenAddress( listenAddress, backupPort ) );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        storeDir = new File( new File( new File( neo4jHome, "data" ), "databases" ), "graph.db" );
        //noinspection ResultOfMethodCallIgnored
        storeDir.mkdirs();

        this.monitors = monitors;
    }

    public String boltAdvertisedAddress()
    {
        return boltAdvertisedSocketAddress;
    }

    public String routingURI()
    {
        return String.format( "bolt+routing://%s", boltAdvertisedSocketAddress );
    }

    @Override
    public void start()
    {
        database = new ReadReplicaGraphDatabase( storeDir, Config.defaults( config ),
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory,
                memberId() );
    }

    @Override
    public void shutdown()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
        }
    }

    public CatchupPollingProcess txPollingClient()
    {
        return database.getDependencyResolver().resolveDependency( CatchupPollingProcess.class );
    }

    @Override
    public ReadReplicaGraphDatabase database()
    {
        return database;
    }

    @Override
    public ClientConnectorAddresses clientConnectorAddresses()
    {
        return ClientConnectorAddresses.extractFromConfig( Config.defaults( this.config ) );
    }

    @Override
    public String settingValue( String settingName )
    {
        return config.get(settingName);
    }

    public File storeDir()
    {
        return storeDir;
    }

    public String toString()
    {
        return format( "ReadReplica{serverId=%d}", serverId );
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltAdvertisedSocketAddress );
    }

    public File homeDir()
    {
        return neo4jHome;
    }

    public void setUpstreamDatabaseSelectionStrategy( String key )
    {
        config.put( CausalClusteringSettings.upstream_selection_strategy.name(), key );
    }

    public MemberId memberId()
    {
        return new MemberId( new UUID( serverId, 0 ) );
    }

    public int serverId()
    {
        return serverId;
    }
}
