/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.function.IntFunction;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Level;
import org.neo4j.server.configuration.ClientConnectorSettings;
import org.neo4j.server.configuration.ClientConnectorSettings.HttpConnector.Encryption;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ReadReplica implements ClusterMember
{
    private final Map<String, String> config = stringMap();
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File storeDir;
    private final int memberId;
    private final String boltAdvertisedAddress;
    private ReadReplicaGraphDatabase database;
    private Monitors monitors;

    public ReadReplica( File parentDir, int memberId, DiscoveryServiceFactory discoveryServiceFactory,
                        List<AdvertisedSocketAddress> coreMemberHazelcastAddresses, Map<String, String> extraParams,
                        Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        this( parentDir, memberId, discoveryServiceFactory, coreMemberHazelcastAddresses, extraParams,
                instanceExtraParams, recordFormat, new Monitors() );
    }

    public ReadReplica( File parentDir, int memberId, DiscoveryServiceFactory discoveryServiceFactory,
                        List<AdvertisedSocketAddress> coreMemberHazelcastAddresses, Map<String, String> extraParams,
                        Map<String, IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors )
    {
        this.memberId = memberId;
        int boltPort = 9000 + memberId;
        int httpPort = 11000 + memberId;

        String initialHosts = coreMemberHazelcastAddresses.stream()
                .map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );

        config.put( "dbms.mode", "READ_REPLICA" );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialHosts );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( memberId ) );
        }

        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).listen_address.name(), "127.0.0.1:" + boltPort );
        boltAdvertisedAddress = "127.0.0.1:" + boltPort;
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).advertised_address.name(), boltAdvertisedAddress );
        config.put( new ClientConnectorSettings.HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP" );
        config.put( new ClientConnectorSettings.HttpConnector( "http", Encryption.NONE ).enabled.name(), "true" );
        config.put( new ClientConnectorSettings.HttpConnector( "http", Encryption.NONE ).listen_address.name(), "127.0.0.1:" + httpPort );
        config.put( new ClientConnectorSettings.HttpConnector( "http", Encryption.NONE ).advertised_address.name(), "127.0.0.1:" + httpPort );

        File neo4jHome = new File( parentDir, "read-replica-" + memberId );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        storeDir = new File( new File( new File( neo4jHome, "data" ), "databases" ), "graph.db" );
        storeDir.mkdirs();

        this.monitors = monitors;
    }

    public String boltAdvertisedAddress()
    {
        return boltAdvertisedAddress;
    }

    public String routingURI()
    {
        return String.format( "bolt+routing://%s", boltAdvertisedAddress );
    }

    @Override

    public void start()
    {
        database = new ReadReplicaGraphDatabase( storeDir, config,
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory );
    }

    @Override
    public void shutdown()
    {
        if ( database != null )
        {
            database.shutdown();
        }
        database = null;
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
        return ClientConnectorAddresses.extractFromConfig( new Config( this.config ) );
    }

    public File storeDir()
    {
        return storeDir;
    }

    public String toString()
    {
        return format( "ReadReplica{memberId=%d}", memberId );
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltAdvertisedAddress );
    }

}
