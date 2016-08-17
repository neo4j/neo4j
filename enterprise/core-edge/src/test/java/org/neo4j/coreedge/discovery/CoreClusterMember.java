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
package org.neo4j.coreedge.discovery;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.core.consensus.RaftMachine;
import org.neo4j.coreedge.core.consensus.log.segmented.FileNames;
import org.neo4j.coreedge.core.state.CoreState;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.logging.Level;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.neo4j.coreedge.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.coreedge.core.consensus.log.RaftLog.PHYSICAL_LOG_DIRECTORY_NAME;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreClusterMember
{
    private final File neo4jHome;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final File storeDir;
    private final Map<String, String> config = stringMap();
    private final int serverId;
    private CoreGraphDatabase database;

    public CoreClusterMember( int serverId, int clusterSize,
                              List<AdvertisedSocketAddress> addresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              File parentDir,
                              Map<String, String> extraParams,
                              Map<String, IntFunction<String>> instanceExtraParams )
    {
        this.serverId = serverId;
        int hazelcastPort = 5000 + serverId;
        int txPort = 6000 + serverId;
        int raftPort = 7000 + serverId;
        int boltPort = 8000 + serverId;

        String initialMembers = addresses.stream().map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );

        config.put( "dbms.mode", "CORE" );
        config.put( CoreEdgeClusterSettings.initial_discovery_members.name(), initialMembers );
        config.put( CoreEdgeClusterSettings.discovery_listen_address.name(), "127.0.0.1:" + hazelcastPort );
        config.put( CoreEdgeClusterSettings.transaction_advertised_address.name(), "localhost:" + txPort );
        config.put( CoreEdgeClusterSettings.transaction_listen_address.name(), "127.0.0.1:" + txPort );
        config.put( CoreEdgeClusterSettings.raft_advertised_address.name(), "localhost:" + raftPort );
        config.put( CoreEdgeClusterSettings.raft_listen_address.name(), "127.0.0.1:" + raftPort );
        config.put( CoreEdgeClusterSettings.expected_core_cluster_size.name(), String.valueOf( clusterSize ) );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).address.name(), "0.0.0.0:" + boltPort );
        config.put( GraphDatabaseSettings.bolt_advertised_address.name(), "127.0.0.1:" + boltPort );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        this.neo4jHome = new File( parentDir, "server-core-" + serverId );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        storeDir = new File( new File( new File( neo4jHome, "data" ), "databases" ), "graph.db" );
        storeDir.mkdirs();
    }

    public void start()
    {
        database = new CoreGraphDatabase( storeDir, config,
                GraphDatabaseDependencies.newDependencies(), discoveryServiceFactory );
    }

    public void shutdown()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
        }
    }

    public CoreGraphDatabase database()
    {
        return database;
    }

    public File storeDir()
    {
        return storeDir;
    }

    public CoreState coreState()
    {
        return database.getDependencyResolver().resolveDependency( CoreState.class );
    }

    public MemberId id()
    {
        return database.getDependencyResolver().resolveDependency( RaftMachine.class ).identity();
    }

    public SortedMap<Long, File> getLogFileNames()
    {
        File clusterStateDir = new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME );
        File logFilesDir = new File( clusterStateDir, PHYSICAL_LOG_DIRECTORY_NAME );
        return new FileNames( logFilesDir ).getAllFiles( new DefaultFileSystemAbstraction(), null );
    }

    public File homeDir()
    {
        return neo4jHome;
    }

    @Override
    public String toString()
    {
        return format( "CoreClusterMember{serverId=%d}", serverId );
    }
}
