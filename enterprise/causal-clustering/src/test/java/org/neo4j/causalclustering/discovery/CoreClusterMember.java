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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Level;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.neo4j.causalclustering.core.consensus.log.RaftLog.RAFT_LOG_DIRECTORY_NAME;
import static org.neo4j.helpers.AdvertisedSocketAddress.advertisedAddress;
import static org.neo4j.helpers.ListenSocketAddress.listenAddress;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreClusterMember implements ClusterMember<GraphDatabaseFacade>
{
    private final File neo4jHome;
    protected final DiscoveryServiceFactory discoveryServiceFactory;
    protected final File storeDir;
    private final File clusterStateDir;
    private final File raftLogDir;
    private final Map<String, String> config = stringMap();
    private final int serverId;
    private final String boltAdvertisedSocketAddress;
    private final int discoveryPort;
    protected CoreGraphDatabase database;

    public CoreClusterMember( int serverId,
                              int discoveryPort,
                              int txPort,
                              int raftPort,
                              int boltPort,
                              int httpPort,
                              int backupPort,
                              int clusterSize,
                              List<AdvertisedSocketAddress> addresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              File parentDir,
                              Map<String, String> extraParams,
                              Map<String, IntFunction<String>> instanceExtraParams,
                              String listenAddress,
                              String advertisedAddress )
    {
        this.serverId = serverId;
        this.discoveryPort = discoveryPort;

        String initialMembers = addresses.stream().map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );
        boltAdvertisedSocketAddress = advertisedAddress( advertisedAddress, boltPort );

        config.put( EnterpriseEditionSettings.mode.name(), EnterpriseEditionSettings.Mode.CORE.name() );
        config.put( GraphDatabaseSettings.default_advertised_address.name(), advertisedAddress );
        config.put( CausalClusteringSettings.initial_discovery_members.name(), initialMembers );
        config.put( CausalClusteringSettings.discovery_listen_address.name(), listenAddress( listenAddress, discoveryPort ) );
        config.put( CausalClusteringSettings.transaction_listen_address.name(), listenAddress( listenAddress, txPort ) );
        config.put( CausalClusteringSettings.raft_listen_address.name(), listenAddress( listenAddress, raftPort ) );
        config.put( CausalClusteringSettings.cluster_topology_refresh.name(), "1000ms" );
        config.put( CausalClusteringSettings.expected_core_cluster_size.name(), String.valueOf( clusterSize ) );
        config.put( CausalClusteringSettings.leader_election_timeout.name(), "500ms" );
        config.put( CausalClusteringSettings.raft_messages_log_enable.name(), Settings.TRUE );
        config.put( GraphDatabaseSettings.store_internal_log_level.name(), Level.DEBUG.name() );
        config.put( GraphDatabaseSettings.record_format.name(), recordFormat );
        config.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        config.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        config.put( new BoltConnector( "bolt" ).listen_address.name(), listenAddress( listenAddress, boltPort ) );
        config.put( new BoltConnector( "bolt" ).advertised_address.name(), boltAdvertisedSocketAddress );
        config.put( new HttpConnector( "http", Encryption.NONE ).type.name(), "HTTP" );
        config.put( new HttpConnector( "http", Encryption.NONE ).enabled.name(), "true" );
        config.put( new HttpConnector( "http", Encryption.NONE ).listen_address.name(), listenAddress( listenAddress, httpPort ) );
        config.put( new HttpConnector( "http", Encryption.NONE ).advertised_address.name(), advertisedAddress( advertisedAddress, httpPort ) );
        config.put( OnlineBackupSettings.online_backup_server.name(), listenAddress( listenAddress, backupPort ) );
        config.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        config.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        config.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            config.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        this.neo4jHome = new File( parentDir, "server-core-" + serverId );
        config.put( GraphDatabaseSettings.neo4j_home.name(), neo4jHome.getAbsolutePath() );
        config.put( GraphDatabaseSettings.logs_directory.name(), new File( neo4jHome, "logs" ).getAbsolutePath() );

        this.discoveryServiceFactory = discoveryServiceFactory;
        File dataDir = new File( neo4jHome, "data" );
        clusterStateDir = ClusterStateDirectory.withoutInitializing( dataDir ).get();
        raftLogDir = new File( clusterStateDir, RAFT_LOG_DIRECTORY_NAME );
        storeDir = new File( new File( dataDir, "databases" ), "graph.db" );
        //noinspection ResultOfMethodCallIgnored
        storeDir.mkdirs();
    }

    public String boltAdvertisedAddress()
    {
        return boltAdvertisedSocketAddress;
    }

    public String routingURI()
    {
        return String.format( "bolt+routing://%s", boltAdvertisedSocketAddress );
    }

    public String directURI()
    {
        return String.format( "bolt://%s", boltAdvertisedSocketAddress );
    }

    @Override
    public void start()
    {
        database = new CoreGraphDatabase( storeDir, Config.defaults( config ),
                GraphDatabaseDependencies.newDependencies(), discoveryServiceFactory );
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

    @Override
    public CoreGraphDatabase database()
    {
        return database;
    }

    public File storeDir()
    {
        return storeDir;
    }

    public RaftLogPruner raftLogPruner()
    {
        return database.getDependencyResolver().resolveDependency( RaftLogPruner.class );
    }

    public RaftMachine raft()
    {
        return database.getDependencyResolver().resolveDependency( RaftMachine.class );
    }

    public MemberId id()
    {
        return database.getDependencyResolver().resolveDependency( RaftMachine.class ).identity();
    }

    public SortedMap<Long, File> getLogFileNames() throws IOException
    {
        File logFilesDir = new File( clusterStateDir, RAFT_LOG_DIRECTORY_NAME );
        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            return new FileNames( logFilesDir ).getAllFiles( fileSystem, null );
        }
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

    public int serverId()
    {
        return serverId;
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

    public File clusterStateDirectory()
    {
        return clusterStateDir;
    }

    public File raftLogDirectory()
    {
        return raftLogDir;
    }

    public void stopCatchupServer() throws Throwable
    {
        database.getDependencyResolver().resolveDependency( CatchupServer.class).stop();
    }

    int discoveryPort()
    {
        return discoveryPort;
    }
}
