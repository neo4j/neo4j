/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.harness;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;

import static java.util.Collections.synchronizedList;

public class CausalClusterInProcessBuilder
{

    public static WithCores init()
    {
        return new Builder();
    }

    /**
     * Step Builder to ensure that Cluster has all the required pieces
     * TODO: Add mapping methods to allow for core hosts and replicas to be unevenly distributed  between databases
     */
    public static class Builder implements WithCores, WithReplicas, WithLogger, WithPath, WithOptionalDatabasesAndPorts
    {

        private int numCoreHosts;
        private int numReadReplicas;
        private Log log;
        private Path path;
        private PortPickingFactory portFactory = PortPickingFactory.DEFAULT;
        private List<String> databases = new ArrayList<>( Collections.singletonList( "default" ) );

        public WithReplicas withCores( int n )
        {
            numCoreHosts = n;
            return this;
        }

        public WithLogger withReplicas( int n )
        {
            numReadReplicas = n;
            return this;
        }

        public WithPath withLogger( LogProvider l )
        {
            log = l.getLog( "org.neo4j.harness.CausalCluster" );
            return this;
        }

        public Builder atPath( Path p )
        {
            path = p;
            return this;
        }

        @Override
        public Builder withOptionalPortsStrategy( PortPickingStrategy s )
        {
            portFactory = new PortPickingFactory( s );
            return this;
        }

        @Override
        public Builder withOptionalDatabases( List<String> databaseNames )
        {
            if ( !databaseNames.isEmpty() )
            {
                databases = databaseNames;
            }
            return this;
        }

        public CausalCluster build()
        {
            int nDatabases = databases.size();
            if ( nDatabases > numCoreHosts )
            {
                throw new IllegalArgumentException(
                        "You cannot have more databases than core hosts. Each database in the cluster must have at least 1 core " + "host. You have provided " +
                                nDatabases + " databases and " + numCoreHosts + " core hosts." );
            }
            return new CausalCluster( this );
        }
    }

    /**
     * Builder step interfaces
     */
    interface WithCores
    {
        WithReplicas withCores( int n );
    }

    interface WithReplicas
    {
        WithLogger withReplicas( int n );
    }

    interface WithLogger
    {
        WithPath withLogger( LogProvider l );
    }

    interface WithPath
    {
        Builder atPath( Path p );
    }

    interface WithOptionalDatabasesAndPorts
    {
        Builder withOptionalPortsStrategy( PortPickingStrategy s );

        Builder withOptionalDatabases( List<String> databaseNames );
    }

    /**
     * Port picker functional interface
     */
    public interface PortPickingStrategy
    {
        int port( int offset, int id );
    }

    /**
     * Port picker factory
     */
    public static final class PortPickingFactory
    {
        public static final PortPickingFactory DEFAULT = new PortPickingFactory( ( offset, id ) -> offset + id );

        private final PortPickingStrategy st;

        public PortPickingFactory( PortPickingStrategy st )
        {
            this.st = st;
        }

        int hazelcastPort( int coreId )
        {
            return st.port( 55000, coreId );
        }

        int txCorePort( int coreId )
        {
            return st.port( 56000, coreId );
        }

        int raftCorePort( int coreId )
        {
            return st.port( 57000, coreId );
        }

        int boltCorePort( int coreId )
        {
            return st.port( 58000, coreId );
        }

        int httpCorePort( int coreId )
        {
            return st.port( 59000, coreId );
        }

        int httpsCorePort( int coreId )
        {
            return st.port( 60000, coreId );
        }

        int txReadReplicaPort( int replicaId )
        {
            return st.port( 56500, replicaId );
        }

        int boltReadReplicaPort( int replicaId )
        {
            return st.port( 58500, replicaId );
        }

        int httpReadReplicaPort( int replicaId )
        {
            return st.port( 59500, replicaId );
        }

        int httpsReadReplicaPort( int replicaId )
        {
            return st.port( 60500, replicaId );
        }
    }

    /**
     * Implementation of in process Cluster
     */
    static class CausalCluster
    {
        private final int nCores;
        private final int nReplicas;
        private final int nDatabases;
        private final List<String> databaseNames;
        private final Path clusterPath;
        private final Log log;
        private final PortPickingFactory portFactory;

        private List<ServerControls> coreControls = synchronizedList( new ArrayList<>() );
        private List<ServerControls> replicaControls = synchronizedList( new ArrayList<>() );

        private CausalCluster( CausalClusterInProcessBuilder.Builder bldr )
        {
            this.nCores = bldr.numCoreHosts;
            this.nReplicas = bldr.numReadReplicas;
            this.clusterPath = bldr.path;
            this.log = bldr.log;
            this.portFactory = bldr.portFactory;
            this.nDatabases = bldr.databases.size();
            this.databaseNames = bldr.databases;
        }

        private Map<Integer,String> distributeHostsBetweenDatabases( int nHosts, List<String> databases )
        {
            //Max number of hosts per database is (nHosts / nDatabases) or (nHosts / nDatabases) + 1
            int nDatabases = databases.size();
            int maxCapacity = ( nHosts % nDatabases == 0 ) ? (nHosts / nDatabases) : (nHosts / nDatabases) + 1;

            List<String> repeated =
                    databases.stream().flatMap( db -> IntStream.range( 0, maxCapacity ).mapToObj( ignored -> db ) ).collect( Collectors.toList() );

            Map<Integer,String> mapping = new HashMap<>( nHosts );

            for ( int hostId = 0; hostId < nHosts; hostId++ )
            {
                mapping.put( hostId, repeated.get( hostId ) );
            }
            return mapping;
        }

        void boot() throws InterruptedException
        {
            List<String> initialMembers = new ArrayList<>( nCores );

            Map<Integer,String> initialMembersToDatabase = distributeHostsBetweenDatabases( nCores, databaseNames );

            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int hazelcastPort = portFactory.hazelcastPort( coreId );
                initialMembers.add( "localhost:" + hazelcastPort );
            }

            List<Thread> coreThreads = new ArrayList<>();
            List<Thread> replicaThreads = new ArrayList<>();

            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int hazelcastPort = portFactory.hazelcastPort( coreId );
                int txPort = portFactory.txCorePort( coreId );
                int raftPort = portFactory.raftCorePort( coreId );
                int boltPort = portFactory.boltCorePort( coreId );
                int httpPort = portFactory.httpCorePort( coreId );
                int httpsPort = portFactory.httpsCorePort( coreId );

                String homeDir = "core-" + coreId;
                TestServerBuilder builder = new EnterpriseInProcessServerBuilder( clusterPath.toFile(), homeDir );

                String homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath().toString();
                builder.withConfig( GraphDatabaseSettings.neo4j_home.name(), homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory.name(), "8m" );

                builder.withConfig( EnterpriseEditionSettings.mode.name(), EnterpriseEditionSettings.Mode.CORE.name() );
                builder.withConfig( CausalClusteringSettings.multi_dc_license.name(), "true" );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members.name(), String.join( ",", initialMembers ) );

                builder.withConfig( CausalClusteringSettings.discovery_listen_address.name(), specifyPortOnly( hazelcastPort ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address.name(), specifyPortOnly( txPort ) );
                builder.withConfig( CausalClusteringSettings.raft_listen_address.name(), specifyPortOnly( raftPort ) );

                builder.withConfig( CausalClusteringSettings.database.name(), initialMembersToDatabase.get( coreId ) );

                builder.withConfig( CausalClusteringSettings.minimum_core_cluster_size_at_formation.name(), String.valueOf( nCores ) );
                builder.withConfig( CausalClusteringSettings.minimum_core_cluster_size_at_runtime.name(), String.valueOf( nCores ) );
                builder.withConfig( CausalClusteringSettings.server_groups.name(), "core," + "core" + coreId );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( ServerSettings.jmx_module_enabled.name(), Settings.FALSE );

                builder.withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

                int finalCoreId = coreId;
                Thread coreThread = new Thread( () ->
                {
                    coreControls.add( builder.newServer() );
                    log.info( "Core " + finalCoreId + " started." );
                } );
                coreThreads.add( coreThread );
                coreThread.start();
            }

            for ( Thread coreThread : coreThreads )
            {
                coreThread.join();
            }

            Map<Integer,String> replicasToDatabase = distributeHostsBetweenDatabases( nReplicas, databaseNames );

            for ( int replicaId = 0; replicaId < nReplicas; replicaId++ )
            {
                int txPort = portFactory.txReadReplicaPort( replicaId );
                int boltPort = portFactory.boltReadReplicaPort( replicaId );
                int httpPort = portFactory.httpReadReplicaPort( replicaId );
                int httpsPort = portFactory.httpsReadReplicaPort( replicaId );

                String homeDir = "replica-" + replicaId;
                TestServerBuilder builder = new EnterpriseInProcessServerBuilder( clusterPath.toFile(), homeDir );

                String homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath().toString();
                builder.withConfig( GraphDatabaseSettings.neo4j_home.name(), homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory.name(), "8m" );

                builder.withConfig( EnterpriseEditionSettings.mode.name(), EnterpriseEditionSettings.Mode.READ_REPLICA.name() );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members.name(), String.join( ",", initialMembers ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address.name(), specifyPortOnly( txPort ) );

                builder.withConfig( CausalClusteringSettings.database.name(), replicasToDatabase.get( replicaId ) );

                builder.withConfig( CausalClusteringSettings.server_groups.name(), "replica," + "replica" + replicaId );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( ServerSettings.jmx_module_enabled.name(), Settings.FALSE );

                builder.withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
                int finalReplicaId = replicaId;
                Thread replicaThread = new Thread( () ->
                {
                    replicaControls.add( builder.newServer() );
                    log.info( "Read replica " + finalReplicaId + " started." );
                } );
                replicaThreads.add( replicaThread );
                replicaThread.start();
            }

            for ( Thread replicaThread : replicaThreads )
            {
                replicaThread.join();
            }
        }

        private static String specifyPortOnly( int port )
        {
            return ":" + port;
        }

        private static void configureConnectors( int boltPort, int httpPort, int httpsPort, TestServerBuilder builder )
        {
            builder.withConfig( new BoltConnector( "bolt" ).type.name(), "BOLT" );
            builder.withConfig( new BoltConnector( "bolt" ).enabled.name(), "true" );
            builder.withConfig( new BoltConnector( "bolt" ).listen_address.name(), specifyPortOnly( boltPort ) );
            builder.withConfig( new BoltConnector( "bolt" ).advertised_address.name(), specifyPortOnly( boltPort ) );

            builder.withConfig( new HttpConnector( "http", HttpConnector.Encryption.NONE ).type.name(), "HTTP" );
            builder.withConfig( new HttpConnector( "http", HttpConnector.Encryption.NONE ).enabled.name(), "true" );
            builder.withConfig( new HttpConnector( "http", HttpConnector.Encryption.NONE ).listen_address.name(), specifyPortOnly( httpPort ) );
            builder.withConfig( new HttpConnector( "http", HttpConnector.Encryption.NONE ).advertised_address.name(), specifyPortOnly( httpPort ) );

            builder.withConfig( new HttpConnector( "https", HttpConnector.Encryption.TLS ).type.name(), "HTTP" );
            builder.withConfig( new HttpConnector( "https", HttpConnector.Encryption.TLS ).enabled.name(), "true" );
            builder.withConfig( new HttpConnector( "https", HttpConnector.Encryption.TLS ).listen_address.name(), specifyPortOnly( httpsPort ) );
            builder.withConfig( new HttpConnector( "https", HttpConnector.Encryption.TLS ).advertised_address.name(), specifyPortOnly( httpsPort ) );
        }

        void shutdown() throws InterruptedException
        {
            shutdownControls( replicaControls );
            shutdownControls( coreControls );
        }

        private void shutdownControls( Iterable<? extends ServerControls> controls ) throws InterruptedException
        {
            Collection<Thread> threads = new ArrayList<>();
            for ( ServerControls control : controls )
            {
                Thread thread = new Thread( control::close );
                threads.add( thread );
                thread.start();
            }

            for ( Thread thread : threads )
            {
                thread.join();
            }
        }
    }
}
