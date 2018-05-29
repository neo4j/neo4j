/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;

import static java.util.Collections.synchronizedList;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

/**
 * Simple main class for manual testing of the complete causal cluster stack, including server etc.
 */
public class CausalClusterInProcessRunner
{
    public static void main( String[] args ) throws IOException, ExecutionException, InterruptedException
    {
        try
        {
            Path clusterPath = Files.createTempDirectory( "causal-cluster" );
            System.out.println( "clusterPath = " + clusterPath );

            CausalCluster cluster = new CausalCluster( 3, 3, clusterPath, toOutputStream( System.out ) );

            System.out.println( "Waiting for cluster to boot up..." );
            cluster.boot();

            System.out.println( "Press ENTER to exit..." );
            //noinspection ResultOfMethodCallIgnored
            System.in.read();

            System.out.println( "Shutting down..." );
            cluster.shutdown();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            System.exit( -1 );
        }
        System.exit( 0 );
    }

    static class CausalCluster
    {
        private final int nCores;
        private final int nReplicas;
        private final Path clusterPath;
        private final Log log;

        private List<ServerControls> coreControls = synchronizedList( new ArrayList<>() );
        private List<ServerControls> replicaControls = synchronizedList( new ArrayList<>() );

        CausalCluster( int nCores, int nReplicas, Path clusterPath, LogProvider logProvider )
        {
            this.nCores = nCores;
            this.nReplicas = nReplicas;
            this.clusterPath = clusterPath;
            this.log = logProvider.getLog( getClass() );
        }

        void boot() throws IOException, InterruptedException
        {
            List<String> initialMembers = new ArrayList<>( nCores );

            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int hazelcastPort = 55000 + coreId;
                initialMembers.add( "localhost:" + hazelcastPort );
            }

            List<Thread> coreThreads = new ArrayList<>();
            List<Thread> replicaThreads = new ArrayList<>();

            for ( int coreId = 0; coreId < nCores; coreId++ )
            {
                int hazelcastPort = 55000 + coreId;
                int txPort = 56000 + coreId;
                int raftPort = 57000 + coreId;
                int boltPort = 58000 + coreId;
                int httpPort = 59000 + coreId;
                int httpsPort = 60000 + coreId;

                String homeDir = "core-" + coreId;
                TestServerBuilder builder = new EnterpriseInProcessServerBuilder( clusterPath.toFile(), homeDir );

                String homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath().toString();
                builder.withConfig( GraphDatabaseSettings.neo4j_home.name(), homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
                builder.withConfig( ServerSettings.script_enabled, Settings.TRUE );

                builder.withConfig( ClusterSettings.mode.name(), ClusterSettings.Mode.CORE.name() );
                builder.withConfig( CausalClusteringSettings.multi_dc_license.name(), "true" );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members.name(), String.join( ",", initialMembers ) );

                builder.withConfig( CausalClusteringSettings.discovery_listen_address.name(), specifyPortOnly( hazelcastPort ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address.name(), specifyPortOnly( txPort ) );
                builder.withConfig( CausalClusteringSettings.raft_listen_address.name(), specifyPortOnly( raftPort ) );

                builder.withConfig( CausalClusteringSettings.expected_core_cluster_size.name(), String.valueOf( nCores ) );
                builder.withConfig( CausalClusteringSettings.server_groups.name(), "core," + "core" + coreId );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( ServerSettings.jmx_module_enabled.name(), Settings.FALSE );

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

            for ( int replicaId = 0; replicaId < nReplicas; replicaId++ )
            {
                int txPort = 56500 + replicaId;
                int boltPort = 58500 + replicaId;
                int httpPort = 59500 + replicaId;
                int httpsPort = 60500 + replicaId;

                String homeDir = "replica-" + replicaId;
                TestServerBuilder builder = new EnterpriseInProcessServerBuilder( clusterPath.toFile(), homeDir );

                String homePath = Paths.get( clusterPath.toString(), homeDir ).toAbsolutePath().toString();
                builder.withConfig( GraphDatabaseSettings.neo4j_home.name(), homePath );
                builder.withConfig( GraphDatabaseSettings.pagecache_memory.name(), "8m" );

                builder.withConfig( ClusterSettings.mode.name(), ClusterSettings.Mode.READ_REPLICA.name() );
                builder.withConfig( CausalClusteringSettings.initial_discovery_members.name(), String.join( ",", initialMembers ) );
                builder.withConfig( CausalClusteringSettings.transaction_listen_address.name(), specifyPortOnly( txPort ) );

                builder.withConfig( CausalClusteringSettings.server_groups.name(), "replica," + "replica" + replicaId );
                configureConnectors( boltPort, httpPort, httpsPort, builder );

                builder.withConfig( ServerSettings.jmx_module_enabled.name(), Settings.FALSE );

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
