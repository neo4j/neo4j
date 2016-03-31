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
package org.neo4j.test.ha;

import org.hamcrest.CoreMatchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.logging.Level;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterWithAdditionalArbiters;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class ClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule( Level.OFF );
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "testCluster" ) )
                .withSharedConfig(
                    MapUtil.stringMap(
                            HaSettings.ha_server.name(), "localhost:6001-9999",
                            HaSettings.tx_push_factor.name(), "2" ) )
                .withCluster( clusterOfSize( 3 ) )
                .build();
        try
        {
            clusterManager.start();

            clusterManager.getCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }


            HighlyAvailableGraphDatabase slave = clusterManager.getCluster().getAnySlave();
            try ( Transaction transaction = slave.beginTx() )
            {
                Node node = slave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testClusterWithHostnames() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory(  "testCluster" ) )
                .withCluster( clusterOfSize( "localhost", 3 ) )
                .withSharedConfig( stringMap(
                        HaSettings.ha_server.name(), "localhost:6001-9999",
                        HaSettings.tx_push_factor.name(), "2" ) ).build();
        try
        {
            clusterManager.start();

            clusterManager.getCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }

            HighlyAvailableGraphDatabase anySlave = clusterManager.getCluster().getAnySlave();
            try ( Transaction ignore = anySlave.beginTx() )
            {
                Node node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testClusterWithWildcardIP() throws Throwable
    {
        ClusterManager clusterManager =
                new ClusterManager.Builder( testDirectory.directory(  "testClusterWithWildcardIP" ) )
                .withSharedConfig( stringMap(
                        HaSettings.ha_server.name(), "0.0.0.0:6001-9999",
                        HaSettings.tx_push_factor.name(), "2" ) ).build();
        try
        {
            clusterManager.start();

            clusterManager.getCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }

            HighlyAvailableGraphDatabase anySlave = clusterManager.getCluster().getAnySlave();
            try ( Transaction ignore = anySlave.beginTx() )
            {
                Node node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    @Ignore( "JH: Ignored for by CG in March 2013, needs revisit. I added @ignore instead of commenting out to list " +
            "this in static analysis." )
    public void testArbiterStartsFirstAndThenTwoInstancesJoin() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "testCluster" ) )
                .withCluster( clusterWithAdditionalArbiters( 2, 1 ) ).build();
        try
        {
            clusterManager.start();
            clusterManager.getCluster().await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase master = clusterManager.getCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode();
                tx.success();
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testInstancesWithConflictingClusterPorts() throws Throwable
    {
        HighlyAvailableGraphDatabase first = null;
        try
        {
            File masterStoreDir =
                    testDirectory.directory( "testConflictingClusterPortsMaster" );
            first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newEmbeddedDatabaseBuilder( masterStoreDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                File slaveStoreDir =
                        testDirectory.directory( "testConflictingClusterPortsSlave" );
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newEmbeddedDatabaseBuilder( slaveStoreDir )
                        .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.server_id, "2" )
                        .setConfig( HaSettings.ha_server, "127.0.0.1:6667" )
                        .newGraphDatabase();
                failed.shutdown();
                fail("Should not start when ports conflict");
            }
            catch ( Exception e )
            {
                // good
            }
        }
        finally
        {
            if ( first != null )
            {
                first.shutdown();
            }
        }
    }

    @Test
    public void testInstancesWithConflictingHaPorts() throws Throwable
    {
        HighlyAvailableGraphDatabase first = null;
        try
        {
            File storeDir =
                    testDirectory.directory( "testConflictingHaPorts" );
             first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                     newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newEmbeddedDatabaseBuilder( storeDir )
                        .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5002" )
                        .setConfig( ClusterSettings.server_id, "2" )
                        .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                        .newGraphDatabase();
                failed.shutdown();
                fail( "Should not start when ports conflict" );
            }
            catch ( Exception e )
            {
                // good
            }
        }
        finally
        {
            if ( first != null )
            {
                first.shutdown();
            }
        }
    }

    @Test
    public void given4instanceClusterWhenMasterGoesDownThenElectNewMaster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "4instances" ) )
                .withCluster( ClusterManager.clusterOfSize( 4 ) ).build();
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getCluster();
            cluster.await( allSeesAllAsAvailable() );

            logging.getLogger().info( "STOPPING MASTER" );
            cluster.shutdown( cluster.getMaster() );
            logging.getLogger().info( "STOPPED MASTER" );

            cluster.await( ClusterManager.masterAvailable() );

            GraphDatabaseService master = cluster.getMaster();
            logging.getLogger().info( "CREATE NODE" );
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode();
                logging.getLogger().info( "CREATED NODE" );
                tx.success();
            }

            logging.getLogger().info( "STOPPING CLUSTER" );
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void givenEmptyHostListWhenClusterStartupThenFormClusterWithSingleInstance() throws Exception
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( testDirectory.directory( "singleinstance" ) ).
                setConfig( ClusterSettings.server_id, "1" ).
                setConfig( ClusterSettings.initial_hosts, "" ).
                newGraphDatabase();

        try
        {
            assertTrue( "Single instance cluster was not formed in time", db.isAvailable( 1_000 ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void givenClusterWhenMasterGoesDownAndTxIsRunningThenDontWaitToSwitch() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "waitfortx" ) )
                .withCluster( ClusterManager.clusterOfSize( 3 ) ).build();
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getCluster();
            cluster.await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            try ( Transaction tx = slave.beginTx() )
            {
                // Do a little write operation so that all "write" aspects of this tx is initializes properly
                slave.createNode();

                // Shut down master while we're keeping this transaction open
                cluster.shutdown( cluster.getMaster() );

                cluster.await( masterAvailable() );
                cluster.await( masterSeesSlavesAsAvailable( 1 ) );
                // Ending up here means that we didn't wait for this transaction to complete

                tx.success();
            }
            catch ( TransactionFailureException e )
            {
                // Good
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }
}

