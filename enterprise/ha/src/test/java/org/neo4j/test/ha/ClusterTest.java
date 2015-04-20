/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.net.InetAddress;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.fromXml;

public class ClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap(HaSettings.ha_server.name(), ":6001-6005",
                                  HaSettings.tx_push_factor.name(), "2"));
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();


            HighlyAvailableGraphDatabase slave = clusterManager.getDefaultCluster().getAnySlave();
            Transaction transaction = slave.beginTx();
            try
            {
                node = slave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
            finally
            {
                transaction.finish();
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void testClusterWithHostnames() throws Throwable
    {
        String hostName = InetAddress.getLocalHost().getHostName();
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        for ( int i = 0; i < 3; i++ )
        {
            cluster.getMembers().add( new Clusters.Member( hostName +":"+(5001 + i), true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );

        ClusterManager clusterManager = new ClusterManager( ClusterManager.provided( clusters ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), hostName+":6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ));
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            HighlyAvailableGraphDatabase anySlave = clusterManager.getDefaultCluster().getAnySlave();
            try(Transaction ignore = anySlave.beginTx())
            {
                node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void testClusterWithWildcardIP() throws Throwable
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        for ( int i = 0; i < 3; i++ )
        {
            cluster.getMembers().add( new Clusters.Member( (5001 + i), true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );

        ClusterManager clusterManager = new ClusterManager( ClusterManager.provided( clusters ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), "0.0.0.0:6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ));
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            HighlyAvailableGraphDatabase anySlave = clusterManager.getDefaultCluster().getAnySlave();
            try(Transaction ignore = anySlave.beginTx())
            {
                node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test @Ignore("JH: Ignored for by CG in March 2013, needs revisit. I added @ignore instead of commenting out to list this in static analysis.")
    public void testArbiterStartsFirstAndThenTwoInstancesJoin() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( ClusterManager.clusterWithAdditionalArbiters( 2, 1 ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ), MapUtil.stringMap());
        try
        {
            clusterManager.start();
            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            Transaction tx = master.beginTx();
            master.createNode();
            tx.success();
            tx.finish();
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void testInstancesWithConflictingClusterPorts() throws Throwable
    {
        HighlyAvailableGraphDatabase first = null;
        try
        {
            String masterStoreDir =
                    TargetDirectory.forTest( getClass() ).cleanDirectory( "testConflictingClusterPortsMaster" ).getAbsolutePath();
            first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( masterStoreDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                String slaveStoreDir =
                        TargetDirectory.forTest( getClass() ).cleanDirectory( "testConflictingClusterPortsSlave" ).getAbsolutePath();
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newHighlyAvailableDatabaseBuilder( slaveStoreDir )
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
            String storeDir =
                    TargetDirectory.forTest( getClass() ).cleanDirectory( "testConflictingHaPorts" ).getAbsolutePath();
             first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( storeDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newHighlyAvailableDatabaseBuilder( storeDir )
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
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/fourinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "4instances" ), MapUtil.stringMap() );
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            logging.getLogger().info( "STOPPING MASTER" );
            cluster.shutdown( cluster.getMaster() );
            logging.getLogger().info( "STOPPED MASTER" );

            cluster.await( ClusterManager.masterAvailable() );

            GraphDatabaseService master = cluster.getMaster();
            logging.getLogger().info( "CREATE NODE" );
            Transaction tx = master.beginTx();
            master.createNode();
            logging.getLogger().info( "CREATED NODE" );
            tx.success();
            tx.finish();

            logging.getLogger().info( "STOPPING CLUSTER" );
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void givenEmptyHostListWhenClusterStartupThenFormClusterWithSingleInstance() throws Exception
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( TargetDirectory.forTest( getClass() ).cleanDirectory(
                        "singleinstance" ).getAbsolutePath() ).
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
}

