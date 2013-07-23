/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.test.ha.ClusterManager.fromXml;

public class ClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule();

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ),
                MapUtil.stringMap(HaSettings.ha_server.name(), ":6001-6005",
                                  HaSettings.tx_push_factor.name(), "2"));
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( ClusterManager.allSeesAllAsAvailable() );

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

//    @Test
    public void testArbiterStartsFirstAndThenTwoInstancesJoin() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( ClusterManager.clusterWithAdditionalArbiters( 2, 1 ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ), MapUtil.stringMap());
        try
        {
            clusterManager.start();
            clusterManager.getDefaultCluster().await( ClusterManager.allSeesAllAsAvailable() );

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
                    TargetDirectory.forTest( getClass() ).directory( "testConflictingClusterPortsMaster", true ).getAbsolutePath();
            first = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( masterStoreDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                String slaveStoreDir =
                        TargetDirectory.forTest( getClass() ).directory( "testConflictingClusterPortsSlave", true ).getAbsolutePath();
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
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
                    TargetDirectory.forTest( getClass() ).directory( "testConflictingHaPorts", true ).getAbsolutePath();
             first = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( storeDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
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
                TargetDirectory.forTest( getClass() ).directory( "4instances", true ), MapUtil.stringMap() );
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( ClusterManager.allSeesAllAsAvailable() );

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
}
