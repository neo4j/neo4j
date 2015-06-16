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
package org.neo4j.kernel.ha;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;


public class TestBasicHaOperations
{
    @Rule
    public LoggerRule logger = new LoggerRule();

    public TargetDirectory dir = TargetDirectory.forTest( getClass() );
    private ClusterManager clusterManager;

    @After
    public void after() throws Throwable
    {
        if ( clusterManager != null )
        {
            clusterManager.stop();
            clusterManager = null;
        }
    }

    @Test
    public void testBasicFailover() throws Throwable
    {
        // given
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.cleanDirectory( "failover" ), stringMap() );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );

        // When
        long start = System.nanoTime();
        cluster.shutdown( master );
        logger.getLogger().warn( "Shut down master" );

        cluster.await( ClusterManager.masterAvailable() );
        long end = System.nanoTime();

        logger.getLogger().warn( "Failover took:" + (end - start) / 1000000 + "ms" );

        // Then
        boolean slave1Master = slave1.isMaster();
        boolean slave2Master = slave2.isMaster();

        if ( slave1Master )
        {
            assertFalse( slave2Master );
        }
        else
        {
            assertTrue( slave2Master );
        }
    }

    @Test
    public void testBasicPropagationFromSlaveToMaster() throws Throwable
    {
        // given
        // a cluster of 2
        clusterManager = new ClusterManager( clusterOfSize( 2 ), dir.cleanDirectory( "propagation" ),
                stringMap(HaSettings.tx_push_factor.name(), "1" ) );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        long nodeId;

        // a node with a property
        try( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
        }

        try( Transaction tx = master.beginTx() )
        {
            // make sure it's in the cache
            master.getNodeById( nodeId ).getProperty( "foo" );
            tx.success();
        }

        // which has propagated to the slaves
        slave.getDependencyResolver().resolveDependency( UpdatePullerClient.class ).pullUpdates();

        // when
        // the slave does a change
        try ( Transaction tx = slave.beginTx() )
        {
            slave.getNodeById( nodeId ).setProperty( "foo", "bar2" );
            tx.success();
        }

        // then
        // the master must pick up the change
        try ( Transaction tx = master.beginTx() )
        {
            assertEquals( "bar2", master.getNodeById( nodeId ).getProperty( "foo" ) );
            tx.success();
        }
    }

    @Test
    public void testBasicPropagationFromMasterToSlave() throws Throwable
    {
        // given
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.cleanDirectory( "propagation" ),
                stringMap( tx_push_factor.name(), "2" ) );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );

        long nodeId = 4;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            node.setProperty( "Hello", "World" );
            nodeId = node.getId();

            tx.success();
        }

        // No need to wait, the push factor is 2
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        String value;
        try ( Transaction tx = slave1.beginTx() )
        {
            value = slave1.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
            tx.success();
        }

        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave(slave1);
        try ( Transaction tx = slave2.beginTx() )
        {
            value = slave2.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
            tx.success();
        }
    }
}
