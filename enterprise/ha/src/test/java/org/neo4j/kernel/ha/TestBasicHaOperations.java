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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

/**
 * TODO
 */
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
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.directory( "failover", true ), stringMap() );
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

        logger.getLogger().warn( "Failover took:"+(end-start)/1000000+"ms" );

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
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.directory( "propagation", true ),
                stringMap( tx_push_factor.name(), "2" ) );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        long nodeId = 0;
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Transaction tx = null;
        try
        {
            tx = slave.beginTx();

            Node node = slave.createNode();
            node.setProperty( "Hello", "World" );
            nodeId = node.getId();

            tx.success();
        }
        catch ( Throwable ex )
        {
            ex.printStackTrace();
            fail();
        }
        finally
        {
            if ( tx != null ) tx.finish();
        }

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Transaction transaction = master.beginTx();
        try
        {
            String value = master.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void testBasicPropagationFromMasterToSlave() throws Throwable
    {
        // given
        clusterManager = new ClusterManager( clusterOfSize( 3 ), dir.directory( "propagation", true ),
                stringMap( tx_push_factor.name(), "2" ) );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        long nodeId = 0;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Transaction tx = null;
        try
        {
            tx = master.beginTx();

            Node node = master.createNode();
            node.setProperty( "Hello", "World" );
            nodeId = node.getId();

            tx.success();
        }
        catch ( Throwable ex )
        {
            ex.printStackTrace();
            fail();
        }
        finally
        {
            if ( tx != null ) tx.finish();
        }

        // No need to wait, the push factor is 2
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        Transaction transaction = slave1.beginTx();
        String value;
        try
        {
            value = slave1.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
        }
        finally
        {
            transaction.finish();
        }


        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave(slave1);
        transaction = slave2.beginTx();
        try
        {
            value = slave2.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
        }
        finally
        {
            transaction.finish();
        }
    }
}
