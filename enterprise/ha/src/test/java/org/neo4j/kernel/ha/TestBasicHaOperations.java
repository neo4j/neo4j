/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.logging.Level;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBasicHaOperations
{
    @ClassRule
    public static LoggerRule logger = new LoggerRule( Level.OFF );
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() ).withSharedSetting( HaSettings.tx_push_factor, "2" );

    @Test
    public void testBasicFailover() throws Throwable
    {
        // given
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );

        // When
        long start = System.nanoTime();
        RepairKit repair = cluster.shutdown( master );
        try
        {
            logger.getLogger().warning( "Shut down master" );
            cluster.await( ClusterManager.masterAvailable() );
            long end = System.nanoTime();
            logger.getLogger().warning( "Failover took:" + (end - start) / 1000000 + "ms" );
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
        finally
        {
            repair.repair();
        }
    }

    @Test
    public void testBasicPropagationFromSlaveToMaster() throws Throwable
    {
        // given
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        long nodeId;

        // a node with a property
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
        }

        cluster.sync();

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
        ManagedCluster cluster = clusterRule.startCluster();
        long nodeId = 4;
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode();
            node.setProperty( "Hello", "World" );
            nodeId = node.getId();

            tx.success();
        }

        cluster.sync();

        // No need to wait, the push factor is 2
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        checkNodeOnSlave( nodeId, slave1 );

        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );
        checkNodeOnSlave( nodeId, slave2 );
    }

    private void checkNodeOnSlave( long nodeId, HighlyAvailableGraphDatabase slave2 )
    {
        try ( Transaction tx = slave2.beginTx() )
        {
            String value = slave2.getNodeById( nodeId ).getProperty( "Hello" ).toString();
            logger.getLogger().info( "Hello=" + value );
            assertEquals( "World", value );
            tx.success();
        }
    }
}
