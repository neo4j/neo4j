/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.ha;

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionCounters;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.fromXml;

public class TransactionMonitoringIT
{
    @Test
    public void countersShouldBeUpdatesOnAllMachinesWhenCommittingOnTheMaster() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = new ClusterManager(
                fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), ":6001-6005", HaSettings.tx_push_factor.name(), "2" )
        );

        TransactionCounters masterMonitor = null;
        TransactionCounters firstSlaveMonitor = null;
        TransactionCounters secondSlaveMonitor = null;
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            masterMonitor = master.getDependencyResolver().resolveDependency( TransactionCounters.class );

            HighlyAvailableGraphDatabase firstSlave = clusterManager.getDefaultCluster().getAnySlave();
            firstSlaveMonitor = firstSlave.getDependencyResolver().resolveDependency( TransactionCounters.class );

            HighlyAvailableGraphDatabase secondSlave = clusterManager.getDefaultCluster().getAnySlave( firstSlave );
            secondSlaveMonitor = secondSlave.getDependencyResolver().resolveDependency( TransactionCounters.class );

            // WHEN
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode();
                tx.success();
            }

            // make sure the slaves pulled updates
            firstSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            secondSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();

        }
        finally
        {
            clusterManager.stop();
        }

        // THEN
        assertNotNull( masterMonitor );
        assertEquals( 1, masterMonitor.getNumberOfCommittedTransactions() );

        assertNotNull( firstSlaveMonitor );
        assertEquals( 1, firstSlaveMonitor.getNumberOfCommittedTransactions() );

        assertNotNull( secondSlaveMonitor );
        assertEquals( 1, secondSlaveMonitor.getNumberOfCommittedTransactions() );
    }

    @Test
    public void countersShouldBeUpdatesOnAllMachinesWhenCommittingOnASlave() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = new ClusterManager(
                fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), ":6001-6005", HaSettings.tx_push_factor.name(), "2" )
        );

        TransactionCounters masterMonitor = null;
        TransactionCounters firstSlaveMonitor = null;
        TransactionCounters secondSlaveMonitor = null;
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            masterMonitor = master.getDependencyResolver().resolveDependency( TransactionCounters.class );

            HighlyAvailableGraphDatabase firstSlave = clusterManager.getDefaultCluster().getAnySlave();
            firstSlaveMonitor = firstSlave.getDependencyResolver().resolveDependency( TransactionCounters.class );

            HighlyAvailableGraphDatabase secondSlave = clusterManager.getDefaultCluster().getAnySlave( firstSlave );
            secondSlaveMonitor = secondSlave.getDependencyResolver().resolveDependency( TransactionCounters.class );

            // WHEN
            try ( Transaction tx = firstSlave.beginTx() )
            {
                firstSlave.createNode();
                tx.success();
            }

            // make sure the slaves pulled updates
            firstSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
            secondSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();

        }
        finally
        {
            clusterManager.stop();
        }

        // THEN
        assertNotNull( masterMonitor );
        assertEquals( 1, masterMonitor.getNumberOfCommittedTransactions() );

        assertNotNull( firstSlaveMonitor );
        assertEquals( 1, firstSlaveMonitor.getNumberOfCommittedTransactions() );

        assertNotNull( secondSlaveMonitor );
        assertEquals( 1, secondSlaveMonitor.getNumberOfCommittedTransactions() );
    }
}
