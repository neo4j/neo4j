/**
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
package org.neo4j.ha;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.fromXml;

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.EideticTransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

public class TransactionMonitoringIT
{
    @Test
    public void injectedTransactionCountShouldBeMonitored() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), ":6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ) );

        EideticTransactionMonitor masterMonitor = new EideticTransactionMonitor();
        EideticTransactionMonitor firstSlaveMonitor = new EideticTransactionMonitor();
        EideticTransactionMonitor secondSlaveMonitor = new EideticTransactionMonitor();

        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            master.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener(
                    masterMonitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

            HighlyAvailableGraphDatabase firstSlave = clusterManager.getDefaultCluster().getAnySlave();
            firstSlave.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener(
                    firstSlaveMonitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

            HighlyAvailableGraphDatabase secondSlave = clusterManager.getDefaultCluster().getAnySlave( firstSlave );
            secondSlave.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener(
                    secondSlaveMonitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

            // WHEN
            Transaction tx = master.beginTx();
            master.createNode();
            tx.success();
            tx.finish();

            tx = firstSlave.beginTx();
            firstSlave.createNode();
            tx.success();
            tx.finish();

            tx = secondSlave.beginTx();
            secondSlave.createNode();
            tx.success();
            tx.finish();
        }
        finally
        {
            clusterManager.stop();
        }

        // THEN
        assertEquals( 3, masterMonitor.getCommitCount() );
        assertEquals( 0, masterMonitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, masterMonitor.getInjectTwoPhaseCommitCount() );

        assertEquals( 1, firstSlaveMonitor.getCommitCount() );
        assertEquals( 2, firstSlaveMonitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, firstSlaveMonitor.getInjectTwoPhaseCommitCount() );

        assertEquals( 1, secondSlaveMonitor.getCommitCount() );
        assertEquals( 2, secondSlaveMonitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, secondSlaveMonitor.getInjectTwoPhaseCommitCount() );
    }

    @Test
    public void pullUpdatesShouldUpdateCounters() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).cleanDirectory( "testCluster" ),
                MapUtil.stringMap( HaSettings.ha_server.name(), ":6001-6005",
                        HaSettings.tx_push_factor.name(), "0" ) );

        EideticTransactionMonitor masterMonitor = new EideticTransactionMonitor();
        EideticTransactionMonitor firstSlaveMonitor = new EideticTransactionMonitor();

        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            GraphDatabaseAPI master = clusterManager.getDefaultCluster().getMaster();
            master.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener(
                    masterMonitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

            HighlyAvailableGraphDatabase firstSlave = clusterManager.getDefaultCluster().getAnySlave();
            firstSlave.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener(
                    firstSlaveMonitor, XaResourceManager.class.getName(), NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

            // WHEN
            for ( int i = 0; i < 10; i++ )
            {

                Transaction tx = master.beginTx();
                master.createNode();
                tx.success();
                tx.finish();
            }

            firstSlave.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        }
        finally
        {
            clusterManager.stop();
        }

        // THEN
        assertEquals( 0, firstSlaveMonitor.getCommitCount() );
        assertEquals( 10, firstSlaveMonitor.getInjectOnePhaseCommitCount() );
        assertEquals( 0, firstSlaveMonitor.getInjectTwoPhaseCommitCount() );
    }
}
