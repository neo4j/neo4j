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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.kernel.impl.ha.ClusterManager.allAvailabilityGuardsReleased;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;

public class NativeLabelScanStoreHaIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final LifeSupport life = new LifeSupport();
    private ManagedCluster cluster;
    private final TestMonitor monitor = new TestMonitor();

    private enum Labels implements Label
    {
        First,
        Second
    }

    @Before
    public void setUp()
    {
        TestHighlyAvailableGraphDatabaseFactory factory = new TestHighlyAvailableGraphDatabaseFactory();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        factory.setMonitors( monitors );
        factory.removeKernelExtensions( extension -> extension.getClass().getName().contains( "LabelScan" ) );
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "root" ) )
                .withDbFactory( factory )
                .withStoreDirInitializer( ( serverId, storeDir ) ->
                {
                    if ( serverId == 1 )
                    {
                        GraphDatabaseService db = new TestGraphDatabaseFactory()
                                .newEmbeddedDatabaseBuilder( storeDir.getAbsoluteFile() )
                                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                                .newGraphDatabase();
                        try
                        {
                            createSomeLabeledNodes( db,
                                    new Label[]{Labels.First},
                                    new Label[]{Labels.First, Labels.Second},
                                    new Label[]{Labels.Second} );
                        }
                        finally
                        {
                            db.shutdown();
                        }
                    }
                } ).build();
        life.add( clusterManager );
        life.start();
        cluster = clusterManager.getCluster();
        cluster.await( allSeesAllAsAvailable() );
        cluster.await( allAvailabilityGuardsReleased() );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    @Test
    public void shouldCopyLabelScanStoreToNewSlaves()
    {
        // This check is here o check so that the extension provided by this test is selected.
        // It can be higher than 3 (number of cluster members) since some members may restart
        // some services to switch role.
        assertTrue( "Expected initial calls to init to be at least one per cluster member (>= 3), " +
                        "but was " + monitor.callsTo_init,
                monitor.callsTo_init >= 3 );

        // GIVEN
        // An HA cluster where the master started with initial data consisting
        // of a couple of nodes, each having one or more properties.
        // The cluster starting up should have the slaves copy their stores from the master
        // and get the label scan store with it.

        // THEN
        assertEquals( "Expected none to build their label scan store index.",
                0, monitor.timesRebuiltWithData );
        for ( GraphDatabaseService db : cluster.getAllMembers() )
        {
            assertEquals( 2, numberOfNodesHavingLabel( db, Labels.First ) );
            assertEquals( 2, numberOfNodesHavingLabel( db, Labels.First ) );
        }
    }

    private long numberOfNodesHavingLabel( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long count = count( db.findNodes( label ) );
            tx.success();
            return count;
        }
    }

    private void createSomeLabeledNodes( GraphDatabaseService db, Label[]... labelArrays )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Label[] labels : labelArrays )
            {
                db.createNode( labels );
            }
            tx.success();
        }
    }

    private static class TestMonitor extends LabelScanStore.Monitor.Adaptor
    {
        private volatile int callsTo_init;
        private volatile int timesRebuiltWithData;

        @Override
        public void init()
        {
            callsTo_init++;
        }

        @Override
        public void noIndex()
        {
        }

        @Override
        public void notValidIndex()
        {
        }

        @Override
        public void rebuilding()
        {
        }

        @Override
        public void rebuilt( long roughNodeCount )
        {
            // In HA each slave database will startup with an empty database before realizing that
            // it needs to copy a store from its master, let alone find its master.
            // So we're expecting one call to this method from each slave with node count == 0. Ignore those.
            // We're tracking number of times we're rebuilding the index where there's data to rebuild,
            // i.e. after the db has been copied from the master.
            if ( roughNodeCount > 0 )
            {
                timesRebuiltWithData++;
            }
        }
    }
}
