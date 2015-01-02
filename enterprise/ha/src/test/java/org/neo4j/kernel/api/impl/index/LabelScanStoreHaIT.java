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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.LockObtainFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.test.ha.ClusterManager.allAvailabilityGuardsReleased;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class LabelScanStoreHaIT
{
    @Test
    public void shouldCopyLabelScanStoreToNewSlaves() throws Exception
    {
        // This check is here o check so that the extension provided by this test is selected.
        // It can be higher than 3 (number of cluster members) since some members may restart
        // some services to switch role.
        assertTrue( monitor.callsTo_init >= 3 );

        // GIVEN
        // An HA cluster where the master started with initial data consisting
        // of a couple of nodes, each having one or more properties.
        // The cluster starting up should have the slaves copy their stores from the master
        // and get the label scan store with it.

        // THEN
        assertEquals( "Expected noone to build their label scan store index.",
                0, monitor.timesRebuiltWithData );
        for ( GraphDatabaseService db : cluster.getAllMembers() )
        {
            assertEquals( 2, numberOfNodesHavingLabel( db, Labels.First ) );
            assertEquals( 2, numberOfNodesHavingLabel( db, Labels.First ) );
        }
    }

    private int numberOfNodesHavingLabel( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int count = count( GlobalGraphOperations.at( db ).getAllNodesWithLabel( label ) );
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

    private static enum Labels implements Label
    {
        First,
        Second;
    }

    private final File rootDirectory = TargetDirectory.forTest( getClass() ).cleanDirectory( "root" );
    private ClusterManager clusterManager;
    private final LifeSupport life = new LifeSupport();
    private ManagedCluster cluster;
    private final TestMonitor monitor = new TestMonitor();

    @Before
    public void setup()
    {
        KernelExtensionFactory<?> testExtension = new LuceneLabelScanStoreExtension( 100, monitor );
        HighlyAvailableGraphDatabaseFactory factory = new HighlyAvailableGraphDatabaseFactory();
        factory.addKernelExtensions( Arrays.<KernelExtensionFactory<?>>asList( testExtension ) );
        clusterManager = new ClusterManager.Builder( rootDirectory )
                .withDbFactory( factory )
                .withStoreDirInitializer( new ClusterManager.StoreDirInitializer()
        {
            @Override
            public void initializeStoreDir( int serverId, File storeDir ) throws IOException
            {
                if ( serverId == 1 )
                {
                    GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                            storeDir.getAbsolutePath() );
                    try
                    {
                        createSomeLabeledNodes( db,
                                new Label[] {Labels.First},
                                new Label[] {Labels.First, Labels.Second},
                                new Label[] {Labels.Second} );
                    }
                    finally
                    {
                        db.shutdown();
                    }
                }
            }
        } ).build();
        life.add( clusterManager );
        life.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
        cluster.await( allAvailabilityGuardsReleased() );
    }

    @After
    public void teardown()
    {
        life.shutdown();
    }

    private static class TestMonitor implements LuceneLabelScanStore.Monitor
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
        public void lockedIndex( LockObtainFailedException e )
        {
        }

        @Override
        public void corruptIndex( IOException e )
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
