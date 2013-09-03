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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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

import static org.junit.Assert.assertEquals;

import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class LabelScanStoreHaIT
{
    @Test
    public void shouldCopyLabelScanStoreToNewSlaves() throws Exception
    {
        // THEN
        assertEquals( "Expected noone to build their label scan store index.",
                0, monitor.callsTo_noIndex );
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
    
    private final File rootDirectory = TargetDirectory.forTest( getClass() ).directory( "root", true );
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
        clusterManager = new ClusterManager.Builder( rootDirectory ).withStoreDirInitializer(
                new ClusterManager.StoreDirInitializer()
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
    }
    
    @After
    public void teardown()
    {
        life.shutdown();
    }
    
    private static class TestMonitor implements LuceneLabelScanStore.Monitor
    {
        private volatile int callsTo_noIndex;
        
        @Override
        public void noIndex()
        {
            callsTo_noIndex++;
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
        public void rebuilt()
        {
        }
    }
}
