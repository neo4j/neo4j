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
package org.neo4j.ha;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterManager.RepairKit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.SillyUtils.nonNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class TestBranchedData
{
    private final File dir = TargetDirectory.forTest( getClass() ).makeGraphDbDir();

    @Test
    public void migrationOfBranchedDataDirectories() throws Exception
    {
        long[] timestamps = new long[3];
        for ( int i = 0; i < timestamps.length; i++ )
        {
            startDbAndCreateNode();
            timestamps[i] = moveAwayToLookLikeOldBranchedDirectory();
            Thread.sleep( 1 ); // To make sure we get different timestamps
        }

        new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( dir.getAbsolutePath() )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( ClusterSettings.initial_hosts, ":5001" )
                .newGraphDatabase().shutdown();
        // It should have migrated those to the new location. Verify that.
        for ( long timestamp : timestamps )
        {
            assertFalse( "directory branched-" + timestamp + " still exists.",
                    new File( dir, "branched-" + timestamp ).exists() );
            assertTrue( "directory " + timestamp + " is not there",
                    BranchedDataPolicy.getBranchedDataDirectory( dir, timestamp ).exists() );
        }
    }
    
    @Test
    public void shouldCopyStoreFromMasterIfBranched() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = life.add( new ClusterManager( clusterOfSize( 2 ), dir, stringMap() ) );
        life.start();
        ManagedCluster cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
        createNode( cluster.getMaster(), "A" );
        cluster.sync();
        
        // WHEN
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        String storeDir = slave.getStoreDir();
        RepairKit starter = cluster.shutdown( slave );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        createNode( master, "B1" );
        createNode( master, "C" );
        createTransaction( storeDir, "B2" );
        slave = starter.repair();

        // THEN
        cluster.await( allSeesAllAsAvailable() );
        slave.beginTx().finish();
    }
    
    private final LifeSupport life = new LifeSupport();

    @After
    public void after()
    {
        life.shutdown();
    }

    private void createTransaction( String storeDir, String name )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            createNode( db, name );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void createNode( GraphDatabaseService db, String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();//.setProperty( "name", name );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private long moveAwayToLookLikeOldBranchedDirectory() throws IOException
    {
        long timestamp = System.currentTimeMillis();
        File branchDir = new File( dir, "branched-" + timestamp );
        assertTrue( "create directory: " + branchDir, branchDir.mkdirs() );
        for ( File file : nonNull( dir.listFiles() ) )
        {
            String fileName = file.getName();
            if ( !fileName.equals( StringLogger.DEFAULT_NAME ) && !file.getName().startsWith( "branched-" ) )
            {
                assertTrue( FileUtils.renameFile( file, new File( branchDir, file.getName() ) ) );
            }
        }
        return timestamp;
    }

    private void startDbAndCreateNode()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.getAbsolutePath() );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        db.shutdown();
    }
}
