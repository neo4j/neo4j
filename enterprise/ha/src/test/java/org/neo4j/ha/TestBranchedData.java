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
package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.impl.util.StoreUtil;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static java.lang.String.format;

import static org.neo4j.helpers.SillyUtils.nonNull;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class TestBranchedData
{
    public final @Rule LifeRule life = new LifeRule( true );
    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

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

        File dir = directory.directory();
        new TestHighlyAvailableGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( dir )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( ClusterSettings.initial_hosts, "localhost:5001" )
                .newGraphDatabase().shutdown();
        // It should have migrated those to the new location. Verify that.
        for ( long timestamp : timestamps )
        {
            assertFalse( "directory branched-" + timestamp + " still exists.",
                    new File( dir, "branched-" + timestamp ).exists() );
            assertTrue( "directory " + timestamp + " is not there",
                    StoreUtil.getBranchedDataDirectory( dir, timestamp ).exists() );
        }
    }

    @Test
    public void shouldCopyStoreFromMasterIfBranched() throws Throwable
    {
        // GIVEN
        File dir = directory.directory();
        ClusterManager clusterManager = life.add( new ClusterManager.Builder( dir )
                .withProvider( clusterOfSize( 2 ) ).build() );
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
        createNodeOffline( storeDir, "B2" );
        slave = starter.repair();

        // THEN
        cluster.await( allSeesAllAsAvailable() );
        slave.beginTx().close();
    }

    /**
     * Main difference to {@link #shouldCopyStoreFromMasterIfBranched()} is that no instances are shut down
     * during the course of the test. This to test functionality of some internal components being restarted.
     */
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldCopyStoreFromMasterIfBranchedInLiveScenario() throws Throwable
    {
        // GIVEN a cluster of 3, all having the same data (node A)
        // thor is whoever is the master to begin with
        // odin is whoever is picked as _the_ slave given thor as initial master
        File dir = directory.directory();
        ClusterManager clusterManager = life.add( new ClusterManager.Builder( dir )
                .withSharedConfig( stringMap(
                // Effectively disable automatic transaction propagation within the cluster
                HaSettings.tx_push_factor.name(), "0",
                HaSettings.pull_interval.name(), "0" ) ).build() );
        ManagedCluster cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
        HighlyAvailableGraphDatabase thor = cluster.getMaster();
        String indexName = "valhalla";
        createNode( thor, "A", andIndexInto( indexName ) );
        cluster.sync();

        // WHEN creating a node B1 on thor (note the disabled cluster transaction propagation)
        createNode( thor, "B1", andIndexInto( indexName ) );
        // and right after that failing the master so that it falls out of the cluster
        HighlyAvailableGraphDatabase odin = cluster.getAnySlave();
        cluster.info( format( "%n   ==== TAMPERING WITH " + thor + "'s CABLES ====%n" ) );
        RepairKit thorRepairKit = cluster.fail( thor );
        // try to create a transaction on odin until it succeeds
        cluster.await( ClusterManager.masterAvailable( thor ) );
        createNode( odin, "B2", andIndexInto( indexName ) );
        assertTrue( odin.isMaster() );
        // perform transactions so that index files changes under the hood
        Set<File> odinLuceneFilesBefore = asSet( gatherLuceneFiles( odin, indexName ) );
        for ( char prefix = 'C'; !changed( odinLuceneFilesBefore, asSet( gatherLuceneFiles( odin, indexName ) ) ); prefix++ )
        {
            createNodes( odin, String.valueOf( prefix ), 10_000, andIndexInto( indexName ) );
            cluster.force(); // Force will most likely cause lucene legacy indexes to commit and change file structure
        }
        // so anyways, when thor comes back into the cluster
        cluster.info( format( "%n   ==== REPAIRING CABLES ====%n" ) );
        thorRepairKit.repair();
        while ( thor.isMaster() )
        {
            Thread.sleep( 100 );
        }
        cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );
        assertFalse( thor.isMaster() );

        // Now do some more transactions on current master (odin) and have thor pull those
        for( int i = 0; i < 3; i++ )
        {
            createNodes( odin, String.valueOf( "" + i ), 100_000, andIndexInto( indexName ) );
            cluster.sync();
            cluster.force();
        }

        // THEN thor should be a slave, having copied a store from master and good to go
        assertFalse( hasNode( thor, "B1" ) );
        assertTrue( hasNode( thor, "B2" ) );
        assertTrue( hasNode( thor, "C-0" ) );
        assertTrue( hasNode( thor, "0-0" ) );
        assertTrue( hasNode( odin, "0-0" ) );
    }

    private boolean changed( Set<File> before, Set<File> after )
    {
        return !before.containsAll( after ) && !after.containsAll( before );
    }

    private Collection<File> gatherLuceneFiles( HighlyAvailableGraphDatabase db, String indexName ) throws IOException
    {
        Collection<File> result = new ArrayList<>();
        NeoStoreDataSource ds = db.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
        try ( ResourceIterator<File> files = ds.listStoreFiles( false ) )
        {
            while ( files.hasNext() )
            {
                File file = files.next();
                if ( file.getPath().contains( indexName ) )
                {
                    result.add( file );
                }
            }
        }
        return result;
    }

    private Listener<Node> andIndexInto( final String indexName )
    {
        return new Listener<Node>()
        {
            @Override
            public void receive( Node node )
            {
                Index<Node> index = node.getGraphDatabase().index().forNodes( indexName );
                for ( String key : node.getPropertyKeys() )
                {
                    index.add( node, key, node.getProperty( key ) );
                }
            }
        };
    }

    private boolean hasNode( GraphDatabaseService db, String nodeName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                if ( nodeName.equals( node.getProperty( "name", null ) ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    @SuppressWarnings( "unchecked" )
    private void createNodeOffline( String storeDir, String name )
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

    @SuppressWarnings( "unchecked" )
    private void createNode( GraphDatabaseService db, String name, Listener<Node>... additional )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = createNamedNode( db, name );
            for ( Listener<Node> listener : additional )
            {
                listener.receive( node );
            }
            tx.success();
        }
    }

    @SuppressWarnings( "unchecked" )
    private void createNodes( GraphDatabaseService db, String namePrefix, int count, Listener<Node>... additional )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < count; i++ )
            {
                Node node = createNamedNode( db, namePrefix + "-" + i );
                for ( Listener<Node> listener : additional )
                {
                    listener.receive( node );
                }
            }
            tx.success();
        }
    }

    private Node createNamedNode( GraphDatabaseService db, String name )
    {
        Node node = db.createNode();
        node.setProperty( "name", name );
        return node;
    }

    private long moveAwayToLookLikeOldBranchedDirectory() throws IOException
    {
        File dir = directory.directory();
        long timestamp = System.currentTimeMillis();
        File branchDir = new File( dir, "branched-" + timestamp );
        assertTrue( "create directory: " + branchDir, branchDir.mkdirs() );
        for ( File file : nonNull( dir.listFiles() ) )
        {
            String fileName = file.getName();
            if ( !fileName.equals( StoreLogService.INTERNAL_LOG_NAME ) && !file.getName().startsWith( "branched-" ) )
            {
                assertTrue( FileUtils.renameFile( file, new File( branchDir, file.getName() ) ) );
            }
        }
        return timestamp;
    }

    private void startDbAndCreateNode()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }
}
