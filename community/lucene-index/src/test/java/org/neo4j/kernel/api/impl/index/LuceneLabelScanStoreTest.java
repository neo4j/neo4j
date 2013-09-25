/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.store.LockObtainFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.TargetDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;
import static org.neo4j.kernel.api.scan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

public class LuceneLabelScanStoreTest
{
    private static final long[] NO_LABELS = new long[0];

    @Test
    public void shouldUpdateIndexOnLabelChange() throws Exception
    {
        // GIVEN
        int labelId = 1;
        long nodeId = 10;
        start();

        // WHEN
        store.updateAndCommit( iterator( labelChanges( nodeId, NO_LABELS, new long[] {labelId} ) ) );

        // THEN
        assertNodesForLabel( labelId, nodeId );
    }

    @Test
    public void shouldUpdateIndexOnAddedLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 1, labelId2 = 2;
        long nodeId = 10;
        start();
        store.updateAndCommit( iterator( labelChanges( nodeId, NO_LABELS, new long[] {labelId1} ) ) );
        assertNodesForLabel( labelId2 );

        // WHEN
        store.updateAndCommit( iterator( labelChanges( nodeId, NO_LABELS, new long[] {labelId1, labelId2} ) ) );

        // THEN
        assertNodesForLabel( labelId1, nodeId );
        assertNodesForLabel( labelId2, nodeId );
    }

    @Test
    public void shouldUpdateIndexOnRemovedLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 1, labelId2 = 2;
        long nodeId = 10;
        start();
        store.updateAndCommit( iterator( labelChanges( nodeId, NO_LABELS, new long[] {labelId1, labelId2} ) ) );
        assertNodesForLabel( labelId1, nodeId );
        assertNodesForLabel( labelId2, nodeId );

        // WHEN
        store.updateAndCommit( iterator( labelChanges( nodeId, new long[] {labelId1, labelId2}, new long[] {labelId2} ) ) );

        // THEN
        assertNodesForLabel( labelId1 );
        assertNodesForLabel( labelId2, nodeId );
    }

    @Test
    public void shouldDeleteFromIndexWhenDeletedNode() throws Exception
    {
        // GIVEN
        int labelId = 1;
        long nodeId = 10;
        start();
        store.updateAndCommit( iterator( labelChanges( nodeId, NO_LABELS, new long[] {labelId} ) ) );

        // WHEN
        store.updateAndCommit( iterator( labelChanges( nodeId, new long[] {labelId}, NO_LABELS ) ) );

        // THEN
        assertNodesForLabel( labelId );
    }

    @Test
    public void shouldRebuildFromScratchIfIndexMissing() throws Exception
    {
        // GIVEN a start of the store with existing data in it
        start( asList(
                labelChanges( 1, NO_LABELS, new long[] {1} ),
                labelChanges( 2, NO_LABELS, new long[] {1, 2} )
                ) );

        // THEN
        assertTrue( "Didn't rebuild the store on startup",
                monitor.noIndexCalled&monitor.rebuildingCalled&monitor.rebuiltCalled );
        assertNodesForLabel( 1,
                1, 2 );
        assertNodesForLabel( 2,
                2 );
    }

    @Test
    public void shouldRefuseStartIfIndexCorrupted() throws Exception
    {
        // GIVEN a start of the store with existing data in it
        usePersistentDirectory();
        List<NodeLabelUpdate> data = asList(
                labelChanges( 1, NO_LABELS, new long[] {1} ),
                labelChanges( 2, NO_LABELS, new long[] {1, 2} ) );
        start( data );

        // WHEN the index is corrupted and then started again
        try
        {
            scrambleIndexFilesAndRestart( data );
            fail("Should not have been able to start.");
        }
        catch( LifecycleException e )
        {
            assertThat(e.getCause(), instanceOf( IOException.class ));
            assertThat(e.getCause().getMessage(), equalTo(
                    "Label scan store is corrupted, and needs to be rebuilt. To trigger a rebuild, ensure the " +
                    "database is stopped, delete the files in '"+dir.getAbsolutePath()+"', and then start the " +
                    "database again." ));
        }
    }

    private void assertNodesForLabel( int labelId, long... expectedNodeIds )
    {
        Set<Long> nodeSet = new HashSet<>();
        PrimitiveLongIterator nodes = store.newReader().nodesWithLabel( labelId );
        while ( nodes.hasNext() )
        {
            nodeSet.add( nodes.next() );
        }

        for ( long expectedNodeId : expectedNodeIds )
        {
            assertTrue( "Expected node " + expectedNodeId + " not found in scan store",
                    nodeSet.remove( expectedNodeId ) );
        }
        assertTrue( "Unexpected nodes in scan store " + nodeSet, nodeSet.isEmpty() );
    }

    private final File dir = TargetDirectory.forTest( getClass() ).directory( "lucene", true );
    private final Random random = new Random();
    private DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private LifeSupport life;
    private TrackingMonitor monitor;
    private LuceneLabelScanStore store;

    private List<NodeLabelUpdate> noData()
    {
        return emptyList();
    }

    private void usePersistentDirectory()
    {
        directoryFactory = DirectoryFactory.PERSISTENT;
    }

    private void start()
    {
        start( noData() );
    }

    private void start( List<NodeLabelUpdate> existingData )
    {
        life = new LifeSupport();
        monitor = new TrackingMonitor();
        store = life.add( new LuceneLabelScanStore( new LuceneDocumentStructure(), directoryFactory, dir,
                new DefaultFileSystemAbstraction(), standard(), asStream( existingData ), monitor ) );
        life.start();
        assertTrue( monitor.initCalled );
    }

    private FullStoreChangeStream asStream( final List<NodeLabelUpdate> existingData )
    {
        return new FullStoreChangeStream()
        {
            @Override
            public Iterator<NodeLabelUpdate> iterator()
            {
                return existingData.iterator();
            }

            @Override
            public long highestNodeId()
            {
                return existingData.size(); // Well... not really
            }

            @Override
            public PrimitiveLongIterator labelIds()
            {
                return emptyPrimitiveLongIterator();
            }
        };
    }

    private void scrambleIndexFilesAndRestart( List<NodeLabelUpdate> data ) throws IOException
    {
        shutdown();
        File[] files = dir.listFiles();
        if ( files != null )
        {
            for ( File indexFile : files )
            {
                scrambleFile( indexFile );
            }
        }
        start( data );
    }

    private void scrambleFile( File file ) throws IOException
    {
        try ( RandomAccessFile fileAccess = new RandomAccessFile( file, "rw" );
              FileChannel channel = fileAccess.getChannel() )
        {
            // The files will be small, so OK to allocate a buffer for the full size
            byte[] bytes = new byte[(int) channel.size()];
            putRandomBytes( bytes );
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( 0 );
            channel.write( buffer );
        }
    }

    private void putRandomBytes( byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
    }

    @Before
    public void clearDir() throws IOException
    {
        if(dir.exists())
        {
            deleteRecursively( dir );
        }
    }

    @After
    public void shutdown() throws IOException
    {
        life.shutdown();
    }

    private static class TrackingMonitor implements LuceneLabelScanStore.Monitor
    {
        boolean initCalled, rebuildingCalled, rebuiltCalled, noIndexCalled;

        @Override
        public void noIndex()
        {
            noIndexCalled = true;
        }

        @Override
        public void lockedIndex( LockObtainFailedException e )
        {
        }

        @Override
        public void corruptIndex( IOException corruptionException )
        {
        }

        @Override
        public void rebuilding()
        {
            rebuildingCalled = true;
        }

        @Override
        public void rebuilt( long roughNodeCount )
        {
            rebuiltCalled = true;
        }

        @Override
        public void init()
        {
            initCalled = true;
        }
    }
}
