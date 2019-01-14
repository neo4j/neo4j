/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.labelscan;

import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.asStream;

public abstract class LabelScanStoreTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    protected final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    final RandomRule random = new RandomRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( random ).around( testDirectory ).around( expectedException )
            .around( fileSystemRule );

    private static final long[] NO_LABELS = new long[0];

    private LifeSupport life;
    private TrackingMonitor monitor;
    private LabelScanStore store;
    protected File dir;

    @Before
    public void clearDir()
    {
        dir = testDirectory.directory();
    }

    @After
    public void shutdown()
    {
        if ( life != null )
        {
            life.shutdown();
        }
    }

    protected abstract LabelScanStore createLabelScanStore( FileSystemAbstraction fileSystemAbstraction,
            File rootFolder, FullStoreChangeStream fullStoreChangeStream, boolean usePersistentStore, boolean readOnly,
            LabelScanStore.Monitor monitor );

    @Test
    public void failToRetrieveWriterOnReadOnlyScanStore()
    {
        createAndStartReadOnly();
        expectedException.expect( UnsupportedOperationException.class );
        store.newWriter();
    }

    @Test
    public void forceShouldNotForceWriterOnReadOnlyScanStore()
    {
        createAndStartReadOnly();
        store.force( IOLimiter.unlimited() );
    }

    @Test
    public void shouldStartIfLabelScanStoreIndexDoesNotExistInReadOnlyMode() throws IOException
    {
        // WHEN
        start( false, true );

        // THEN

        // no exception
        assertTrue( store.isEmpty() );
    }

    @Test
    public void snapshotReadOnlyLabelScanStore() throws IOException
    {
        prepareIndex();
        createAndStartReadOnly();
        try ( ResourceIterator<File> indexFiles = store.snapshotStoreFiles() )
        {
            List<String> filesNames = indexFiles.stream().map( File::getName ).collect( toList() );
            assertThat( "Should have at least index segment file.", filesNames, hasBareMinimumFileList() );
        }
    }

    protected abstract Matcher<Iterable<? super String>> hasBareMinimumFileList();

    @Test
    public void shouldUpdateIndexOnLabelChange() throws Exception
    {
        // GIVEN
        int labelId = 1;
        long nodeId = 10;
        start();

        // WHEN
        write( iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId} ) ) );

        // THEN
        assertNodesForLabel( labelId, nodeId );
    }

    @Test
    public void shouldUpdateIndexOnAddedLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId = 10;
        start();
        write( iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId1} ) ) );
        assertNodesForLabel( labelId2 );

        // WHEN
        write( iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId1, labelId2} ) ) );

        // THEN
        assertNodesForLabel( labelId1, nodeId );
        assertNodesForLabel( labelId2, nodeId );
    }

    @Test
    public void shouldUpdateIndexOnRemovedLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId = 10;
        start();
        write( iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId1, labelId2} ) ) );
        assertNodesForLabel( labelId1, nodeId );
        assertNodesForLabel( labelId2, nodeId );

        // WHEN
        write( iterator( labelChanges( nodeId, new long[]{labelId1, labelId2}, new long[]{labelId2} ) ) );

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
        write( iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId} ) ) );

        // WHEN
        write( iterator( labelChanges( nodeId, new long[]{labelId}, NO_LABELS ) ) );

        // THEN
        assertNodesForLabel( labelId );
    }

    @Test
    public void shouldScanSingleRange()
    {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId1 = 10;
        long nodeId2 = 11;
        start( asList(
                labelChanges( nodeId1, NO_LABELS, new long[]{labelId1} ),
                labelChanges( nodeId2, NO_LABELS, new long[]{labelId1, labelId2} )
        ) );

        // WHEN
        BoundedIterable<NodeLabelRange> reader = store.allNodeLabelRanges();
        NodeLabelRange range = single( reader.iterator() );

        // THEN
        assertArrayEquals( new long[]{nodeId1, nodeId2}, reducedNodes( range ) );

        assertArrayEquals( new long[]{labelId1}, sorted( range.labels( nodeId1 ) ) );
        assertArrayEquals( new long[]{labelId1, labelId2}, sorted( range.labels( nodeId2 ) ) );
    }

    @Test
    public void shouldScanMultipleRanges()
    {
        // GIVEN
        int labelId1 = 1;
        int labelId2 = 2;
        long nodeId1 = 10;
        long nodeId2 = 1280;
        start( asList(
                labelChanges( nodeId1, NO_LABELS, new long[]{labelId1} ),
                labelChanges( nodeId2, NO_LABELS, new long[]{labelId1, labelId2} )
        ) );

        // WHEN
        BoundedIterable<NodeLabelRange> reader = store.allNodeLabelRanges();
        Iterator<NodeLabelRange> iterator = reader.iterator();
        NodeLabelRange range1 = iterator.next();
        NodeLabelRange range2 = iterator.next();
        assertFalse( iterator.hasNext() );

        // THEN
        assertArrayEquals( new long[]{nodeId1}, reducedNodes( range1 ) );
        assertArrayEquals( new long[]{nodeId2}, reducedNodes( range2 ) );

        assertArrayEquals( new long[]{labelId1}, sorted( range1.labels( nodeId1 ) ) );

        assertArrayEquals( new long[]{labelId1, labelId2}, sorted( range2.labels( nodeId2 ) ) );
    }

    @Test
    public void shouldWorkWithAFullRange()
    {
        // given
        long labelId = 0;
        List<NodeLabelUpdate> updates = new ArrayList<>();
        Set<Long> nodes = new HashSet<>();
        for ( int i = 0; i < 34; i++ )
        {
            updates.add( NodeLabelUpdate.labelChanges( i, new long[]{}, new long[]{labelId} ) );
            nodes.add( (long) i );
        }

        start( updates );

        // when
        LabelScanReader reader = store.newReader();
        Set<Long> nodesWithLabel = PrimitiveLongCollections.toSet( reader.nodesWithLabel( (int) labelId ) );

        // then
        assertEquals( nodes, nodesWithLabel );
    }

    @Test
    public void shouldUpdateAFullRange() throws Exception
    {
        // given
        long label0Id = 0;
        List<NodeLabelUpdate> label0Updates = new ArrayList<>();
        Set<Long> nodes = new HashSet<>();
        for ( int i = 0; i < 34; i++ )
        {
            label0Updates.add( NodeLabelUpdate.labelChanges( i, new long[]{}, new long[]{label0Id} ) );
            nodes.add( (long) i );
        }

        start( label0Updates );

        // when
        write( Collections.emptyIterator() );

        // then
        LabelScanReader reader = store.newReader();
        Set<Long> nodesWithLabel0 = PrimitiveLongCollections.toSet( reader.nodesWithLabel( (int) label0Id ) );
        assertEquals( nodes, nodesWithLabel0 );
    }

    @Test
    public void shouldSeeEntriesWhenOnlyLowestIsPresent()
    {
        // given
        long labelId = 0;
        List<NodeLabelUpdate> labelUpdates = new ArrayList<>();
        labelUpdates.add( NodeLabelUpdate.labelChanges( 0L, new long[]{}, new long[]{labelId} ) );

        start( labelUpdates );

        // when
        MutableInt count = new MutableInt();
        AllEntriesLabelScanReader nodeLabelRanges = store.allNodeLabelRanges();
        nodeLabelRanges.forEach( nlr ->
        {
            for ( long nodeId : nlr.nodes() )
            {
                count.add( nlr.labels( nodeId ).length );
            }
        } );
        assertThat( count.intValue(), is( 1 ) );
    }

    private void write( Iterator<NodeLabelUpdate> iterator ) throws IOException
    {
        try ( LabelScanWriter writer = store.newWriter() )
        {
            while ( iterator.hasNext() )
            {
                writer.write( iterator.next() );
            }
        }
    }

    private long[] sorted( long[] input )
    {
        Arrays.sort( input );
        return input;
    }

    private long[] reducedNodes( NodeLabelRange range )
    {
        long[] nodes = range.nodes();
        long[] result = new long[nodes.length];
        int cursor = 0;
        for ( long node : nodes )
        {
            if ( range.labels( node ).length > 0 )
            {
                result[cursor++] = node;
            }
        }
        return Arrays.copyOf( result, cursor );
    }

    @Test
    public void shouldRebuildFromScratchIfIndexMissing()
    {
        // GIVEN a start of the store with existing data in it
        start( asList(
                labelChanges( 1, NO_LABELS, new long[]{1} ),
                labelChanges( 2, NO_LABELS, new long[]{1, 2} )
        ) );

        // THEN
        assertTrue( "Didn't rebuild the store on startup",
                monitor.noIndexCalled & monitor.rebuildingCalled & monitor.rebuiltCalled );
        assertNodesForLabel( 1, 1, 2 );
        assertNodesForLabel( 2, 2 );
    }

    @Test
    public void rebuildCorruptedIndexIndexOnStartup() throws Exception
    {
        // GIVEN a start of the store with existing data in it
        List<NodeLabelUpdate> data = asList(
                labelChanges( 1, NO_LABELS, new long[]{1} ),
                labelChanges( 2, NO_LABELS, new long[]{1, 2} ) );
        start( data, true, false );

        // WHEN the index is corrupted and then started again
        scrambleIndexFilesAndRestart( data, true, false );

        assertTrue( "Index corruption should be detected", monitor.corruptedIndex );
        assertTrue( "Index should be rebuild", monitor.rebuildingCalled );
    }

    @Test
    public void shouldFindDecentAmountOfNodesForALabel() throws Exception
    {
        // GIVEN
        // 16 is the magic number of the page iterator
        // 32 is the number of nodes in each lucene document
        final int labelId = 1;
        int nodeCount = 32 * 16 + 10;
        start();
        write( new PrefetchingIterator<NodeLabelUpdate>()
        {
            private int i = -1;

            @Override
            protected NodeLabelUpdate fetchNextOrNull()
            {
                return ++i < nodeCount ? labelChanges( i, NO_LABELS, new long[]{labelId} ) : null;
            }
        } );

        // WHEN
        Set<Long> nodeSet = new TreeSet<>();
        LabelScanReader reader = store.newReader();
        PrimitiveLongIterator nodes = reader.nodesWithLabel( labelId );
        while ( nodes.hasNext() )
        {
            nodeSet.add( nodes.next() );
        }
        reader.close();

        // THEN
        assertEquals( "Found gaps in node id range: " + gaps( nodeSet, nodeCount ), nodeCount, nodeSet.size() );
    }

    @Test
    public void shouldFindNodesWithAnyOfGivenLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 3;
        int labelId2 = 5;
        int labelId3 = 13;
        start();

        // WHEN
        write( iterator(
                labelChanges( 2, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2} ),
                labelChanges( 1, EMPTY_LONG_ARRAY, new long[] {labelId1} ),
                labelChanges( 4, EMPTY_LONG_ARRAY, new long[] {labelId1,           labelId3} ),
                labelChanges( 5, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2, labelId3} ),
                labelChanges( 3, EMPTY_LONG_ARRAY, new long[] {labelId1} ),
                labelChanges( 7, EMPTY_LONG_ARRAY, new long[] {          labelId2} ),
                labelChanges( 8, EMPTY_LONG_ARRAY, new long[] {                    labelId3} ),
                labelChanges( 6, EMPTY_LONG_ARRAY, new long[] {          labelId2} ),
                labelChanges( 9, EMPTY_LONG_ARRAY, new long[] {                    labelId3} ) ) );

        // THEN
        try ( LabelScanReader reader = store.newReader() )
        {
            assertArrayEquals(
                    new long[] {1, 2, 3, 4, 5, 6, 7},
                    PrimitiveLongCollections.asArray( reader.nodesWithAnyOfLabels( labelId1, labelId2 ) ) );
            assertArrayEquals(
                    new long[] {1, 2, 3, 4, 5, 8, 9},
                    PrimitiveLongCollections.asArray( reader.nodesWithAnyOfLabels( labelId1, labelId3 ) ) );
            assertArrayEquals(
                    new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9},
                    PrimitiveLongCollections.asArray( reader.nodesWithAnyOfLabels( labelId1, labelId2, labelId3 ) ) );
        }
    }

    @Test
    public void shouldFindNodesWithAllGivenLabels() throws Exception
    {
        // GIVEN
        int labelId1 = 3;
        int labelId2 = 5;
        int labelId3 = 13;
        start();

        // WHEN
        write( iterator(
                labelChanges( 5, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2, labelId3} ),
                labelChanges( 8, EMPTY_LONG_ARRAY, new long[] {                    labelId3} ),
                labelChanges( 3, EMPTY_LONG_ARRAY, new long[] {labelId1} ),
                labelChanges( 6, EMPTY_LONG_ARRAY, new long[] {          labelId2} ),
                labelChanges( 1, EMPTY_LONG_ARRAY, new long[] {labelId1} ),
                labelChanges( 7, EMPTY_LONG_ARRAY, new long[] {          labelId2} ),
                labelChanges( 4, EMPTY_LONG_ARRAY, new long[] {labelId1,           labelId3} ),
                labelChanges( 2, EMPTY_LONG_ARRAY, new long[] {labelId1, labelId2} ),
                labelChanges( 9, EMPTY_LONG_ARRAY, new long[] {                    labelId3} ) ) );

        // THEN
        try ( LabelScanReader reader = store.newReader() )
        {
            assertArrayEquals(
                    new long[] {2, 5},
                    PrimitiveLongCollections.asArray( reader.nodesWithAllLabels( labelId1, labelId2 ) ) );
            assertArrayEquals(
                    new long[] {4, 5},
                    PrimitiveLongCollections.asArray( reader.nodesWithAllLabels( labelId1, labelId3 ) ) );
            assertArrayEquals(
                    new long[] {5},
                    PrimitiveLongCollections.asArray( reader.nodesWithAllLabels( labelId1, labelId2, labelId3 ) ) );
        }
    }

    private void prepareIndex() throws IOException
    {
        start();
        try ( LabelScanWriter labelScanWriter = store.newWriter() )
        {
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[]{}, new long[]{1} ) );
        }
        store.shutdown();
    }

    private Set<Long> gaps( Set<Long> ids, int expectedCount )
    {
        Set<Long> gaps = new HashSet<>();
        for ( long i = 0; i < expectedCount; i++ )
        {
            if ( !ids.contains( i ) )
            {
                gaps.add( i );
            }
        }
        return gaps;
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

    private void createAndStartReadOnly()
    {
        // create label scan store and shutdown it
        start();
        life.shutdown();

        start( false, true );
    }

    private void start()
    {
        start( false, false );
    }

    private void start( boolean usePersistentStore, boolean readOnly )
    {
        start( Collections.emptyList(), usePersistentStore, readOnly );
    }

    private void start( List<NodeLabelUpdate> existingData )
    {
        start( existingData, false, false );
    }

    private void start( List<NodeLabelUpdate> existingData, boolean usePersistentStore,
            boolean readOnly )
    {
        life = new LifeSupport();
        monitor = new TrackingMonitor();

        store = createLabelScanStore( fileSystemRule.get(), dir, asStream( existingData ), usePersistentStore, readOnly,
                monitor );
        life.add( store );

        life.start();
        assertTrue( monitor.initCalled );
    }

    private void scrambleIndexFilesAndRestart( List<NodeLabelUpdate> data,
            boolean usePersistentStore, boolean readOnly ) throws IOException
    {
        shutdown();
        corruptIndex( fileSystemRule.get(), dir );
        start( data, usePersistentStore, readOnly );
    }

    protected abstract void corruptIndex( FileSystemAbstraction fileSystem, File rootFolder ) throws IOException;

    protected void scrambleFile( File file ) throws IOException
    {
        scrambleFile( this.random.random(), file );
    }

    public static void scrambleFile( Random random, File file ) throws IOException
    {
        try ( RandomAccessFile fileAccess = new RandomAccessFile( file, "rw" );
              FileChannel channel = fileAccess.getChannel() )
        {
            // The files will be small, so OK to allocate a buffer for the full size
            byte[] bytes = new byte[(int) channel.size()];
            putRandomBytes( random, bytes );
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            channel.position( 0 );
            channel.write( buffer );
        }
    }

    private static void putRandomBytes( Random random, byte[] bytes )
    {
        for ( int i = 0; i < bytes.length; i++ )
        {
            bytes[i] = (byte) random.nextInt();
        }
    }

    public static class TrackingMonitor extends LabelScanStore.Monitor.Adaptor
    {
        boolean initCalled;
        public boolean rebuildingCalled;
        public boolean rebuiltCalled;
        public boolean noIndexCalled;
        public boolean corruptedIndex;

        @Override
        public void noIndex()
        {
            noIndexCalled = true;
        }

        @Override
        public void notValidIndex()
        {
            corruptedIndex = true;
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

        public void reset()
        {
            initCalled = false;
            rebuildingCalled = false;
            rebuiltCalled = false;
            noIndexCalled = false;
            corruptedIndex = false;
        }
    }
}
