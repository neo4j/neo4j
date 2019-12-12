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
package org.neo4j.internal.index.label;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.iterator.LongIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.LongStream;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.index.label.FullStoreChangeStream.EMPTY;
import static org.neo4j.internal.index.label.FullStoreChangeStream.asStream;
import static org.neo4j.storageengine.api.NodeLabelUpdate.labelChanges;

@PageCacheExtension
@Neo4jLayoutExtension
@ExtendWith( RandomExtension.class )
public class NativeLabelScanStoreTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;
    @Inject
    private DatabaseLayout databaseLayout;

    private static final long[] NO_LABELS = new long[0];

    private LifeSupport life;
    private TrackingMonitor monitor;
    private LabelScanStore store;

    @AfterEach
    void shutdown()
    {
        if ( life != null )
        {
            life.shutdown();
        }
    }

    @Test
    void shutdownNonInitialisedNativeScanStoreWithoutException() throws IOException
    {
        String expectedMessage = "Expected exception message";
        Monitors monitors = mock( Monitors.class );
        when( monitors.newMonitor( LabelScanStore.Monitor.class ) ).thenReturn( LabelScanStore.Monitor.EMPTY );
        doThrow( new RuntimeException( expectedMessage ) ).when( monitors ).addMonitorListener( any() );

        LabelScanStore scanStore = getLabelScanStore( fileSystem, databaseLayout, EMPTY, true, monitors );
        RuntimeException exception = assertThrows( RuntimeException.class, scanStore::init );
        assertEquals( expectedMessage, exception.getMessage() );
        scanStore.shutdown();
    }

    @Test
    void shouldStartPopulationAgainIfNotCompletedFirstTime()
    {
        // given
        // label scan store init but no start
        LifeSupport life = new LifeSupport();
        TrackingMonitor monitor = new TrackingMonitor();
        life.add( createLabelScanStore( fileSystem, databaseLayout, EMPTY, false, monitor ) );
        life.init();
        assertTrue( monitor.noIndexCalled );
        monitor.reset();
        life.shutdown();

        // when
        // starting label scan store again
        life = new LifeSupport();
        life.add( createLabelScanStore( fileSystem, databaseLayout, EMPTY, false, monitor ) );
        life.init();

        // then
        // label scan store should recognize it still needs to be rebuilt
        assertTrue( monitor.corruptedIndex );
        life.start();
        assertTrue( monitor.rebuildingCalled );
        assertTrue( monitor.rebuiltCalled );
        life.shutdown();
    }

    @Test
    void shouldRestartPopulationIfIndexFileWasNeverFullyInitialized() throws IOException
    {
        // given
        File labelScanStoreFile = NativeLabelScanStore.getLabelScanStoreFile( databaseLayout );
        fileSystem.write( labelScanStoreFile ).close();
        TrackingMonitor monitor = new TrackingMonitor();
        LifeSupport life = new LifeSupport();

        // when
        life.add( createLabelScanStore( fileSystem, databaseLayout, EMPTY, false, monitor ) );
        life.start();

        // then
        assertTrue( monitor.corruptedIndex );
        assertTrue( monitor.rebuildingCalled );
        life.shutdown();
    }

    @Test
    void failToRetrieveWriterOnReadOnlyScanStore()
    {
        createAndStartReadOnly();
        assertThrows( UnsupportedOperationException.class, () -> store.newWriter() );
    }

    @Test
    void forceShouldNotForceWriterOnReadOnlyScanStore() throws IOException
    {
        createAndStartReadOnly();
        store.force( IOLimiter.UNLIMITED );
    }

    @Test
    void shouldNotStartIfLabelScanStoreIndexDoesNotExistInReadOnlyMode()
    {
        // WHEN
        final Exception exception = assertThrows( Exception.class, () -> start( true ) );
        assertTrue( Exceptions.contains( exception, t -> t instanceof NoSuchFileException ) );
        assertTrue( Exceptions.contains( exception, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( exception, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void snapshotReadOnlyLabelScanStore() throws IOException
    {
        prepareIndex();
        createAndStartReadOnly();
        try ( ResourceIterator<File> indexFiles = store.snapshotStoreFiles() )
        {
            List<File> files = Iterators.asList( indexFiles );
            assertThat( files ).as( "Should have at least index segment file." ).contains( databaseLayout.labelScanStore() );
        }
    }

    @Test
    void shouldUpdateIndexOnLabelChange() throws Exception
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
    void shouldUpdateIndexOnAddedLabels() throws Exception
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
    void shouldUpdateIndexOnRemovedLabels() throws Exception
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
    void shouldDeleteFromIndexWhenDeletedNode() throws Exception
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
    void shouldScanSingleRange()
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
    void shouldScanMultipleRanges()
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
    void shouldWorkWithAFullRange()
    {
        // given
        long labelId = 0;
        List<NodeLabelUpdate> updates = new ArrayList<>();
        Set<Long> nodes = new HashSet<>();
        for ( int i = 0; i < 34; i++ )
        {
            updates.add( labelChanges( i, new long[]{}, new long[]{labelId} ) );
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
    void shouldUpdateAFullRange() throws Exception
    {
        // given
        long label0Id = 0;
        List<NodeLabelUpdate> label0Updates = new ArrayList<>();
        Set<Long> nodes = new HashSet<>();
        for ( int i = 0; i < 34; i++ )
        {
            label0Updates.add( labelChanges( i, new long[]{}, new long[]{label0Id} ) );
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
    void shouldSeeEntriesWhenOnlyLowestIsPresent()
    {
        // given
        long labelId = 0;
        List<NodeLabelUpdate> labelUpdates = new ArrayList<>();
        labelUpdates.add( labelChanges( 0L, new long[]{}, new long[]{labelId} ) );

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
        assertThat( count.intValue() ).isEqualTo( 1 );
    }

    @Test
    void shouldRebuildFromScratchIfIndexMissing()
    {
        // GIVEN a start of the store with existing data in it
        start( asList(
                labelChanges( 1, NO_LABELS, new long[]{1} ),
                labelChanges( 2, NO_LABELS, new long[]{1, 2} )
        ) );

        // THEN
        assertTrue( monitor.noIndexCalled & monitor.rebuildingCalled & monitor.rebuiltCalled, "Didn't rebuild the store on startup" );
        assertNodesForLabel( 1, 1, 2 );
        assertNodesForLabel( 2, 2 );
    }

    @Test
    void rebuildCorruptedIndexIndexOnStartup() throws Exception
    {
        // GIVEN a start of the store with existing data in it
        List<NodeLabelUpdate> data = asList(
                labelChanges( 1, NO_LABELS, new long[]{1} ),
                labelChanges( 2, NO_LABELS, new long[]{1, 2} ) );
        start( data, false );

        // WHEN the index is corrupted and then started again
        scrambleIndexFilesAndRestart( data );

        assertTrue( monitor.corruptedIndex, "Index corruption should be detected" );
        assertTrue( monitor.rebuildingCalled, "Index should be rebuild" );
    }

    @Test
    void shouldFindDecentAmountOfNodesForALabel() throws Exception
    {
        // GIVEN
        // 16 is the magic number of the page iterator
        // 32 is the number of nodes in each lucene document
        final int labelId = 1;
        int nodeCount = 32 * 16 + 10;
        start();
        write( new PrefetchingIterator<>()
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
        PrimitiveLongResourceIterator nodes = reader.nodesWithLabel( labelId );
        while ( nodes.hasNext() )
        {
            nodeSet.add( nodes.next() );
        }
        nodes.close();

        // THEN
        assertEquals( nodeCount, nodeSet.size(), "Found gaps in node id range: " + gaps( nodeSet, nodeCount ) );
    }

    @Test
    void shouldFindNodesWithAnyOfGivenLabels() throws Exception
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
        LabelScanReader reader = store.newReader();
        assertArrayEquals(
                new long[]{1, 2, 3, 4, 5, 6, 7},
                closingAsArray( reader.nodesWithAnyOfLabels( new int[]{labelId1, labelId2} ) ) );
        assertArrayEquals(
                new long[]{1, 2, 3, 4, 5, 8, 9},
                closingAsArray( reader.nodesWithAnyOfLabels( new int[]{labelId1, labelId3} ) ) );
        assertArrayEquals(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                closingAsArray( reader.nodesWithAnyOfLabels( new int[]{labelId1, labelId2, labelId3} ) ) );
    }

    @Test
    void shouldWriteDataUsingBatchWriter() throws IOException
    {
        // given
        start();
        List<NodeLabelUpdate> updates = new ArrayList<>();
        Long[] possibleLabelIds = new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};
        for ( long nodeId = 0; nodeId < 10_000; nodeId++ )
        {
            long[] longLabels = Arrays.stream( random.selection( possibleLabelIds, 0, 5, false ) ).mapToLong( Long::longValue ).sorted().toArray();
            updates.add( labelChanges( nodeId, EMPTY_LONG_ARRAY, longLabels ) );
        }

        // when
        try ( LabelScanWriter writer = store.newBulkAppendWriter() )
        {
            for ( NodeLabelUpdate update : updates )
            {
                writer.write( update );
            }
        }

        // then
        for ( Long labelId : possibleLabelIds )
        {
            PrimitiveLongResourceIterator nodesWithLabel = store.newReader().nodesWithLabel( labelId.intValue() );
            Iterator<NodeLabelUpdate> expected =
                    updates.stream().filter( update -> LongStream.of( update.getLabelsAfter() ).anyMatch( candidateId -> candidateId == labelId ) ).iterator();
            while ( nodesWithLabel.hasNext() )
            {
                long node = nodesWithLabel.next();
                assertEquals( expected.next().getNodeId(), node );
            }
            assertFalse( expected.hasNext() );
        }
    }

    private LabelScanStore createLabelScanStore( FileSystemAbstraction fileSystemAbstraction, DatabaseLayout databaseLayout,
                                                 FullStoreChangeStream fullStoreChangeStream, boolean readOnly,
                                                 LabelScanStore.Monitor monitor )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        return getLabelScanStore( fileSystemAbstraction, databaseLayout, fullStoreChangeStream, readOnly, monitors );
    }

    private LabelScanStore getLabelScanStore( FileSystemAbstraction fileSystemAbstraction, DatabaseLayout databaseLayout,
            FullStoreChangeStream fullStoreChangeStream, boolean readOnly, Monitors monitors )
    {
        return new NativeLabelScanStore( pageCache, databaseLayout, fileSystemAbstraction,
                fullStoreChangeStream, readOnly, monitors, RecoveryCleanupWorkCollector.immediate() );
    }

    private void corruptIndex( DatabaseLayout databaseLayout ) throws IOException
    {
        File lssFile = databaseLayout.labelScanStore();
        scrambleFile( lssFile );
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

    private static long[] sorted( long[] input )
    {
        Arrays.sort( input );
        return input;
    }

    private static long[] reducedNodes( NodeLabelRange range )
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

    private void prepareIndex() throws IOException
    {
        start();
        try ( LabelScanWriter labelScanWriter = store.newWriter() )
        {
            labelScanWriter.write( labelChanges( 1, new long[]{}, new long[]{1} ) );
        }
        store.shutdown();
    }

    private static Set<Long> gaps( Set<Long> ids, int expectedCount )
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
        LongIterator nodes = store.newReader().nodesWithLabel( labelId );
        while ( nodes.hasNext() )
        {
            nodeSet.add( nodes.next() );
        }

        for ( long expectedNodeId : expectedNodeIds )
        {
            assertTrue( nodeSet.remove( expectedNodeId ), "Expected node " + expectedNodeId + " not found in scan store" );
        }
        assertTrue( nodeSet.isEmpty(), "Unexpected nodes in scan store " + nodeSet );
    }

    private void createAndStartReadOnly()
    {
        // create label scan store and shutdown it
        start();
        life.shutdown();

        start( true );
    }

    private void start()
    {
        start( false );
    }

    private void start( boolean readOnly )
    {
        start( Collections.emptyList(), readOnly );
    }

    private void start( List<NodeLabelUpdate> existingData )
    {
        start( existingData, false );
    }

    private void start( List<NodeLabelUpdate> existingData,
                        boolean readOnly )
    {
        life = new LifeSupport();
        monitor = new TrackingMonitor();

        store = createLabelScanStore( fileSystem, databaseLayout, asStream( existingData ), readOnly,
                                      monitor );
        life.add( store );

        life.start();
        assertTrue( monitor.initCalled );
    }

    private void scrambleIndexFilesAndRestart( List<NodeLabelUpdate> data ) throws IOException
    {
        shutdown();
        corruptIndex( databaseLayout );
        start( data, false );
    }

    void scrambleFile( File file ) throws IOException
    {
        scrambleFile( random.random(), file );
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
        boolean rebuildingCalled;
        boolean rebuiltCalled;
        boolean noIndexCalled;
        boolean corruptedIndex;

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
