/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Function;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.direct.NodeLabelRange;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.tracking;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;

@RunWith(Parameterized.class)
public class LuceneLabelScanStoreTest
{
    private static final long[] NO_LABELS = new long[0];
    private LuceneLabelScanStore labelScanStore;

    @Test
    public void failToDeleteDocumentsOnReadOnlyScanStore() throws Exception
    {
        startReadOnlyLabelScanStore();
        expectedException.expect( UnsupportedOperationException.class );
        labelScanStore.deleteDocuments( new Term( "key", "value" ) );
    }

    @Test
    public void failToUpdateDocumentsOnReadOnlyScanStore() throws IOException, IndexCapacityExceededException
    {
        startReadOnlyLabelScanStore();
        expectedException.expect( UnsupportedOperationException.class );
        labelScanStore.updateDocument( new Term( "key", "value" ), new Document() );
    }

    @Test
    public void forceShouldNotForceWriterOnReadOnlyScanStore()
    {
        startReadOnlyLabelScanStore();
        labelScanStore.force();
    }

    @Test
    public void failToGetWriterOnReadOnlyScanStore() throws Exception
    {
        startReadOnlyLabelScanStore();
        expectedException.expect( UnsupportedOperationException.class );
        labelScanStore.newWriter();
    }

    @Test
    public void failToStartIfLabelScanStoreIndexDoesNotExistInReadOnlyMode()
    {
        expectedException.expectCause( Matchers.<Throwable>instanceOf( IOException.class ) );
        startLabelScanStore( MapUtil.stringMap(GraphDatabaseSettings.read_only.name(), "true") );
        assertTrue( monitor.noIndexCalled );
    }

    @Test
    public void snapshotReadOnlyLabelScanStore() throws IOException
    {
        startReadOnlyLabelScanStore();
        try (ResourceIterator<File> indexFiles = labelScanStore.snapshotStoreFiles())
        {
            List<String> filesNames = Iterables.toList( Iterables.map( new Function<File,String>()
            {
                @Override
                public String apply( File file )
                {
                    return file.getName();
                }
            }, indexFiles ) );

            assertThat( "Should have at least index segment file.", filesNames, contains( startsWith( IndexFileNames.SEGMENTS ) ) );
        }
    }

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
        int labelId1 = 1, labelId2 = 2;
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
        int labelId1 = 1, labelId2 = 2;
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
    public void shouldScanSingleRange() throws Exception
    {
        // GIVEN
        int labelId1 = 1, labelId2 = 2;
        long nodeId1 = 10, nodeId2 = 11;
        start( asList(
                labelChanges( nodeId1, NO_LABELS, new long[] { labelId1 } ),
                labelChanges( nodeId2, NO_LABELS, new long[] { labelId1, labelId2 } )
        ) );

        // WHEN
        AllEntriesLabelScanReader reader = store.newAllEntriesReader();
        NodeLabelRange range = single( reader.iterator() );

        // THEN
        assertArrayEquals( new long[]{nodeId1, nodeId2}, sorted( range.nodes() ) );

        assertArrayEquals( new long[]{labelId1}, sorted( range.labels( nodeId1 ) ) );
        assertArrayEquals( new long[]{labelId1, labelId2}, sorted( range.labels( nodeId2 ) ) );
    }

    @Test
    public void shouldScanMultipleRanges() throws Exception
    {
        // GIVEN
        int labelId1 = 1, labelId2 = 2;
        long nodeId1 = 10, nodeId2 = 1280;
        start( asList(
                labelChanges( nodeId1, NO_LABELS, new long[] { labelId1 } ),
                labelChanges( nodeId2, NO_LABELS, new long[] { labelId1, labelId2 } )
        ) );

        // WHEN
        AllEntriesLabelScanReader reader = store.newAllEntriesReader();
        Iterator<NodeLabelRange> iterator = reader.iterator();
        NodeLabelRange range1 = iterator.next();
        NodeLabelRange range2 = iterator.next();
        assertFalse( iterator.hasNext() );

        // THEN
        assertArrayEquals( new long[] { nodeId1 }, sorted( range1.nodes() ) );
        assertArrayEquals( new long[] { nodeId2 }, sorted( range2.nodes() ) );

        assertArrayEquals( new long[] { labelId1 }, sorted( range1.labels( nodeId1 ) ) );

        assertArrayEquals( new long[] { labelId1, labelId2 }, sorted( range2.labels( nodeId2 ) ) );
    }

    @Test
    public void shouldWorkWithAFullRange() throws Exception
    {
        // given
        long labelId = 0;
        List<NodeLabelUpdate> updates = new ArrayList<>(  );
        for ( int i = 0; i < 34; i++)
        {
            updates.add( NodeLabelUpdate.labelChanges(i, new long[] {}, new long[]{labelId}));
        }

        start(updates);

        // when
        LabelScanReader reader = store.newReader();
        Set<Long> nodesWithLabel = asSet( reader.nodesWithLabel( (int) labelId ) );

        // then
        for ( long i = 0; i < 34; i++ )
        {
            assertThat( nodesWithLabel, hasItem( i ) );
            Set<Long> labels = asSet( reader.labelsForNode( i ) );
            assertThat( labels, hasItem( labelId ) );
        }
    }

    @Test
    public void shouldUpdateAFullRange() throws Exception
    {
        // given
        long label0Id = 0;
        List<NodeLabelUpdate> label0Updates = new ArrayList<>(  );
        for ( int i = 0; i < 34; i++)
        {
            label0Updates.add( NodeLabelUpdate.labelChanges( i, new long[]{}, new long[]{label0Id} ) );
        }

        start(label0Updates);

        // when
        write( Collections.<NodeLabelUpdate>emptyIterator() );

        // then
        LabelScanReader reader = store.newReader();
        Set<Long> nodesWithLabel0 = asSet( reader.nodesWithLabel( (int) label0Id ) );
        for ( long i = 0; i < 34; i++ )
        {
            assertThat( nodesWithLabel0, hasItem( i ) );
            Set<Long> labels = asSet( reader.labelsForNode( i ) );
            assertThat( labels, hasItem( label0Id ) );
        }
    }
    private void write( Iterator<NodeLabelUpdate> iterator ) throws IOException, IndexCapacityExceededException
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
                    "Label scan store could not be read, and needs to be rebuilt. To trigger a rebuild, ensure the " +
                            "database is stopped, delete the files in '"+dir.getAbsolutePath()+"', and then start the " +
                            "database again." ));
        }
    }

    @Test
    public void shouldFindDecentAmountOfNodesForALabel() throws Exception
    {
        // GIVEN
        // 16 is the magic number of the page iterator
        // 32 is the number of nodes in each lucene document
        final int labelId = 1, nodeCount = 32*16 + 10;
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
    public void shouldFindAllLabelsForGivenNode() throws Exception
    {
        // GIVEN
        // 16 is the magic number of the page iterator
        // 32 is the number of nodes in each lucene document
        final long labelId1 = 1, labelId2 = 2, labelId3 = 87;
        start();

        int nodeId = 42;
        write( IteratorUtil.iterator( labelChanges( nodeId, NO_LABELS, new long[]{labelId1, labelId2} ) ) );
        write( IteratorUtil.iterator( labelChanges( 41, NO_LABELS, new long[]{labelId3, labelId2} ) ) );

        // WHEN
        LabelScanReader reader = store.newReader();

        // THEN
        assertThat( asSet( reader.labelsForNode( nodeId ) ), hasItems(labelId1, labelId2) );
        reader.close();
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

    private final LabelScanStorageStrategy strategy;

    public LuceneLabelScanStoreTest( LabelScanStorageStrategy strategy )
    {
        this.strategy = strategy;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameterizedWithStrategies()
    {
        return asList(
                new Object[]{
                        new NodeRangeDocumentLabelScanStorageStrategy(
                                BitmapDocumentFormat._32 )},
                new Object[]{
                        new NodeRangeDocumentLabelScanStorageStrategy(
                                BitmapDocumentFormat._64 )}
        );
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

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Random random = new Random();
    private DirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private LifeSupport life;
    private TrackingMonitor monitor;
    private LuceneLabelScanStore store;
    private File dir;

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
        startLabelScanStore( existingData, MapUtil.stringMap() );
        assertTrue( monitor.initCalled );
    }

    private void startReadOnlyLabelScanStore()
    {
        // create label scan store and shutdown it
        startLabelScanStore( MapUtil.stringMap() );
        life.shutdown();

        startLabelScanStore( MapUtil.stringMap(GraphDatabaseSettings.read_only.name(), "true") );
    }

    private void startLabelScanStore( Map<String,String> configParams )
    {
        startLabelScanStore( Collections.<NodeLabelUpdate>emptyList(), configParams );
    }

    private void startLabelScanStore( List<NodeLabelUpdate> existingData, Map<String,String> configParams )
    {
        life = new LifeSupport();
        monitor = new TrackingMonitor();
        labelScanStore = new LuceneLabelScanStore( strategy,
                directoryFactory, dir, new DefaultFileSystemAbstraction(), tracking(), asStream( existingData ),
                new Config( configParams ), OperationalMode.single, monitor );
        store = life.add( labelScanStore );
        life.start();
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
        dir = testDirectory.directory( "lucene" );
        if ( dir.exists() )
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
