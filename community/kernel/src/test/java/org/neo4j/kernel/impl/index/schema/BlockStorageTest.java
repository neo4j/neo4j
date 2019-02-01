package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.Math.toIntExact;
import static java.nio.ByteBuffer.allocate;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.BlockStorage.Monitor.NO_MONITOR;

@ExtendWith( {TestDirectoryExtension.class, RandomExtension.class} )
class BlockStorageTest
{
    private static final ByteBufferFactory BUFFER_FACTORY = bufferSize -> allocate( toIntExact( bufferSize ) );

    @Inject
    TestDirectory directory;
    @Inject
    RandomRule random;

    private File file;
    private FileSystemAbstraction fileSystem;

    @BeforeEach
    void setup()
    {
        file = directory.file( "block" );
        fileSystem = directory.getFileSystem();
    }

    @Test
    void shouldCreateAndCloseTheBlockFile() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        assertFalse( fileSystem.fileExists( file ) );
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, NO_MONITOR, 100 ) )
        {
            // then
            assertTrue( fileSystem.fileExists( file ) );
        }
    }

    @Test
    void shouldAddSingleEntryInLastBlock() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        TrackingMonitor monitor = new TrackingMonitor();
        int bufferSize = 100;
        MutableLong key = new MutableLong( 10 );
        MutableLong value = new MutableLong( 20 );
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, bufferSize ) )
        {
            // when
            storage.add( key, value );
            storage.doneAdding();

            // then
            assertEquals( 1, monitor.blockFlushedCallCount );
            assertEquals( 1, monitor.lastKeyCount );
            assertEquals( BlockStorage.BLOCK_HEADER_SIZE + monitor.totalEntrySize, monitor.lastBufferSize );
            assertEquals( bufferSize, monitor.lastPositionAfterFlush );
            assertContents( layout, storage, singletonList( Pair.of( key, value ) ) );
        }
    }

    @Test
    void shouldSortAndAddMultipleEntriesInLastBlock() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        TrackingMonitor monitor = new TrackingMonitor();
        int bufferSize = 1_000;
        List<Pair<MutableLong,MutableLong>> expected = new ArrayList<>();
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, bufferSize ) )
        {
            // when
            for ( int i = 0; i < 10; i++ )
            {
                long keyNumber = random.nextLong( 10_000_000 );
                MutableLong key = new MutableLong( keyNumber );
                MutableLong value = new MutableLong( i );
                storage.add( key, value );
                expected.add( Pair.of( key, value ) );
            }
            storage.doneAdding();

            // then

        }
    }

    // TODO shouldNotAcceptAddedEntriesAfterDoneAdding
    // TODO shouldNotFlushAnythingOnEmptyBufferInDoneAdding
    // TODO shouldFlushMultipleBlocks
    // TODO shouldFlushSortedEntriesOnFullBuffer
    // TODO shouldFlushPaddedBlocks

    private void assertContents( SimpleLongLayout layout, BlockStorage<MutableLong,MutableLong> storage, List<Pair<MutableLong,MutableLong>>... expectedBlocks )
            throws IOException
    {
        try ( BlockStorage<MutableLong,MutableLong>.BlockReader reader = storage.reader() )
        {
            for ( List<Pair<MutableLong,MutableLong>> expectedBlock : expectedBlocks )
            {
                BlockStorage<MutableLong,MutableLong>.EntryReader block = reader.nextBlock();
                assertNotNull( block );
                assertEquals( expectedBlock.size(), block.entryCount() );
                for ( Pair<MutableLong,MutableLong> expectedEntry : expectedBlock )
                {
                    assertTrue( block.next() );
                    assertEquals( 0, layout.compare( block.key(), expectedEntry.getKey() ) );
                    assertEquals( block.value(), expectedEntry.getValue() );
                }
            }
        }
    }

    private SimpleLongLayout layout( int keyPadding )
    {
        return SimpleLongLayout.longLayout().withKeyPadding( keyPadding ).build();
    }

    private static class TrackingMonitor implements BlockStorage.Monitor
    {
        // For entryAdded
        private int entryAddedCallCount;
        private int lastEntrySize;
        private long totalEntrySize;

        // For blockFlushed
        private int blockFlushedCallCount;
        private long lastKeyCount;
        private int lastBufferSize;
        private long lastPositionAfterFlush;

        @Override
        public void entryAdded( int entrySize )
        {
            entryAddedCallCount++;
            lastEntrySize = entrySize;
            totalEntrySize += entrySize;
        }

        @Override
        public void blockFlushed( long keyCount, int bufferSize, long positionAfterFlush )
        {
            blockFlushedCallCount++;
            lastKeyCount = keyCount;
            lastBufferSize = bufferSize;
            lastPositionAfterFlush = positionAfterFlush;
        }
    }
}
