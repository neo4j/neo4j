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
import static java.util.Comparator.comparingLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        int blockSize = 100;
        MutableLong key = new MutableLong( 10 );
        MutableLong value = new MutableLong( 20 );
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, blockSize ) )
        {
            // when
            storage.add( key, value );
            storage.doneAdding();

            // then
            assertEquals( 1, monitor.blockFlushedCallCount );
            assertEquals( 1, monitor.lastKeyCount );
            assertEquals( BlockStorage.BLOCK_HEADER_SIZE + monitor.totalEntrySize, monitor.lastNumberOfBytes );
            assertEquals( blockSize, monitor.lastPositionAfterFlush );
            assertThat( monitor.lastNumberOfBytes, lessThan( blockSize ) );
            assertContents( layout, storage, singletonList( Pair.of( key, value ) ) );
        }
    }

    @Test
    void shouldSortAndAddMultipleEntriesInLastBlock() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        TrackingMonitor monitor = new TrackingMonitor();
        int blockSize = 1_000;
        List<Pair<MutableLong,MutableLong>> expected = new ArrayList<>();
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, blockSize ) )
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
            sortExpectedBlock( expected );
            assertContents( layout, storage, expected );
        }
    }

    @Test
    void shouldSortAndAddMultipleEntriesInMultipleBlocks() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( random.nextInt( 900 ) );
        TrackingMonitor monitor = new TrackingMonitor();
        int blockSize = 1_000;
        List<List<Pair<MutableLong,MutableLong>>> expected = new ArrayList<>();
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, blockSize ) )
        {
            // when
            List<Pair<MutableLong,MutableLong>> currentExpected = new ArrayList<>();
            long currentBlock = 0;
            for ( long i = 0; monitor.blockFlushedCallCount < 3; i++ )
            {
                long keyNumber = random.nextLong( 10_000_000 );
                MutableLong key = new MutableLong( keyNumber );
                MutableLong value = new MutableLong( i );

                storage.add( key, value );
                if ( monitor.blockFlushedCallCount > currentBlock )
                {
                    sortExpectedBlock( currentExpected );
                    expected.add( currentExpected );
                    currentExpected = new ArrayList<>();
                    currentBlock = monitor.blockFlushedCallCount;
                }
                currentExpected.add( Pair.of( key, value ) );
            }
            storage.doneAdding();
            sortExpectedBlock( currentExpected );
            expected.add( currentExpected );

            // then
            assertContents( layout, storage, expected.toArray( new List[0] ) );
        }
    }

    @Test
    void shouldNotAcceptAddedEntriesAfterDoneAdding() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, NO_MONITOR, 100 ) )
        {
            // when
            storage.doneAdding();

            // then
            assertThrows( IllegalStateException.class, () -> storage.add( new MutableLong( 0 ), new MutableLong( 1 ) ) );
        }
    }

    @Test
    void shouldNotFlushAnythingOnEmptyBufferInDoneAdding() throws IOException
    {
        // given
        SimpleLongLayout layout = layout( 0 );
        TrackingMonitor monitor = new TrackingMonitor();
        try ( BlockStorage<MutableLong,MutableLong> storage = new BlockStorage<>( layout, BUFFER_FACTORY, fileSystem, file, monitor, 100 ) )
        {
            // when
            storage.doneAdding();

            // then
            assertEquals( 0, monitor.blockFlushedCallCount );
        }
    }

    private void sortExpectedBlock( List<Pair<MutableLong,MutableLong>> currentExpected )
    {
        currentExpected.sort( comparingLong( p -> p.getKey().longValue() ) );
    }

    private void assertContents( SimpleLongLayout layout, BlockStorage<MutableLong,MutableLong> storage, List<Pair<MutableLong,MutableLong>>... expectedBlocks )
            throws IOException
    {
        try ( BlockStorageReader<MutableLong,MutableLong> reader = storage.reader() )
        {
            for ( List<Pair<MutableLong,MutableLong>> expectedBlock : expectedBlocks )
            {
                try ( BlockReader<MutableLong,MutableLong> block = reader.nextBlock() )
                {
                    assertNotNull( block );
                    assertEquals( expectedBlock.size(), block.entryCount() );
                    for ( Pair<MutableLong,MutableLong> expectedEntry : expectedBlock )
                    {
                        assertTrue( block.next() );
                        assertEquals( 0, layout.compare( expectedEntry.getKey(), block.key() ) );
                        assertEquals( expectedEntry.getValue(), block.value() );
                    }
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
        private int lastNumberOfBytes;
        private long lastPositionAfterFlush;

        @Override
        public void entryAdded( int entrySize )
        {
            entryAddedCallCount++;
            lastEntrySize = entrySize;
            totalEntrySize += entrySize;
        }

        @Override
        public void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush )
        {
            blockFlushedCallCount++;
            lastKeyCount = keyCount;
            lastNumberOfBytes = numberOfBytes;
            lastPositionAfterFlush = positionAfterFlush;
        }
    }
}
