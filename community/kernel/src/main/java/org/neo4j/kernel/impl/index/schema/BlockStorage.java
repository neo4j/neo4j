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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.util.Preconditions;

/**
 * Transforms an unordered stream of key-value pairs ({@link BlockEntry}) to an ordered one. It does so in two phases:
 * 1. ADD: Entries are added through {@link #add(KEY, VALUE)}. Those entries are buffered in memory until there are enough of them to fill up a Block.
 * At that point they are sorted and flushed out to a file. This file will eventually contain multiple Blocks that each contain many entries in sorted order.
 * When there are no more entries to add {@link #doneAdding()} is called and the last Block is flushed to file.
 * 2. MERGE: By calling {@link #merge(int, Cancellation)} (after {@link #doneAdding()} has been called) the multiple Blocks are merge joined into a new file
 * resulting in larger blocks of sorted entries. Those larger blocks are then merge joined back to the original file. Merging continues in this ping pong
 * fashion until there is only a single large block in the resulting file. The entries are now ready to be read in sorted order, call {@link #reader()}.
 */
class BlockStorage<KEY, VALUE> implements Closeable
{
    static final int BLOCK_HEADER_SIZE = Long.BYTES  // blockSize
                                       + Long.BYTES; // entryCount

    private final Layout<KEY,VALUE> layout;
    private final FileSystemAbstraction fs;
    private final MutableList<BlockEntry<KEY,VALUE>> bufferedEntries;
    private final ByteBuffer byteBuffer;
    private final Comparator<BlockEntry<KEY,VALUE>> comparator;
    private final StoreChannel storeChannel;
    private final Monitor monitor;
    private final int blockSize;
    private final ByteBufferFactory bufferFactory;
    private final File blockFile;
    private int numberOfBlocksInCurrentFile;
    private int currentBufferSize;
    private boolean doneAdding;

    BlockStorage( Layout<KEY,VALUE> layout, ByteBufferFactory bufferFactory, FileSystemAbstraction fs, File blockFile, Monitor monitor, int blockSize )
            throws IOException
    {
        this.layout = layout;
        this.fs = fs;
        this.blockFile = blockFile;
        this.monitor = monitor;
        this.blockSize = blockSize;
        this.bufferedEntries = Lists.mutable.empty();
        this.bufferFactory = bufferFactory;
        this.byteBuffer = bufferFactory.newBuffer( blockSize );
        this.comparator = ( e0, e1 ) -> layout.compare( e0.key(), e1.key() );
        this.storeChannel = fs.create( blockFile );
        resetBufferedEntries();
    }

    public void add( KEY key, VALUE value ) throws IOException
    {
        Preconditions.checkState( !doneAdding, "Cannot add more after done adding" );

        int entrySize = BlockEntry.entrySize( layout, key, value );

        if ( currentBufferSize + entrySize > blockSize )
        {
            // append buffer to file and clear buffers
            flushAndResetBuffer();
            numberOfBlocksInCurrentFile++;
        }

        bufferedEntries.add( new BlockEntry<>( key, value ) );
        currentBufferSize += entrySize;
        monitor.entryAdded( entrySize );
    }

    void doneAdding() throws IOException
    {
        if ( !bufferedEntries.isEmpty() )
        {
            flushAndResetBuffer();
            numberOfBlocksInCurrentFile++;
        }
        doneAdding = true;
        storeChannel.close();
    }

    private void resetBufferedEntries()
    {
        bufferedEntries.clear();
        currentBufferSize = BLOCK_HEADER_SIZE;
    }

    private void flushAndResetBuffer() throws IOException
    {
        bufferedEntries.sortThis( comparator );

        ListBasedBlockEntryCursor<KEY,VALUE> entries = new ListBasedBlockEntryCursor<>( bufferedEntries );
        writeBlock( storeChannel, entries, blockSize, bufferedEntries.size(), NOT_CANCELLABLE );

        // Append to file
        monitor.blockFlushed( bufferedEntries.size(), currentBufferSize, storeChannel.position() );
        resetBufferedEntries();
    }

    /**
     * There are two files: sourceFile and targetFile. Blocks are merged, mergeFactor at the time, from source to target. When all blocks from source have
     * been merged into a larger block one merge iteration is done and source and target are flipped. As long as source contain more than a single block more
     * merge iterations are needed and we start over again.
     * When source only contain a single block we are finished and the extra file is deleted and {@link #blockFile} contains the result with a single sorted
     * block.
     *
     * See {@link #performSingleMerge(int, BlockReader, StoreChannel, Cancellation)} for further details.
     *
     * @param mergeFactor See {@link #performSingleMerge(int, BlockReader, StoreChannel, Cancellation)}.
     * @param cancellation Injected so that this merge can be cancelled, if an external request to do that comes in.
     * A cancelled merge will leave the same end state file/channel-wise, just not quite completed, which is fine because the merge
     * was cancelled meaning that the result will not be used for anything other than deletion.
     * @throws IOException If something goes wrong when reading from file.
     */
    public void merge( int mergeFactor, Cancellation cancellation ) throws IOException
    {
        File sourceFile = blockFile;
        File tempFile = new File( blockFile.getParent(), blockFile.getName() + ".b" );
        try
        {
            File targetFile = tempFile;
            while ( numberOfBlocksInCurrentFile > 1 )
            {
                // Perform one complete merge iteration, merging all blocks from source into target.
                // After this step, target will contain fewer blocks than source, but may need another merge iteration.
                try ( BlockReader<KEY,VALUE> reader = reader( sourceFile );
                      StoreChannel targetChannel = fs.open( targetFile, OpenMode.READ_WRITE ) )
                {
                    int blocksMergedSoFar = 0;
                    int blocksInMergedFile = 0;
                    while ( !cancellation.cancelled() && blocksMergedSoFar < numberOfBlocksInCurrentFile )
                    {
                        blocksMergedSoFar += performSingleMerge( mergeFactor, reader, targetChannel, cancellation );
                        blocksInMergedFile++;
                    }
                    numberOfBlocksInCurrentFile = blocksInMergedFile;
                    monitor.mergeIterationFinished( blocksMergedSoFar, blocksInMergedFile );
                }

                // Flip and restore the channelz
                File tmpSourceFile = sourceFile;
                sourceFile = targetFile;
                targetFile = tmpSourceFile;
            }
        }
        finally
        {
            if ( sourceFile == blockFile )
            {
                fs.deleteFile( tempFile );
            }
            else
            {
                fs.deleteFile( blockFile );
                fs.renameFile( tempFile, blockFile );
            }
        }
    }

    /**
     * Merge some number of blocks, how many is decided by mergeFactor, into a single sorted block. This is done by opening {@link BlockEntryReader} on each
     * block that we want to merge and give them to a {@link MergingBlockEntryReader}. The {@link BlockEntryReader}s are pulled from a {@link BlockReader}
     * that iterate over Blocks in file in sequential order.
     *
     * {@link MergingBlockEntryReader} pull head from each {@link BlockEntryReader} and hand them out in sorted order, making the multiple entry readers look
     * like a single large and sorted entry reader.
     *
     * The large block resulting from the merge is written down to targetChannel by calling
     * {@link #writeBlock(StoreChannel, BlockEntryCursor, long, long, Cancellation)}.
     *
     * @param mergeFactor How many blocks to merge at the same time. Influence how much memory will be used because each merge block will have it's own
     * {@link ByteBuffer} that they read from.
     * @param reader The {@link BlockReader} to pull blocks / {@link BlockEntryReader}s from.
     * @param targetChannel The {@link StoreChannel} to write the merge result to. Result will be appended to current position.
     * @param cancellation Injected so that this merge can be cancelled, if an external request to do that comes in.
     * @return The number of blocks that where merged, most often this will be equal to mergeFactor but can be less if there are fewer blocks left in source.
     * @throws IOException If something goes wrong when reading from file.
     */
    private int performSingleMerge( int mergeFactor, BlockReader<KEY,VALUE> reader, StoreChannel targetChannel, Cancellation cancellation )
            throws IOException
    {
        try ( MergingBlockEntryReader<KEY,VALUE> merger = new MergingBlockEntryReader<>( layout ) )
        {
            long blockSize = 0;
            long entryCount = 0;
            int blocksMerged = 0;
            for ( int i = 0; i < mergeFactor; i++ )
            {
                BlockEntryReader<KEY,VALUE> source = reader.nextBlock();
                if ( source != null )
                {
                    blockSize += source.blockSize();
                    entryCount += source.entryCount();
                    blocksMerged++;
                    merger.addSource( source );
                }
                else
                {
                    break;
                }
            }

            writeBlock( targetChannel, merger, blockSize, entryCount, cancellation );
            monitor.mergedBlocks( blockSize, entryCount, blocksMerged );
            return blocksMerged;
        }
    }

    private void writeBlock( StoreChannel targetChannel, BlockEntryCursor<KEY,VALUE> blockEntryCursor, long blockSize, long entryCount,
            Cancellation cancellation ) throws IOException
    {
        writeHeader( byteBuffer, blockSize, entryCount );
        long actualDataSize = writeEntries( targetChannel, byteBuffer, layout, blockEntryCursor, cancellation );
        writeLastEntriesWithPadding( targetChannel, byteBuffer, blockSize - actualDataSize );
    }

    private static void writeHeader( ByteBuffer byteBuffer, long blockSize, long entryCount )
    {
        byteBuffer.putLong( blockSize );
        byteBuffer.putLong( entryCount );
    }

    private static <KEY, VALUE> long writeEntries( StoreChannel targetChannel, ByteBuffer byteBuffer, Layout<KEY,VALUE> layout,
            BlockEntryCursor<KEY,VALUE> blockEntryCursor, Cancellation cancellation ) throws IOException
    {
        // Loop over block entries
        long actualDataSize = BLOCK_HEADER_SIZE;
        ByteArrayPageCursor pageCursor = new ByteArrayPageCursor( byteBuffer );
        while ( blockEntryCursor.next() )
        {
            KEY key = blockEntryCursor.key();
            VALUE value = blockEntryCursor.value();
            int entrySize = BlockEntry.entrySize( layout, key, value );
            actualDataSize += entrySize;

            if ( byteBuffer.remaining() < entrySize )
            {
                // First check if this merge have been cancelled, if so just break here, it's fine.
                if ( cancellation.cancelled() )
                {
                    break;
                }

                // flush and reset + DON'T PAD!!!
                byteBuffer.flip();
                targetChannel.writeAll( byteBuffer );
                byteBuffer.clear();
            }

            BlockEntry.write( pageCursor, layout, key, value );
        }
        return actualDataSize;
    }

    private static void writeLastEntriesWithPadding( StoreChannel channel, ByteBuffer byteBuffer, long padding ) throws IOException
    {
        boolean didWrite;
        do
        {
            int toPadThisTime = (int) Math.min( byteBuffer.remaining(), padding );
            byte[] padArray = new byte[toPadThisTime];
            byteBuffer.put( padArray );
            padding -= toPadThisTime;
            didWrite = byteBuffer.position() > 0;
            if ( didWrite )
            {
                byteBuffer.flip();
                channel.writeAll( byteBuffer );
                byteBuffer.clear();
            }
        }
        while ( didWrite );
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( storeChannel );
        fs.deleteFile( blockFile );
    }

    BlockReader<KEY,VALUE> reader() throws IOException
    {
        return reader( blockFile );
    }

    private BlockReader<KEY,VALUE> reader( File file ) throws IOException
    {
        return new BlockReader<>( fs, file, layout, bufferFactory, blockSize );
    }

    public interface Monitor
    {
        void entryAdded( int entrySize );

        void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush );

        void mergeIterationFinished( int numberOfBlocksBefore, int numberOfBlocksAfter );

        void mergedBlocks( long resultingBlockSize, long resultingEntryCount, int numberOfBlocks );

        class Adapter implements Monitor
        {
            @Override
            public void entryAdded( int entrySize )
            {   // no-op
            }

            @Override
            public void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush )
            {   // no-op
            }

            @Override
            public void mergeIterationFinished( int numberOfBlocksBefore, int numberOfBlocksAfter )
            {   // no-op
            }

            @Override
            public void mergedBlocks( long resultingBlockSize, long resultingEntryCount, int numberOfBlocks )
            {   // no-op
            }
        }

        Monitor NO_MONITOR = new Adapter();
    }

    public interface Cancellation
    {
        boolean cancelled();
    }

    static final Cancellation NOT_CANCELLABLE = () -> false;
}
