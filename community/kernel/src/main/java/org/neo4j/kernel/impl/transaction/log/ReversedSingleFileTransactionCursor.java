/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.util.Arrays;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.util.collection.ArrayCollection;

/**
 * Returns transactions in reverse order in a log file. It tries to keep peak memory consumption to a minimum
 * by first sketching out the offsets of all transactions in the log. Then it starts from the end and moves backwards,
 * taking advantage of read-ahead feature of the {@link ReadAheadLogChannel} by moving in chunks backwards in roughly
 * the size of the read-ahead window. Coming across large transactions means moving further back to at least read one transaction
 * per chunk "move". This is all internal, so from the outside it simply reverses a transaction log.
 * The memory overhead compared to reading a log in the natural order is almost negligible.
 *
 * This cursor currently only works for a single log file, such that the given {@link ReadAheadLogChannel} should not be
 * instantiated with a {@link LogVersionBridge} moving it over to other versions when exhausted. For reversing a whole
 * log stream consisting of multiple log files have a look at {@link ReversedMultiFileTransactionCursor}.
 *
 * @see ReversedMultiFileTransactionCursor
 */
class ReversedSingleFileTransactionCursor implements TransactionCursor
{
    // Should this be passed in or extracted from the read-ahead channel instead?
    private static final int CHUNK_SIZE = ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

    private final ReadAheadLogChannel channel;
    private final TransactionCursor transactionCursor;
    // Should be generally large enough to hold transactions in a chunk, where one chunk is the read-ahead size of ReadAheadLogChannel
    private final ArrayCollection<CommittedTransactionRepresentation> chunkTransactions = new ArrayCollection<>( 20 );
    // May be longer than required, offsetLength holds the actual length.
    private final long[] offsets;
    private int offsetsLength;
    private int chunkStartOffsetIndex;
    private int chunkLength;
    private int chunkCursor;
    private long totalSize;

    ReversedSingleFileTransactionCursor( ReadAheadLogChannel channel, LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader )
            throws IOException
    {
        this.channel = channel;
        // There's an assumption here: that the underlying channel can move in between calls and that the
        // transaction cursor will just happily read from the new position.
        this.transactionCursor = new PhysicalTransactionCursor<>( channel, logEntryReader );
        this.offsets = sketchOutTransactionStartOffsets();
    }

    // Also initializes offset indexes
    // This method could use some way of reading log entries w/o creating objects. Would be great
    private long[] sketchOutTransactionStartOffsets() throws IOException
    {
        // Grows on demand. Initially sized to be able to hold all transaction start offsets for a single log file
        long[] offsets = new long[10_000];
        int offsetCursor = 0;

        long startOffset = channel.position();
        while ( transactionCursor.next() )
        {
            if ( offsetCursor == offsets.length )
            {   // Grow
                offsets = Arrays.copyOf( offsets, offsetCursor * 2 );
            }
            offsets[offsetCursor++] = startOffset;
            long nextStartOffset = channel.position();
            assert nextStartOffset > startOffset : "It looks like this channel moved over to another version";
            startOffset = channel.position();
        }

        offsetsLength = offsetCursor;
        chunkStartOffsetIndex = offsetCursor;
        totalSize = channel.position();

        return offsets;
    }

    @Override
    public boolean next() throws IOException
    {
        if ( !exhausted() )
        {
            if ( currentChunkExhausted() )
            {
                readNextChunk();
            }
            chunkCursor++;
            assert chunkCursor <= chunkLength;
            return true;
        }
        return false;
    }

    private void readNextChunk() throws IOException
    {
        assert chunkStartOffsetIndex > 0;

        // Start at lowOffsetIndex - 1 and count backwards until almost reaching the chunk size
        long highOffset = chunkStartOffsetIndex == offsetsLength ? totalSize : offsets[chunkStartOffsetIndex];
        int newLowOffsetIndex = chunkStartOffsetIndex;
        while ( newLowOffsetIndex > 0 )
        {
            long deltaOffset = highOffset - offsets[--newLowOffsetIndex];
            if ( deltaOffset > CHUNK_SIZE )
            {   // We've come too far
                if ( chunkStartOffsetIndex - newLowOffsetIndex > 1 )
                {   // Don't include last this transaction, since it wasn't the only one
                    newLowOffsetIndex++;
                }
                break;
            }
        }
        assert chunkStartOffsetIndex - newLowOffsetIndex > 0;

        // We've established the chunk boundaries. Initialize all offsets and read the transactions in this
        // chunk into actual transaction objects
        chunkLength = chunkStartOffsetIndex - newLowOffsetIndex;
        chunkStartOffsetIndex = newLowOffsetIndex;
        chunkCursor = 0;
        channel.setCurrentPosition( offsets[chunkStartOffsetIndex] );
        chunkTransactions.clear();
        for ( int i = 0; i < chunkLength; i++ )
        {
            boolean success = transactionCursor.next();
            assert success;

            chunkTransactions.add( transactionCursor.get() );
        }
    }

    private boolean currentChunkExhausted()
    {
        return chunkCursor == chunkLength;
    }

    private boolean exhausted()
    {
        return chunkStartOffsetIndex == 0 && currentChunkExhausted();
    }

    @Override
    public void close() throws IOException
    {
        transactionCursor.close(); // closes the channel too
    }

    @Override
    public CommittedTransactionRepresentation get()
    {
        return chunkTransactions.item( chunkLength - chunkCursor );
    }

    @Override
    public LogPosition position()
    {
        throw new UnsupportedOperationException( "Perhaps not required" );
    }
}
