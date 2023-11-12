/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.reverse;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.CommittedCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.SketchingCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;

/**
 * Returns command batches in reverse order in a log file. It tries to keep peak memory consumption to a minimum
 * by first sketching out the offsets of all transactions in the log. Then it starts from the end and moves backwards,
 * taking advantage of read-ahead feature of the {@link ReadAheadLogChannel} by moving in chunks backwards in roughly
 * the size of the read-ahead window. Coming across large batches means moving further back to at least read one batch
 * per chunk "move". This is all internal, so from the outside it simply reverses a transaction log.
 * The memory overhead compared to reading a log in the natural order is almost negligible.
 *
 * This cursor currently only works for a single log file, such that the given {@link ReadAheadLogChannel} should not be
 * instantiated with a {@link LogVersionBridge} moving it over to other versions when exhausted. For reversing a whole
 * log stream consisting of multiple log files have a look at {@link ReversedMultiFileCommandBatchCursor}.
 *
 * <pre>
 *
 *              ◄────────────────┤                          {@link #chunkBatches} for the current chunk, reading {@link #readNextChunk()}.
 * [2  |3|4    |5  |6          |7 |8   |9      |10  ]
 * ▲   ▲ ▲     ▲   ▲           ▲  ▲    ▲       ▲
 * │   │ │     │   │           │  │    │       │
 * └───┴─┴─────┼───┴───────────┴──┴────┴───────┴─────────── {@link #offsets}
 *             │
 *             └─────────────────────────────────────────── {@link #chunkStartOffsetIndex} moves forward in {@link #readNextChunk()}
 *
 * </pre>
 *
 * @see ReversedMultiFileCommandBatchCursor
 */
public class ReversedSingleFileCommandBatchCursor implements CommandBatchCursor {
    // Should this be passed in or extracted from the read-ahead channel instead?
    private static final int CHUNK_SIZE = ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

    private final ReadAheadLogChannel channel;
    private final boolean failOnCorruptedLogFiles;
    private final ReversedTransactionCursorMonitor monitor;
    private final CommandBatchCursor commandBatchCursor;
    // Should be generally large enough to hold transactions in a chunk, where one chunk is the read-ahead size of
    // ReadAheadLogChannel
    private final Deque<CommittedCommandBatch> chunkBatches = new ArrayDeque<>(20);
    private final SketchingCommandBatchCursor sketchingCursor;
    private CommittedCommandBatch currentCommandBatch;
    // May be longer than required, offsetLength holds the actual length.
    private final long[] offsets;
    private int offsetsLength;
    private int chunkStartOffsetIndex;
    private long totalSize;

    ReversedSingleFileCommandBatchCursor(
            ReadAheadLogChannel channel,
            LogEntryReader logEntryReader,
            boolean failOnCorruptedLogFiles,
            ReversedTransactionCursorMonitor monitor)
            throws IOException {
        this.channel = channel;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.monitor = monitor;
        // There's an assumption here: that the underlying channel can move in between calls and that the
        // transaction cursor will just happily read from the new position.
        this.commandBatchCursor = new CommittedCommandBatchCursor(channel, logEntryReader);
        this.sketchingCursor = new SketchingCommandBatchCursor(channel, logEntryReader);
        this.offsets = sketchOutTransactionStartOffsets();
    }

    // Also initializes offset indexes
    private long[] sketchOutTransactionStartOffsets() throws IOException {
        // Grows on demand. Initially sized to be able to hold all transaction start offsets for a single log file
        long[] offsets = new long[10_000];
        int offsetCursor = 0;

        long logVersion = channel.getLogVersion();
        long startOffset = channel.position();
        try {
            while (sketchingCursor.next()) {
                if (offsetCursor == offsets.length) {
                    offsets = Arrays.copyOf(offsets, offsetCursor * 2);
                }
                offsets[offsetCursor++] = startOffset;
                startOffset = channel.position();
            }
        } catch (IllegalStateException | IOException | UnsupportedLogVersionException e) {
            monitor.transactionalLogRecordReadFailure(offsets, offsetCursor, logVersion);
            if (failOnCorruptedLogFiles) {
                throw e;
            }
        }

        if (channel.getLogVersion() != logVersion) {
            throw new IllegalArgumentException(
                    "The channel which was passed in bridged multiple log versions, it started at version " + logVersion
                            + ", but continued through to version " + channel.getLogVersion()
                            + ". This isn't supported");
        }

        offsetsLength = offsetCursor;
        chunkStartOffsetIndex = offsetCursor;
        totalSize = channel.position();

        return offsets;
    }

    @Override
    public boolean next() throws IOException {
        if (!exhausted()) {
            if (currentChunkExhausted()) {
                readNextChunk();
            }
            currentCommandBatch = chunkBatches.pop();
            return true;
        }
        return false;
    }

    private void readNextChunk() throws IOException {
        assert chunkStartOffsetIndex > 0;

        // Start at lowOffsetIndex - 1 and count backwards until almost reaching the chunk size
        long highOffset = chunkStartOffsetIndex == offsetsLength ? totalSize : offsets[chunkStartOffsetIndex];
        int newLowOffsetIndex = chunkStartOffsetIndex;
        while (newLowOffsetIndex > 0) {
            long deltaOffset = highOffset - offsets[--newLowOffsetIndex];
            if (deltaOffset
                    > CHUNK_SIZE) { // We've now read more than the read-ahead size, let's call this the end of this
                // chunk
                break;
            }
        }
        assert chunkStartOffsetIndex - newLowOffsetIndex > 0;

        // We've established the chunk boundaries. Initialize all offsets and read the transactions in this
        // chunk into actual transaction objects
        int chunkLength = chunkStartOffsetIndex - newLowOffsetIndex;
        chunkStartOffsetIndex = newLowOffsetIndex;
        channel.position(offsets[chunkStartOffsetIndex]);
        assert chunkBatches.isEmpty();
        for (int i = 0; i < chunkLength; i++) {
            boolean success = commandBatchCursor.next();
            assert success;

            chunkBatches.push(commandBatchCursor.get());
        }
    }

    private boolean currentChunkExhausted() {
        return chunkBatches.isEmpty();
    }

    private boolean exhausted() {
        return chunkStartOffsetIndex == 0 && currentChunkExhausted();
    }

    @Override
    public void close() throws IOException {
        commandBatchCursor.close(); // closes the channel too
    }

    @Override
    public CommittedCommandBatch get() {
        return currentCommandBatch;
    }

    @Override
    public LogPosition position() {
        throw new UnsupportedOperationException("Should not be called");
    }
}
