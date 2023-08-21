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
package org.neo4j.internal.id;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.IOUtils.closeAll;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.neo4j.function.ThrowingIntFunction;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

/**
 * Writes and reads buffered IDs to/from disk, using {@link FileSystemAbstraction} and {@link StoreChannel}.
 *
 * <pre>
 *
 *     Writes and reads segments of a rough max size (50MiB)
 *
 *     [segment1     ] , [segment2     ] , [segment3     ]
 *              ^                             ^
 *            reader                        writer
 *
 *     And deletes segments that have been fully read and applied
 *
 *                       [segment2     ] , [segment3     ] , [segment4     ]
 *                            ^                                        ^
 *                          reader                                   writer
 * </pre>
 */
class DiskBufferedIds implements BufferedIds {
    static final int DEFAULT_SEGMENT_SIZE = (int) mebiBytes(50);
    private static final int INITIAL_SEGMENT_ID = 0;
    private static final byte HEADER_CHUNK = 0x1;

    private final FileSystemAbstraction fs;
    private final Path basePath;
    private final MemoryTracker memoryTracker;
    private final int segmentSize;
    private volatile Position<PhysicalFlushableChannel> writePosition;
    private volatile Position<ReadAheadChannel<StoreChannel>> readPosition;

    DiskBufferedIds(FileSystemAbstraction fs, Path basePath, MemoryTracker memoryTracker, int segmentSize)
            throws IOException {
        this.fs = fs;
        this.basePath = basePath;
        this.memoryTracker = memoryTracker;
        this.segmentSize = segmentSize;

        clearExistingSegments();
        this.writePosition = new Position<>(openSegmentForWriting(INITIAL_SEGMENT_ID), INITIAL_SEGMENT_ID, 0);
        this.readPosition = new Position<>(openSegmentForReading(INITIAL_SEGMENT_ID), INITIAL_SEGMENT_ID, 0);
    }

    private PhysicalFlushableChannel openSegmentForWriting(int segmentId) throws IOException {
        return new PhysicalFlushableChannel(
                fs.open(segmentName(segmentId), Set.of(CREATE, TRUNCATE_EXISTING, WRITE)),
                new NativeScopedBuffer(
                        PhysicalFlushableChannel.DEFAULT_BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN, memoryTracker));
    }

    private ReadAheadChannel<StoreChannel> openSegmentForReading(int segmentId) throws IOException {
        return new ReadAheadChannel<>(
                fs.open(segmentName(segmentId), Set.of(READ)),
                new NativeScopedBuffer(
                        ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE, ByteOrder.LITTLE_ENDIAN, memoryTracker));
    }

    @VisibleForTesting
    Path segmentName(int segmentId) {
        return basePath.resolveSibling(basePath.getFileName().toString() + "." + segmentId);
    }

    @Override
    public void write(IdController.TransactionSnapshot snapshot, List<BufferingIdGeneratorFactory.IdBuffer> idBuffers)
            throws IOException {
        var segment = writePosition.segment;
        segment.position(writePosition.offset);
        segment.put(HEADER_CHUNK);
        segment.putLong(snapshot.snapshotTimeMillis());
        segment.putLong(snapshot.lastCommittedTransactionId());
        var currentSequenceNumber = snapshot.currentSequenceNumber();
        segment.putLong(currentSequenceNumber);

        segment.putInt(idBuffers.size());
        for (var buffer : idBuffers) {
            segment.putInt(buffer.idTypeOrdinal());
            segment.putInt(buffer.ids().size());
            var idIterator = buffer.ids().iterator();
            while (idIterator.hasNext()) {
                segment.putLong(idIterator.next());
            }
        }
        segment.prepareForFlush(); // but don't flush (i.e. force) it, it's just unnecessary work
        writePosition = checkRotate(writePosition, segment.position(), this::openSegmentForWriting);
        IOUtils.closeAll(idBuffers);
    }

    @Override
    public void read(BufferedIdVisitor visitor) throws IOException {
        while (hasMoreToRead()) {
            // Read one entire chunk and hand over to the visitor as we go
            var segment = readPosition.segment;
            segment.position(readPosition.offset);
            var header = segment.get();
            Preconditions.checkState(header == HEADER_CHUNK, "Expecting to read header, but instead read %d", header);
            var timeMillis = segment.getLong();
            var lastCommittedTxId = segment.getLong();
            var transactionSequenceNumber = segment.getLong();
            if (!visitor.startChunk(
                    new IdController.TransactionSnapshot(transactionSequenceNumber, timeMillis, lastCommittedTxId))) {
                // Snapshot still open
                break;
            }

            processChunk(visitor, segment);
            readPosition = checkRotate(readPosition, segment.position(), segmentId -> {
                fs.deleteFile(segmentName(segmentId - 1));
                return openSegmentForReading(segmentId);
            });
        }
    }

    private void processChunk(BufferedIdVisitor visitor, ReadAheadChannel<StoreChannel> segment) throws IOException {
        try {
            var numIdTypes = segment.getInt();
            for (var t = 0; t < numIdTypes; t++) {
                var idTypeOrdinal = segment.getInt();
                visitor.startType(idTypeOrdinal);
                try {
                    var numIds = segment.getInt();
                    for (var i = 0; i < numIds; i++) {
                        var id = segment.getLong();
                        visitor.id(id);
                    }
                } finally {
                    visitor.endType();
                }
            }
        } finally {
            visitor.endChunk();
        }
    }

    private <CHANNEL extends Closeable> Position<CHANNEL> checkRotate(
            Position<CHANNEL> position, long offset, ThrowingIntFunction<CHANNEL, IOException> segmentFactory)
            throws IOException {
        CHANNEL segment = position.segment;
        int segmentId = position.segmentId;
        if (offset > segmentSize) {
            segment.close();
            segment = segmentFactory.apply(++segmentId);
            offset = 0;
        }
        return new Position<>(segment, segmentId, offset);
    }

    private boolean hasMoreToRead() {
        int positionComparison = comparePositions(readPosition, writePosition);
        Preconditions.checkState(
                positionComparison <= 0, "readPosition:" + readPosition + " writePosition:" + writePosition);
        return positionComparison < 0;
    }

    private int comparePositions(Position<?> left, Position<?> right) {
        int segmentIdComparison = Integer.compare(left.segmentId, right.segmentId);
        return segmentIdComparison != 0 ? segmentIdComparison : Long.compare(left.offset, right.offset);
    }

    private void clearExistingSegments() throws IOException {
        Path[] existing;
        try {
            existing = fs.listFiles(
                    basePath.getParent(),
                    entry -> !fs.isDirectory(entry)
                            && entry.getFileName()
                                    .toString()
                                    .startsWith(basePath.getFileName().toString() + "."));
        } catch (NoSuchFileException e) {
            return;
        }

        for (Path path : existing) {
            fs.deleteFile(path);
        }
    }

    @Override
    public void close() throws IOException {
        closeAll(writePosition.segment, readPosition.segment);
        clearExistingSegments();
    }

    private record Position<CHANNEL extends Closeable>(CHANNEL segment, int segmentId, long offset) {}
}
