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
package org.neo4j.wal.enveloped;

import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.wal.entry.LogEnvelopeHeader;

class SegmentBinarySearch implements LogBinarySearch.BinarySearchReader {
    private final EnvelopeReadChannel envelopeReadChannel;
    private final ByteBuffer headerBuffer;
    private final int totalSegments;
    private final int segmentBlockSize;

    public SegmentBinarySearch(EnvelopeReadChannel envelopeReadChannel, int totalSegments, int segmentBlockSize) {
        this.envelopeReadChannel = envelopeReadChannel;
        this.headerBuffer = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        this.totalSegments = totalSegments;
        this.segmentBlockSize = segmentBlockSize;
    }

    @Override
    public int size() {
        // we skip the first segment since it's the file header
        return totalSegments - 1;
    }

    @Override
    public long get(int index) {
        // index + 1 because we skip the first segment since it's the file header
        return (long) (index + 1) * segmentBlockSize;
    }

    @Override
    public int compare(long position, long target) {
        try {
            envelopeReadChannel.channel().position(position);
            try {
                var envelopedHeader = readHeader();
                var type = envelopedHeader.type;
                var entryIndex = envelopedHeader.entryIndex;

                return switch (type) {
                    case ZERO -> 1;
                    case BEGIN, FULL -> Long.compare(entryIndex, target);
                    case END, MIDDLE, START_OFFSET -> {
                        if (entryIndex >= target) {
                            // If it's the exact match or larger than target,
                            // it means that the actual start of the target
                            // is in an earlier segment - so we return 1
                            yield 1;
                        }
                        yield -1;
                    }
                };
            } catch (ReadPastEndException | BufferUnderflowException e) {
                return 1;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private EnvelopedHeader readHeader() throws IOException {
        headerBuffer.clear();
        envelopeReadChannel.channel().read(headerBuffer);
        headerBuffer.flip();
        var header = LogEnvelopeHeader.readHeader(headerBuffer);
        return new EnvelopedHeader(header.type(), header.appendIndex());
    }

    private record EnvelopedHeader(LogEnvelopeHeader.EnvelopeType type, long entryIndex) {}
}
