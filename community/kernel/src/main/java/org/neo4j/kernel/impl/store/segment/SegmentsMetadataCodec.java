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
package org.neo4j.kernel.impl.store.segment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

public final class SegmentsMetadataCodec {
    private SegmentsMetadataCodec() {}

    public static void write(WritableChannel channel, SegmentsMetadata metadata) throws IOException {
        channel.putLong(metadata.lastCheckpointAppendIndex());
        List<FileSegmentsMetadata> fileStates = new ArrayList<>(metadata.filesMetadata());
        fileStates.sort(comparing(FileSegmentsMetadata::name));
        channel.putInt(fileStates.size());
        for (var fileState : fileStates) {
            byte[] nameBytes = fileState.name().getBytes(UTF_8);
            channel.putInt(nameBytes.length);
            channel.put(nameBytes, nameBytes.length);
            long[] segmentAppendIndexes = fileState.segmentAppendIndexes();
            channel.putInt(segmentAppendIndexes.length);
            for (long appendIndex : segmentAppendIndexes) {
                channel.putLong(appendIndex);
            }
        }
    }

    public static SegmentsMetadata read(ReadableChannel channel) throws IOException {
        long lastCheckpointAppendIndex = channel.getLong();
        int fileCount = channel.getInt();
        List<FileSegmentsMetadata> filesMetadata = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            int nameLength = channel.getInt();
            byte[] nameBytes = new byte[nameLength];
            channel.get(nameBytes, nameLength);
            int segmentCount = channel.getInt();
            long[] segmentAppendIndexes = new long[segmentCount];
            for (int segment = 0; segment < segmentCount; segment++) {
                segmentAppendIndexes[segment] = channel.getLong();
            }
            filesMetadata.add(new FileSegmentsMetadata(new String(nameBytes, UTF_8), segmentAppendIndexes));
        }
        return new SegmentsMetadata(lastCheckpointAppendIndex, filesMetadata);
    }
}
