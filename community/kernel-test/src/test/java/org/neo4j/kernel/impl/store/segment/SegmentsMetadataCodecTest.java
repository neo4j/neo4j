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

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.wal.InMemoryClosableChannel;

class SegmentsMetadataCodecTest {

    @Test
    void roundTripMetadata() throws IOException {
        var metadata = new SegmentsMetadata(
                42,
                List.of(
                        new FileSegmentsMetadata("block.x1.db", new long[] {1, 5, 42}),
                        new FileSegmentsMetadata("block.big_values.db", new long[] {7}),
                        new FileSegmentsMetadata("schema/index/range-1.0/1/index-1", new long[] {3, 3})));
        var channel = new InMemoryClosableChannel();

        SegmentsMetadataCodec.write(channel, metadata);
        var read = SegmentsMetadataCodec.read(channel);

        assertThat(read.lastCheckpointAppendIndex()).isEqualTo(42);
        assertThat(marksByName(read)).isEqualTo(marksByName(metadata));
    }

    @Test
    void roundTripEmptyMetadata() throws IOException {
        var channel = new InMemoryClosableChannel();

        SegmentsMetadataCodec.write(channel, EMPTY_SEGMENTS_METADATA);
        var read = SegmentsMetadataCodec.read(channel);

        assertThat(read.lastCheckpointAppendIndex()).isEqualTo(UNKNOWN_APPEND_INDEX);
        assertThat(read.filesMetadata()).isEmpty();
    }

    @Test
    void writeFilesSortedByName() throws IOException {
        var metadata = new SegmentsMetadata(
                10,
                List.of(
                        new FileSegmentsMetadata("c", new long[] {1}),
                        new FileSegmentsMetadata("a", new long[] {2}),
                        new FileSegmentsMetadata("b", new long[] {3})));
        var channel = new InMemoryClosableChannel();

        SegmentsMetadataCodec.write(channel, metadata);
        var read = SegmentsMetadataCodec.read(channel);

        assertThat(read.filesMetadata()).map(FileSegmentsMetadata::name).containsExactly("a", "b", "c");
    }

    private static Map<String, List<Long>> marksByName(SegmentsMetadata metadata) {
        return metadata.filesMetadata().stream()
                .collect(toMap(
                        FileSegmentsMetadata::name,
                        file -> Arrays.stream(file.segmentAppendIndexes())
                                .boxed()
                                .toList()));
    }
}
