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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.segment.DatabaseSegmentTracker;
import org.neo4j.io.pagecache.segment.FileSegmentMetadata;
import org.neo4j.io.pagecache.segment.FileSegmentTracker;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class SegmentedStoreMetadataServiceTest {

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    private final StoreId storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
    private final StubSegmentTracker segmentTracker = new StubSegmentTracker();

    @Test
    void readAndWriteMetadataFile() {
        var service = metadataService(storeId);

        segmentTracker.set(
                new FileSegmentMetadata("block.x1.db", 1, new int[] {0}),
                new FileSegmentMetadata("block.big_values.db", 1, new int[] {0}));
        service.storeMetadata(1);

        segmentTracker.set(
                new FileSegmentMetadata("block.x1.db", 3, new int[] {1, 2}),
                new FileSegmentMetadata("block.big_values.db", 1, new int[] {}));
        service.storeMetadata(7);

        var read = service.segmentsMetadata();

        assertThat(read.lastCheckpointAppendIndex()).isEqualTo(7);
        assertThat(marksByName(read))
                .isEqualTo(Map.of(
                        "block.x1.db", List.of(1L, 7L, 7L),
                        "block.big_values.db", List.of(1L)));
    }

    @Test
    void returnEmptyMetadataWhenFileMissing() {
        assertThat(metadataService(storeId).segmentsMetadata()).isEqualTo(EMPTY_SEGMENTS_METADATA);
    }

    @Test
    void returnEmptyMetadataOnStoreIdMismatch() {
        segmentTracker.set(new FileSegmentMetadata("block.x1.db", 1, new int[] {0}));
        metadataService(storeId).storeMetadata(42);

        var otherStoreId = new StoreId(5, 6, "engine-1", "format-1", 3, 4);

        assertThat(metadataService(otherStoreId).segmentsMetadata()).isEqualTo(EMPTY_SEGMENTS_METADATA);
    }

    @Test
    void returnEmptyMetadataOnCorruptedFile() throws IOException {
        segmentTracker.set(new FileSegmentMetadata("block.x1.db", 1, new int[] {0}));
        var service = metadataService(storeId);
        service.storeMetadata(42);

        flipByte(metadataFile());

        assertThat(service.segmentsMetadata()).isEqualTo(EMPTY_SEGMENTS_METADATA);
    }

    private SegmentedStoreMetadataService metadataService(StoreId storeId) {
        var databaseLayout = mock(DatabaseLayout.class);
        when(databaseLayout.segmentsMetadata()).thenReturn(metadataFile());
        return new SegmentedStoreMetadataService(
                fs,
                databaseLayout,
                storeId,
                segmentTracker,
                EmptyMemoryTracker.INSTANCE,
                new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance()),
                NullLog.getInstance());
    }

    private Path metadataFile() {
        return directory.homePath().resolve("block.segments.metadata");
    }

    private void flipByte(Path file) throws IOException {
        long position = fs.getFileSize(file) - 5;
        try (var channel = fs.open(file, Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE))) {
            var buffer = ByteBuffer.allocate(1);
            channel.position(position);
            channel.readAll(buffer);
            buffer.flip();
            byte value = buffer.get(0);
            buffer.clear();
            buffer.put((byte) (value ^ 0xFF)).flip();
            channel.position(position);
            channel.writeAll(buffer);
        }
    }

    private static Map<String, List<Long>> marksByName(SegmentsMetadata metadata) {
        return metadata.filesMetadata().stream()
                .collect(toMap(
                        FileSegmentsMetadata::name,
                        file -> Arrays.stream(file.segmentAppendIndexes())
                                .boxed()
                                .toList()));
    }

    private static class StubSegmentTracker implements DatabaseSegmentTracker {
        private Collection<FileSegmentMetadata> current = List.of();

        void set(FileSegmentMetadata... files) {
            current = List.of(files);
        }

        @Override
        public FileSegmentTracker createFileSegmentTracer(Path baseSegmentPath) {
            return FileSegmentTracker.EMPTY_FILE_TRACKER;
        }

        @Override
        public void removeFileSegmentTracer(Path baseSegmentPath) {}

        @Override
        public Collection<FileSegmentMetadata> segmentMetadata() {
            return current;
        }
    }
}
