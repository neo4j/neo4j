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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.segment.FileSegmentMetadata;

class SegmentsMetadataTest {
    private static final long FIRST_APPEND_INDEX = 5;
    private static final long SECOND_APPEND_INDEX = 9;
    private static final Path DB_DIR = Path.of("/data/databases/neo4j");
    private static final long CLIENT_CHECKPOINT = 100;
    private static final long SERVER_CHECKPOINT = 200;

    @Test
    void emptyProfileMarksAllSegmentsOfChangedFile() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3, 1)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(FIRST_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void emptyProfileMarksAllSegmentsOfUnchangedFile() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void emptyProfileCoversChangedSegmentsBeyondSegmentCount() {
        var combined = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 2, 3)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void existingProfileMarksOnlyChangedSegment() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3, 2)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX);
    }

    @Test
    void existingProfileMarksScatteredChangedSegments() {
        var previous =
                profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 4, 0, 2)));

        assertThat(marksOf(combined, "file"))
                .containsExactly(SECOND_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void existingProfileKeptForUnchangedFile() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3)));

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void removeDeletedFileAbsentFromChanges() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of());

        assertThat(combined.lastCheckpointAppendIndex()).isEqualTo(SECOND_APPEND_INDEX);
        assertThat(combined.filesMetadata()).isEmpty();
    }

    @Test
    void existingProfileGrowsForNewSegments() {
        var previous = profile(file("file", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("file", 4, 3)));

        // segment 2 is observed for the first time and marked together with the changed segment 3
        assertThat(marksOf(combined, "file"))
                .containsExactly(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, SECOND_APPEND_INDEX);
    }

    @Test
    void combineFilesWithAndWithoutHistory() {
        var previous = profile(
                file("changed", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX),
                file("deleted", FIRST_APPEND_INDEX, FIRST_APPEND_INDEX));

        var combined = previous.combine(SECOND_APPEND_INDEX, List.of(changes("changed", 2, 1), changes("added", 2, 0)));

        assertThat(marksOf(combined, "changed")).containsExactly(FIRST_APPEND_INDEX, SECOND_APPEND_INDEX);
        assertThat(marksOf(combined, "added")).containsExactly(SECOND_APPEND_INDEX, SECOND_APPEND_INDEX);
        assertThat(combined.filesMetadata()).noneMatch(file -> file.name().equals("deleted"));
    }

    @Test
    void combineSequenceOfProfiles() {
        var first = EMPTY_SEGMENTS_METADATA.combine(FIRST_APPEND_INDEX, List.of(changes("file", 3, 0)));
        var second = first.combine(SECOND_APPEND_INDEX, List.of(changes("file", 3, 1)));

        assertThat(marksOf(second, "file"))
                .containsExactly(FIRST_APPEND_INDEX, SECOND_APPEND_INDEX, FIRST_APPEND_INDEX);
    }

    @Test
    void fetchEverythingWhenClientStateIsEmpty() {
        var files = files("block.x1.db", "block.x1.db.1");
        var serverState = serverState(file("block.x1.db", 50, 50));

        assertThat(filterFilesToFetch(EMPTY_SEGMENTS_METADATA, serverState, files))
                .containsExactly(files);
    }

    @Test
    void fetchEverythingWhenServerStateIsEmpty() {
        var files = files("block.x1.db", "block.x1.db.1", "block.x1.db.2", "block.x1.db.3");
        var clientState = clientState(file("block.x1.db", 50, 50, 47));

        assertThat(filterFilesToFetch(clientState, EMPTY_SEGMENTS_METADATA, files))
                .containsExactly(files);
    }

    @Test
    void fetchEverythingWhenClientIsSomehowAheadOfServer() {
        var files = files("block.x1.db", "block.x1.db.1");
        var clientState = new SegmentsMetadata(SERVER_CHECKPOINT + 1, List.of(file("block.x1.db", 50)));
        var serverState = serverState(file("block.x1.db", 50, 50));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files);
    }

    @Test
    void fetchSegmentsChangedAfterClientCheckpoint() {
        var files = files("block.x1.db", "block.x1.db.1", "block.x1.db.2");
        var clientState = clientState(file("block.x1.db", 50, 60, 70));
        var serverState = serverState(file("block.x1.db", 50, 150, 200));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1], files[2]);
    }

    @Test
    void fetchSegmentChangedAfterClientCheckpoint() {
        var files = files("block.x1.db", "block.x1.db.1", "block.x1.db.2");
        var clientState = clientState(file("block.x1.db", 50, 50, 70));
        var serverState = serverState(file("block.x1.db", 50, 51, 70));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1]);
    }

    @Test
    void fetchSegmentsClientDoesNotHave() {
        var files = files("block.x1.db", "block.x1.db.1", "block.x1.db.2");
        var clientState = clientState(file("block.x1.db", 50));
        var serverState = serverState(file("block.x1.db", 50, 50, 50));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1], files[2]);
    }

    @Test
    void fetchFileMissingFromClientState() {
        var files = files("block.x1.db", "block.big_values.db", "block.big_values.db.1");
        var clientState = clientState(file("block.x1.db", 50));
        var serverState = serverState(file("block.x1.db", 50), file("block.big_values.db", 50, 40));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1], files[2]);
    }

    @Test
    void fetchFileUntrackedOnServer() {
        var files = files("block.x1.db", "some.other.file");
        var clientState = clientState(file("block.x1.db", 50), file("some.other.file", 50));
        var serverState = serverState(file("block.x1.db", 50));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1]);
    }

    @Test
    void fetchNothingForUnchangedVectorStoreFiles() {
        var files = files("vector/block.vector_float32_1024.db", "vector/block.vector_float32_1024.db.1");
        var clientState = clientState(file("vector/block.vector_float32_1024.db", 50, 60));
        var serverState = serverState(file("vector/block.vector_float32_1024.db", 50, 60));

        assertThat(filterFilesToFetch(clientState, serverState, files)).isEmpty();
    }

    @Test
    void fetchVectorStoreSegmentsChangedAfterClientCheckpoint() {
        var files = files(
                "vector/block.vector_float32_1024.db",
                "vector/block.vector_float32_1024.db.1",
                "vector/block.vector_float32_1024.db.2");
        var clientState = clientState(file("vector/block.vector_float32_1024.db", 50, 60, 70));
        var serverState = serverState(file("vector/block.vector_float32_1024.db", 50, 60, 150));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[2]);
    }

    @Test
    void fetchVectorStoreMissingFromClientState() {
        var files =
                files("block.x1.db", "vector/block.vector_float32_1024.db", "vector/block.vector_float32_1024.db.1");
        var clientState = clientState(file("block.x1.db", 50));
        var serverState = serverState(file("block.x1.db", 50), file("vector/block.vector_float32_1024.db", 150, 150));

        assertThat(filterFilesToFetch(clientState, serverState, files)).containsExactly(files[1], files[2]);
    }

    private static Collection<Path> filterFilesToFetch(
            SegmentsMetadata clientState, SegmentsMetadata serverState, Path[] recoverableFiles) {
        return serverState
                .differenceFilter(clientState, DatabaseLayout.ofFlat(DB_DIR))
                .filter(List.of(recoverableFiles));
    }

    private static Path[] files(String... names) {
        Path[] files = new Path[names.length];
        for (int i = 0; i < names.length; i++) {
            files[i] = DB_DIR.resolve(names[i]);
        }
        return files;
    }

    private static SegmentsMetadata clientState(FileSegmentsMetadata... files) {
        return new SegmentsMetadata(CLIENT_CHECKPOINT, List.of(files));
    }

    private static SegmentsMetadata serverState(FileSegmentsMetadata... files) {
        return new SegmentsMetadata(SERVER_CHECKPOINT, List.of(files));
    }

    private static SegmentsMetadata profile(FileSegmentsMetadata... files) {
        return new SegmentsMetadata(FIRST_APPEND_INDEX, List.of(files));
    }

    private static FileSegmentsMetadata file(String name, long... segmentAppendIndexes) {
        return new FileSegmentsMetadata(name, segmentAppendIndexes);
    }

    private static FileSegmentMetadata changes(String name, int segmentCount, int... changedSegments) {
        return new FileSegmentMetadata(name, segmentCount, changedSegments);
    }

    private static long[] marksOf(SegmentsMetadata metadata, String name) {
        return metadata.filesMetadata().stream()
                .filter(file -> file.name().equals(name))
                .findFirst()
                .orElseThrow()
                .segmentAppendIndexes();
    }
}
