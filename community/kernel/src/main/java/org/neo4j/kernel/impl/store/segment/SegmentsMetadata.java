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

import static java.util.Collections.emptyList;
import static org.neo4j.io.pagecache.impl.muninn.StoreFile.segmentPath;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.segment.FileSegmentMetadata;
import org.neo4j.storageengine.api.SegmentsFilter;

public record SegmentsMetadata(long lastCheckpointAppendIndex, Collection<FileSegmentsMetadata> filesMetadata) {
    public static final SegmentsMetadata EMPTY_SEGMENTS_METADATA =
            new SegmentsMetadata(UNKNOWN_APPEND_INDEX, emptyList());

    public SegmentsMetadata combine(long checkpointAppendIndex, Collection<FileSegmentMetadata> changedSegments) {
        assert checkpointAppendIndex >= lastCheckpointAppendIndex;
        Map<String, long[]> previousFileMarks = new HashMap<>();
        for (var fileMetadata : filesMetadata) {
            previousFileMarks.put(fileMetadata.name(), fileMetadata.segmentAppendIndexes());
        }

        List<FileSegmentsMetadata> combined = new ArrayList<>(changedSegments.size());
        for (var changes : changedSegments) {
            int[] segments = changes.changesSegments();
            long[] previousMarks = previousFileMarks.get(changes.name());
            long[] marks;
            if (previousMarks == null) {
                marks = new long[Math.max(changes.segmentCount(), requiredLength(segments))];
                Arrays.fill(marks, checkpointAppendIndex);
            } else {
                marks = grow(
                        previousMarks,
                        Math.max(changes.segmentCount(), requiredLength(segments)),
                        checkpointAppendIndex);
                for (int segment : segments) {
                    marks[segment] = checkpointAppendIndex;
                }
            }
            combined.add(new FileSegmentsMetadata(changes.name(), marks));
        }
        return new SegmentsMetadata(checkpointAppendIndex, combined);
    }

    public SegmentsFilter differenceFilter(SegmentsMetadata other, DatabaseLayout databaseLayout) {
        if (other == EMPTY_SEGMENTS_METADATA || isIncorrectState(other)) {
            return SegmentsFilter.ALL;
        }
        Map<Path, Long> localSegmentMarks = buildSegmentMarks(this);
        Map<Path, Long> clientSegmentMarks = buildSegmentMarks(other);

        return segments -> {
            List<Path> differentFiles = new ArrayList<>();
            Path databaseDirectory = databaseLayout.databaseDirectory();
            for (Path segment : segments) {
                Path relativePath = databaseDirectory.relativize(segment);
                if (isDifferent(relativePath, localSegmentMarks, clientSegmentMarks)) {
                    differentFiles.add(segment);
                }
            }
            return differentFiles;
        };
    }

    private static Map<Path, Long> buildSegmentMarks(SegmentsMetadata metadata) {
        Map<Path, Long> markers = new HashMap<>();
        for (var fileMetadata : metadata.filesMetadata) {
            var basePath = Path.of(fileMetadata.name());
            long[] marks = fileMetadata.segmentAppendIndexes();
            for (int segment = 0; segment < marks.length; segment++) {
                markers.put(segmentPath(basePath, segment), marks[segment]);
            }
        }
        return markers;
    }

    private boolean isIncorrectState(SegmentsMetadata clientState) {
        long clientCheckpoint = clientState.lastCheckpointAppendIndex;
        return clientCheckpoint < 0
                || clientState.filesMetadata.isEmpty()
                || filesMetadata.isEmpty()
                || clientCheckpoint > lastCheckpointAppendIndex;
    }

    private static boolean isDifferent(
            Path file, Map<Path, Long> localSegmentMarks, Map<Path, Long> clientSegmentMarks) {
        Long clientStamp = clientSegmentMarks.get(file);
        if (clientStamp == null) {
            return true;
        }
        Long localStamp = localSegmentMarks.get(file);
        if (localStamp == null) {
            return true;
        }
        return clientStamp < localStamp;
    }

    private static int requiredLength(int[] changedSegments) {
        int length = 0;
        for (int segment : changedSegments) {
            length = Math.max(length, segment + 1);
        }
        return length;
    }

    private static long[] grow(long[] marks, int requiredLength, long checkpointAppendIndex) {
        if (requiredLength <= marks.length) {
            return Arrays.copyOf(marks, requiredLength);
        }
        long[] grown = Arrays.copyOf(marks, requiredLength);
        Arrays.fill(grown, marks.length, requiredLength, checkpointAppendIndex);
        return grown;
    }
}
