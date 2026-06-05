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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.AssertableLogProvider;

class EntryCountThresholdTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldReportThresholdReachedWhenThresholdIsReached() {
        PrunePredicate predicate = newThreshold(1).forCycle(2L);
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(10L, 1L)));
    }

    @Test
    void shouldReportThresholdNotReachedWhenThresholdIsNotReached() {
        PrunePredicate predicate = newThreshold(1).forCycle(1L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(10L, 1L)));
    }

    @Test
    void shouldProperlyHandleCaseWithOneEntryPerLogFile() {
        PrunePredicate predicate = newThreshold(1).forCycle(3L);
        LogFileInformation v3 = headerOnly(3L, 3L);
        LogFileInformation v2 = headerOnly(2L, 2L);
        assertFalse(predicate.isLowestVersionToKeep(v3));
        assertTrue(predicate.isLowestVersionToKeep(v2));
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping3Files() {
        PrunePredicate predicate = newThreshold(8).forCycle(18L);
        LogFileInformation v4 = headerOnly(4L, 18L);
        LogFileInformation v3 = headerOnly(3L, 15L);
        LogFileInformation v2 = headerOnly(2L, 5L);
        LogFileInformation v1 = headerOnly(1L, 1L);
        assertFalse(predicate.isLowestVersionToKeep(v4));
        assertFalse(predicate.isLowestVersionToKeep(v3));
        assertTrue(predicate.isLowestVersionToKeep(v2));
        assertTrue(predicate.isLowestVersionToKeep(v1));
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping4Files() {
        PrunePredicate predicate = newThreshold(15).forCycle(18L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(4L, 18L)));
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(3L, 15L)));
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(2L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 1L)));
    }

    @Test
    void shouldWorkWhenCalledMultipleTimesKeeping2FilesOnBoundary() {
        PrunePredicate predicate = newThreshold(3).forCycle(18L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(4L, 18L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(3L, 15L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(2L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 1L)));
    }

    @Test
    void shouldSkipEmptyLogsBetweenLogsThatWillBeKept() {
        // 1, 3 and 4 are empty. 2 has 5 transactions, 5 has 8, 6 is current. Threshold 9 means keep through 2.
        PrunePredicate predicate = newThreshold(9).forCycle(13L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(6L, 13L)));
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(5L, 5L)));
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(4L, 5L)));
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(3L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(2L, 1L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 1L)));
    }

    @Test
    void shouldDeleteNonEmptyLogThatIsAfterASeriesOfEmptyLogs() {
        // Same layout, threshold 8 = exactly what version 5 has, so 2 gets pruned.
        PrunePredicate predicate = newThreshold(8).forCycle(13L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(6L, 13L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(5L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(4L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(3L, 5L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(2L, 1L)));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 1L)));
    }

    @Test
    void thresholdNotReachedWhenEntryIdNotFound() {
        PrunePredicate predicate = newThreshold(0).forCycle(0L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(1L, -1L)));
        assertThat(logProvider)
                .containsMessages(
                        "Failed to get append index of the first entry in the transaction log file. Requested version: 1");
    }

    @Test
    void thresholdNotReachedWhenFailToGetEntryId() {
        PrunePredicate predicate = newThreshold(0).forCycle(0L);
        assertFalse(predicate.isLowestVersionToKeep(throwingHeader(1L, new IOException("boom"))));
        assertThat(logProvider)
                .containsMessages(
                        "Error on attempt to get entry append indexes from transaction log files. Checked version: 1");
    }

    private EntryCountThreshold newThreshold(int maxEntries) {
        return new EntryCountThreshold(logProvider, maxEntries);
    }

    private static LogFileInformation headerOnly(long version, long previousAppendIndex) {
        return new TestLogFileInformation(version) {
            @Override
            public long getPreviousAppendIndexFromHeader() {
                return previousAppendIndex;
            }
        };
    }

    private static LogFileInformation throwingHeader(long version, IOException toThrow) {
        return new TestLogFileInformation(version) {
            @Override
            public long getPreviousAppendIndexFromHeader() throws IOException {
                throw toThrow;
            }
        };
    }
}
