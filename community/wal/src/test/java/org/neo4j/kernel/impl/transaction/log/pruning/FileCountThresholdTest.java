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

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.NullLogProvider;

class FileCountThresholdTest {

    @Test
    void shouldReturnFalseWhenTheMaxNonEmptyLogCountIsNotReached() {
        PrunePredicate predicate = newThreshold(2).forCycle(0L);
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(1L, 0L)));
    }

    @Test
    void shouldReturnTrueWhenTheMaxNonEmptyLogCountIsReached() {
        // EntryCountThreshold(1) is the inner guard. lastEntryAppendIndex=5, predecessor=3 → delta=2 ≥ 1 → kept.
        PrunePredicate predicate = newThreshold(2).forCycle(5L);
        predicate.isLowestVersionToKeep(headerOnly(1L, 3L));
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 3L)));
    }

    @Test
    void shouldReturnFalseWhenTheMaxNonEmptyLogCountIsReachedButNotTheMinimumOneWholeChunk() {
        PrunePredicate predicate = newThreshold(2).forCycle(5L);
        predicate.isLowestVersionToKeep(headerOnly(1L, 5L));
        // delta = 5 - 5 = 0 < 1 → inner entryCount guard rejects even though file-count threshold reached.
        assertFalse(predicate.isLowestVersionToKeep(headerOnly(1L, 5L)));
        // Once a real chunk's worth of entries lies above, inner guard accepts.
        assertTrue(predicate.isLowestVersionToKeep(headerOnly(1L, 4L)));
    }

    @Test
    void shouldResetCounterAcrossCycles() {
        FileCountThreshold threshold = newThreshold(2);
        PrunePredicate first = threshold.forCycle(5L);
        first.isLowestVersionToKeep(headerOnly(1L, 3L));
        first.isLowestVersionToKeep(headerOnly(1L, 3L));

        PrunePredicate second = threshold.forCycle(5L);
        assertFalse(second.isLowestVersionToKeep(headerOnly(1L, 3L)));
    }

    private static FileCountThreshold newThreshold(int maxFiles) {
        return new FileCountThreshold(maxFiles, NullLogProvider.getInstance());
    }

    private static LogFileInformation headerOnly(long version, long previousAppendIndex) {
        return new TestLogFileInformation(version) {
            @Override
            public long getPreviousAppendIndexFromHeader() {
                return previousAppendIndex;
            }
        };
    }
}
