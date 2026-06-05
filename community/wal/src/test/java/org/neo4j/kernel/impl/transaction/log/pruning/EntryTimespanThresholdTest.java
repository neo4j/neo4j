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

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

class EntryTimespanThresholdTest {
    private final FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
    private final SystemNanoClock clock = new FakeClock(1000, MILLISECONDS);
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldReturnFalseWhenPreviousTimestampIsAtOrAfterLowerLimit() {
        // cutoff = 1000 - 200 = 800; previous file's first ts = 800 → not strictly older.
        PrunePredicate predicate = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 200).forCycle(0L);
        predicate.isLowestVersionToKeep(withTimestamp(5L, 800L)); // captures as previous
        assertFalse(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldReturnTrueWhenPreviousTimestampIsBeforeLowerLimit() {
        // cutoff = 1000 - 100 = 900; previous ts = 800 → older → prune.
        PrunePredicate predicate = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 100).forCycle(0L);
        predicate.isLowestVersionToKeep(withTimestamp(5L, 800L));
        assertTrue(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldReturnFalseOnFirstCallWithNoPriorObservation() {
        PrunePredicate predicate = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 100).forCycle(0L);
        assertFalse(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void thresholdNotReachedWhenTheLogCannotBeRead() {
        PrunePredicate predicate = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 100).forCycle(0L);
        predicate.isLowestVersionToKeep(throwingTimestamp(5L, new IOException("boom"))); // captured as prev
        assertFalse(predicate.isLowestVersionToKeep(file(4L)));
        assertThat(logProvider).containsMessages("Fail to get timestamp info from transaction log file");
    }

    @Test
    void thresholdReachedWhenFileRestrictionIsReached() throws IOException {
        when(fs.getFileSize(java.nio.file.Path.of("test-v4"))).thenReturn(129L);
        PrunePredicate predicate = new EntryTimespanThreshold(
                        logProvider, clock, MILLISECONDS, 200, new FileSizeThreshold(fs, 128, logProvider))
                .forCycle(0L);
        // Size-only threshold trips on the first call.
        assertTrue(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldHonourDaysUnit() {
        FakeClock daysClock = new FakeClock(0, MILLISECONDS);
        daysClock.forward(java.time.Duration.ofDays(30));
        PrunePredicate predicate = new EntryTimespanThreshold(logProvider, daysClock, DAYS, 7).forCycle(0L);
        long eightDaysAgo = daysClock.millis() - java.time.Duration.ofDays(8).toMillis();
        predicate.isLowestVersionToKeep(withTimestamp(5L, eightDaysAgo));
        assertTrue(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldResetCutoffAcrossCycles() {
        FakeClock movingClock = new FakeClock(1000, MILLISECONDS);
        EntryTimespanThreshold threshold = new EntryTimespanThreshold(logProvider, movingClock, MILLISECONDS, 100);
        // t=1000, cutoff=900; previous ts=950 → not older → keep.
        PrunePredicate first = threshold.forCycle(0L);
        first.isLowestVersionToKeep(withTimestamp(5L, 950L));
        assertFalse(first.isLowestVersionToKeep(file(4L)));

        movingClock.forward(java.time.Duration.ofMillis(200));
        // t=1200, cutoff=1100; previous ts=950 → older → prune.
        PrunePredicate second = threshold.forCycle(0L);
        second.isLowestVersionToKeep(withTimestamp(5L, 950L));
        assertTrue(second.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldNotReachWhenNeitherTimeNorSizeExceeded() throws IOException {
        when(fs.getFileSize(java.nio.file.Path.of("test-v4"))).thenReturn(64L);
        PrunePredicate predicate = new EntryTimespanThreshold(
                        logProvider, clock, MILLISECONDS, 200, new FileSizeThreshold(fs, 128, logProvider))
                .forCycle(0L);
        predicate.isLowestVersionToKeep(withTimestamp(5L, 900L));
        assertFalse(predicate.isLowestVersionToKeep(file(4L)));
    }

    @Test
    void shouldReachOnSizeRestrictionEvenWhenTimeIsWithinWindow() throws IOException {
        when(fs.getFileSize(java.nio.file.Path.of("test-v4"))).thenReturn(128L);
        PrunePredicate predicate = new EntryTimespanThreshold(
                        logProvider, clock, MILLISECONDS, 200, new FileSizeThreshold(fs, 64, logProvider))
                .forCycle(0L);
        assertTrue(predicate.isLowestVersionToKeep(file(4L)));
    }

    private static LogFileInformation file(long version) {
        return new TestLogFileInformation(version) {};
    }

    private static LogFileInformation withTimestamp(long version, long ts) {
        return new TestLogFileInformation(version) {
            @Override
            public long getFirstStartRecordTimestamp() {
                return ts;
            }
        };
    }

    private static LogFileInformation throwingTimestamp(long version, IOException toThrow) {
        return new TestLogFileInformation(version) {
            @Override
            public long getFirstStartRecordTimestamp() throws IOException {
                throw toThrow;
            }
        };
    }
}
