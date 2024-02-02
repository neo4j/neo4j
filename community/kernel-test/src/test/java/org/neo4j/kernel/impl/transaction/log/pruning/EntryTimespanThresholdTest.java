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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

class EntryTimespanThresholdTest {
    private FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
    private final Path file = mock(Path.class);
    private final LogFileInformation source = mock(LogFileInformation.class);
    private final long version = 4;
    private final SystemNanoClock clock = new FakeClock(1000, MILLISECONDS);
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void shouldReturnFalseWhenTimeIsEqualOrAfterTheLowerLimit() throws IOException {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 200);

        when(source.getFirstStartRecordTimestamp(version + 1)).thenReturn(800L);

        // when
        threshold.init();
        final boolean result = threshold.reached(file, version, source);

        // then
        assertFalse(result);
    }

    @Test
    void shouldReturnTrueWhenTimeIsBeforeTheLowerLimit() throws IOException {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 100);

        when(source.getFirstStartRecordTimestamp(version + 1)).thenReturn(800L);

        // when
        threshold.init();
        final boolean result = threshold.reached(file, version, source);

        // then
        assertTrue(result);
    }

    @Test
    void thresholdNotReachedWhenTheLogCannotBeRead() throws IOException {
        // given
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold(logProvider, clock, MILLISECONDS, 100);

        final IOException ex = new IOException();
        when(source.getFirstStartRecordTimestamp(version + 1)).thenThrow(ex);

        // when
        threshold.init();
        assertFalse(threshold.reached(file, version, source));
        assertThat(logProvider).containsMessages("Fail to get timestamp info from transaction log file");
    }

    @Test
    void thresholdReachedWhenFileRestrictionIsReached() throws IOException {
        final EntryTimespanThreshold threshold = new EntryTimespanThreshold(
                logProvider, clock, MILLISECONDS, 200, new FileSizeThreshold(fs, 128, logProvider));
        when(source.getFirstStartRecordTimestamp(version + 1)).thenReturn(800L);
        when(fs.getFileSize(file)).thenReturn(129L);

        // when
        threshold.init();
        assertTrue(threshold.reached(file, version, source));
    }
}
