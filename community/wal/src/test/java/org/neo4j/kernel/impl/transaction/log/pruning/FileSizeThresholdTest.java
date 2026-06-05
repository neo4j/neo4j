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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.InternalLogProvider;

class FileSizeThresholdTest {
    private final FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
    private final InternalLogProvider logProvider = mock(InternalLogProvider.class);

    @Test
    void shouldReturnFalseWhenFileSizeIsLowerThanMaxSize() throws IOException {
        when(fs.getFileSize(pathFor(1L))).thenReturn(5L);

        PrunePredicate predicate = new FileSizeThreshold(fs, 10, logProvider).forCycle(0);
        assertFalse(predicate.isLowestVersionToKeep(file(1L)));
    }

    @Test
    void shouldReturnTrueWhenASingleFileSizeIsGreaterOrEqualThanMaxSize() throws IOException {
        long sixteenGigabytes = 16L * 1024 * 1024 * 1024;
        when(fs.getFileSize(pathFor(1L))).thenReturn(sixteenGigabytes);

        PrunePredicate predicate = new FileSizeThreshold(fs, sixteenGigabytes, logProvider).forCycle(0);
        assertTrue(predicate.isLowestVersionToKeep(file(1L)));
    }

    @Test
    void shouldSumSizeWhenCalledMultipleTimes() throws IOException {
        when(fs.getFileSize(pathFor(1L))).thenReturn(5L);
        when(fs.getFileSize(pathFor(2L))).thenReturn(5L);

        PrunePredicate predicate = new FileSizeThreshold(fs, 10, logProvider).forCycle(0);
        predicate.isLowestVersionToKeep(file(1L));
        assertTrue(predicate.isLowestVersionToKeep(file(2L)));
    }

    @Test
    void shouldForgetPreviousValuesAcrossCycles() throws IOException {
        when(fs.getFileSize(pathFor(1L))).thenReturn(5L);
        when(fs.getFileSize(pathFor(2L))).thenReturn(5L);

        FileSizeThreshold threshold = new FileSizeThreshold(fs, 10, logProvider);
        PrunePredicate first = threshold.forCycle(0);
        first.isLowestVersionToKeep(file(1L));
        first.isLowestVersionToKeep(file(2L));

        PrunePredicate second = threshold.forCycle(0);
        assertFalse(second.isLowestVersionToKeep(file(1L)));
    }

    private static TestLogFileInformation file(long version) {
        return new TestLogFileInformation(version) {};
    }

    private static java.nio.file.Path pathFor(long version) {
        return java.nio.file.Path.of("test-v" + version);
    }
}
