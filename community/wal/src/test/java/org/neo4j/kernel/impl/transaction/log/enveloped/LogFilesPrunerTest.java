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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory.KEEP_ALL;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory.PRUNE_ALL;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.PrunePredicate;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class LogFilesPrunerTest {
    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private LogsRepository logsRepository;

    @BeforeEach
    void setUp() throws IOException {
        logsRepository = new LogsRepository(fs, new SequentialFileNameHelper(testDirectory.homePath(), "test"));
        logsRepository.initialise();
    }

    @Test
    void shouldReturnMinusOneOnEmptyRepository() throws IOException {
        long pruned = new LogFilesPruner(logsRepository, PRUNE_ALL).pruneUpTo(0, 0);

        assertThat(pruned).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.EMPTY_RANGE);
    }

    @Test
    void shouldNotPruneIfThresholdNeverReached() throws IOException {
        createFileWithHeader(3, 0);
        createFileWithHeader(4, 0);
        createFileWithHeader(5, 0);

        long pruned = new LogFilesPruner(logsRepository, KEEP_ALL).pruneUpTo(4, 0);

        assertThat(pruned).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 5));
    }

    @Test
    void shouldPruneEverythingBelowReachedFile() throws IOException {
        createFileWithHeader(1, 0);
        createFileWithHeader(2, 5);
        createFileWithHeader(3, 10);
        createFileWithHeader(4, 15);

        // desired=3 → iteration capped at upToVersion=4 (i.e. include the tail). PRUNE_ALL hits v=4 first;
        // boundary=4, prune everything strictly older within the caller's willing range: files 1, 2, 3.
        long pruned = new LogFilesPruner(logsRepository, PRUNE_ALL).pruneUpTo(3, 0);

        assertThat(pruned).isEqualTo(3);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(4, 4));
    }

    @Test
    void shouldNotPruneWhenDesiredVersionIsBelowLowest() throws IOException {
        createFileWithHeader(3, 0);
        createFileWithHeader(4, 5);

        // desired=2, upToVersion=3; iterate v=3 (≤ 3, considered). PRUNE_ALL → boundary=3, highestToPrune=2 <
        // lowest=3 → no prune.
        long pruned = new LogFilesPruner(logsRepository, PRUNE_ALL).pruneUpTo(2, 0);

        assertThat(pruned).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 4));
    }

    @Test
    void shouldNeverPruneLastFileEvenWhenDesiredVersionExceedsHighest() throws IOException {
        createFileWithHeader(3, 0);
        createFileWithHeader(4, 5);

        // Caller asks to prune up to v=5, but v=5 doesn't exist. Boundary lands on v=4
        // (newest existing); we still keep it and only delete strictly older.
        long pruned = new LogFilesPruner(logsRepository, PRUNE_ALL).pruneUpTo(5, 0);

        assertThat(pruned).isEqualTo(3);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(4, 4));
    }

    @Test
    void shouldStopAtFirstReachedBoundary() throws IOException {
        createFileWithHeader(1, 0);
        createFileWithHeader(2, 5);
        createFileWithHeader(3, 10);
        createFileWithHeader(4, 15);
        createFileWithHeader(5, 20);

        // desired=3 → upToVersion=4. Predicate sees every file (newest-first, including v=5 even though it's
        // above the prune horizon). Reaches on the 3rd call: v=5, v=4, v=3 → boundary at v=3, prune files 1, 2.
        long pruned = new LogFilesPruner(logsRepository, reachedOnNthCall(3)).pruneUpTo(3, 0);

        assertThat(pruned).isEqualTo(2);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 5));
    }

    private void createFileWithHeader(long version, long prevAppendIndex) throws IOException {
        try (LogChannelContext<StoreChannel> channel = logsRepository.createWriteChannel(version)) {
            LogFormat.writeLogHeader(
                    channel.channel(),
                    LogFormat.V11.newHeader(
                            version,
                            prevAppendIndex,
                            0,
                            StoreIdentifier.UNKNOWN,
                            246,
                            1,
                            KernelVersion.V2026_01,
                            UNSPECIFIED_CREATION_TIME),
                    EmptyMemoryTracker.INSTANCE);
            channel.channel().flush();
        }
    }

    private static LogPruneThreshold reachedOnNthCall(int targetCall) {
        return state -> new PrunePredicate() {
            private int count;

            @Override
            public boolean isLowestVersionToKeep(LogFileInformation current) {
                return ++count >= targetCall;
            }
        };
    }
}
