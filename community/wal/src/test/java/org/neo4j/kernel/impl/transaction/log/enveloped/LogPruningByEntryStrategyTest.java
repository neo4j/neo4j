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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.enveloped.PruneStrategy.PruneConstraint;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogPruningByEntryStrategyTest {

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
    void shouldNotPruneIfEntriesIsSmaller() throws IOException {
        createFileWithHeader(0, -1);
        createFileWithHeader(1, 9);

        var pruneConstraint = new LogPruningByEntryStrategy(fs, 11).newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldNotBeStateful() throws IOException {
        createFileWithHeader(0, -1);
        createFileWithHeader(1, 9);

        var strategy = new LogPruningByEntryStrategy(fs, 12);
        var pruneConstraint = strategy.newConstraint(9, 0, logsRepository.pathFor(1));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);

        pruneConstraint = strategy.newConstraint(0, 0, logsRepository.pathFor(0));
        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldNotPruneIfTotalEntriesIsSameAsConstraint() throws IOException {
        createFileWithHeader(0, 10);
        createFileWithHeader(1, 11);
        createFileWithHeader(2, 15);

        var strategy = new LogPruningByEntryStrategy(fs, 5);
        var pruneConstraint = strategy.newConstraint(15, 0, logsRepository.pathFor(2));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldPruneIfCurrentEntryIsIncluded() throws IOException {
        createFileWithHeader(0, 10);
        createFileWithHeader(1, 11);
        createFileWithHeader(2, 15);

        var strategy = new LogPruningByEntryStrategy(fs, 5);
        var pruneConstraint = strategy.newConstraint(16, 0, logsRepository.pathFor(2));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(0L);
    }

    @Test
    void shouldNotPruneTheFileThatExceedsTheConstraint() throws IOException {
        createFileWithHeader(0, 3);
        createFileWithHeader(1, 9);
        createFileWithHeader(2, 15);

        var strategy = new LogPruningByEntryStrategy(fs, 4);
        var pruneConstraint = strategy.newConstraint(15, 0, logsRepository.pathFor(2));

        assertThat(checkConstraint(pruneConstraint)).isZero();
    }

    @Test
    void shouldPruneIfEnoughEntriesInFullFile() throws IOException {
        createFileWithHeader(0, 4);
        createFileWithHeader(1, 256);
        createFileWithHeader(2, 512);

        var strategy = new LogPruningByEntryStrategy(fs, 256);
        var pruneConstraint = strategy.newConstraint(512, 0, logsRepository.pathFor(2));

        assertThat(checkConstraint(pruneConstraint)).isZero();
    }

    private void createFileWithHeader(long version, int prevIndex) throws IOException {
        try (LogChannelContext<StoreChannel> channel = logsRepository.createWriteChannel(version)) {

            LogFormat.writeLogHeader(
                    channel.channel(),
                    LogFormat.V11.newHeader(
                            version, prevIndex, 0, StoreIdentifier.UNKNOWN, 246, 1, KernelVersion.V2026_01),
                    EmptyMemoryTracker.INSTANCE);
            channel.channel().flush();
        }
    }

    private long checkConstraint(PruneConstraint pruneConstraint) throws IOException {
        var versions = logsRepository.logVersions(true);
        for (var v : versions) {
            if (pruneConstraint.shouldPrune(logsRepository.pathFor(v))) {
                return v;
            }
        }
        return -1;
    }
}
