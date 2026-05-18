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
import static org.neo4j.kernel.impl.transaction.log.enveloped.PruneStrategy.ALWAYS_PRUNE;
import static org.neo4j.kernel.impl.transaction.log.enveloped.PruneStrategy.NEVER_PRUNE;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.impl.transaction.log.enveloped.PruneStrategy.PruneConstraint;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class LogFilesPrunerTest {
    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private final PruneStrategy pruneOnTwo =
            (long currentEntry, long currentOffset, Path currentLogFile) -> new PruneConstraint() {
                int count = 0;

                @Override
                public boolean shouldPrune(Path path) {
                    return ++count >= 2;
                }
            };
    private LogsRepository logsRepository;

    @BeforeEach
    void setUp() throws IOException {
        logsRepository = new LogsRepository(fs, new SequentialFileNameHelper(testDirectory.homePath(), "test"));
        logsRepository.initialise();
    }

    @Test
    void shouldNotExceedDesiredVersion() throws IOException {
        createFile(0);
        createFile(1);

        var prunedVersion = new LogFilesPruner(logsRepository, ALWAYS_PRUNE).pruneUpTo(0, 0, 0, 0);

        assertThat(prunedVersion).isZero();
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(1, 1));
    }

    @Test
    void shouldNotPruneIfDesiredVersionIsBelow() throws IOException {
        createFile(3);
        createFile(4);

        var prunedVersion = new LogFilesPruner(logsRepository, ALWAYS_PRUNE).pruneUpTo(2, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 4));
    }

    @Test
    void shouldNeverPruneLastFileEvenIfConstraintAllowsIt() throws IOException {
        createFile(3);
        createFile(4);

        var prunedVersion = new LogFilesPruner(logsRepository, ALWAYS_PRUNE).pruneUpTo(5, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(3L);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(4, 4));
    }

    @Test
    void shouldNotPruneIfStrategyDoesNotAllowIt() throws IOException {
        createFile(3);
        createFile(4);

        var prunedVersion = new LogFilesPruner(logsRepository, NEVER_PRUNE).pruneUpTo(2, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 4));
    }

    @Test
    void shouldOnlyPruneIfStrategyAllowsIt() throws IOException {
        createFile(1);
        createFile(2);
        createFile(3);
        createFile(4);

        var prunedVersion = new LogFilesPruner(logsRepository, pruneOnTwo).pruneUpTo(3, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(2);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 4));
    }

    @Test
    void shouldCallConstraintEventWhenDesiredVersionIsNotMet() throws IOException {
        createFile(1);
        createFile(2);
        createFile(3);
        createFile(4);
        createFile(5);
        createFile(6);

        var prunedVersion = new LogFilesPruner(logsRepository, pruneOnTwo).pruneUpTo(3, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(3);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(4, 6));
    }

    @Test
    void shouldPruneDesiredVersion() throws IOException {
        createFile(1);
        createFile(2);
        createFile(3);
        createFile(4);

        var prunedVersion = new LogFilesPruner(logsRepository, ALWAYS_PRUNE).pruneUpTo(2, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(2);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(3, 4));
    }

    @Test
    void shouldHandleEmptyRepository() throws IOException {
        var prunedVersion = new LogFilesPruner(logsRepository, ALWAYS_PRUNE).pruneUpTo(0, 0, 0, 0);

        assertThat(prunedVersion).isEqualTo(-1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.EMPTY_RANGE);
    }

    private void createFile(long version) throws IOException {
        try (LogChannelContext<StoreChannel> ignored = logsRepository.createWriteChannel(version)) {}
    }
}
