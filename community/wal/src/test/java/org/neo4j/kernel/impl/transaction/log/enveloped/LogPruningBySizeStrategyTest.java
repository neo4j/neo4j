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
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.impl.transaction.log.enveloped.PruneStrategy.PruneConstraint;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class LogPruningBySizeStrategyTest {

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private LogsRepository logsRepository;
    private final byte[] data = new byte[8];

    @BeforeEach
    void setUp() throws IOException {
        logsRepository = new LogsRepository(fs, new SequentialFileNameHelper(testDirectory.homePath(), "test"));
        logsRepository.initialise();
    }

    @Test
    void shouldNotPruneIfTotalSizeIsSmaller() throws IOException {
        createFileWithData(0);
        createFileWithData(1);

        var pruneConstraint = new LogPruningBySizeStrategy(fs, 17).newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldNotBeStateful() throws IOException {
        createFileWithData(0);
        createFileWithData(1);

        var logPruningBySizeStrategy = new LogPruningBySizeStrategy(fs, 19);
        var pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);

        pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 0, logsRepository.pathFor(0));
        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldNotPruneIfTotalSizeIsSame() throws IOException {
        createFileWithData(0);
        createFileWithData(1);
        createFileWithData(2);

        var logPruningBySizeStrategy = new LogPruningBySizeStrategy(fs, 24);
        var pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isEqualTo(-1);
    }

    @Test
    void shouldNotPruneTheFileThatExceedsTheTotalSize() throws IOException {
        createFileWithData(0);
        createFileWithData(1);
        createFileWithData(2);

        var logPruningBySizeStrategy = new LogPruningBySizeStrategy(fs, 12);
        var pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isZero();
    }

    @Test
    void shouldPruneIfOffsetIsIncluded() throws IOException {
        createFileWithData(0);
        createFileWithData(1);
        createFileWithData(2);

        var logPruningBySizeStrategy = new LogPruningBySizeStrategy(fs, 18);
        var pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 2, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isZero();
    }

    @Test
    void shouldPruneIfFullFileIsLargerThanTotalSize() throws IOException {
        createFileWithData(0);
        createFileWithData(1);
        createFileWithData(2);

        var logPruningBySizeStrategy = new LogPruningBySizeStrategy(fs, 16);
        var pruneConstraint = logPruningBySizeStrategy.newConstraint(0, 0, logsRepository.pathFor(0));

        assertThat(checkConstraint(pruneConstraint)).isZero();
    }

    private void createFileWithData(long version) throws IOException {
        try (LogChannelContext<StoreChannel> channel = logsRepository.createWriteChannel(version)) {
            channel.channel().write(ByteBuffer.wrap(data));
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
