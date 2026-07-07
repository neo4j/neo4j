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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.pruning.EntryTimespanThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.FakeClock;

/**
 * Integration between {@link LogFilesPruner} and {@link EntryTimespanThreshold} using enveloped log files.
 *
 * <p>{@link EphemeralFileSystemAbstraction} tracks each file's last-modified time via the shared {@link FakeClock},
 * so advancing the clock between file writes controls the mtime that
 * {@link EnvelopedLogFileInformation#getFirstStartRecordTimestamp()} returns to the threshold predicate.
 */
@TestDirectoryExtension
class LogFilesPrunerTimespanThresholdTest {

    @Inject
    TestDirectory testDirectory;

    private final FakeClock clock = new FakeClock();
    private final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction(clock);
    private LogsRepository logsRepository;

    @BeforeEach
    void setUp() throws IOException {
        logsRepository = new LogsRepository(fs, new SequentialFileNameHelper(testDirectory.homePath(), "test"));
        logsRepository.initialise();
    }

    @AfterEach
    void tearDown() throws IOException {
        fs.close();
    }

    @Test
    void shouldPruneByLastModifiedTimeWhenNoTimestampInHeader() throws IOException {
        createFileWithHeader(1);
        createFileWithHeader(2);
        createFileWithHeader(3);

        clock.forward(Duration.ofMinutes(90));
        createFileWithHeader(4);

        clock.forward(Duration.ofMinutes(30)); // clock is now at t=2h

        var threshold = ThresholdFactory.fromConfigValue(fs, NullLogProvider.getInstance(), clock, "1 hours");
        long pruned = new LogFilesPruner(logsRepository, threshold).pruneUpTo(4, 0);

        assertThat(pruned).isEqualTo(1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(2, 4));
    }

    @Test
    void shouldPruneByTimestampWhenInInHeader() throws IOException {
        createFileWithHeader(1, TimeUnit.MINUTES.toMillis(5));
        createFileWithHeader(2, TimeUnit.MINUTES.toMillis(10));
        createFileWithHeader(3, TimeUnit.MINUTES.toMillis(15));
        createFileWithHeader(4, TimeUnit.MINUTES.toMillis(90));

        clock.forward(Duration.ofMinutes(120)); // If timestamps weren't provided then the boundary would be version 3

        var threshold = ThresholdFactory.fromConfigValue(fs, NullLogProvider.getInstance(), clock, "1 hours");
        long pruned = new LogFilesPruner(logsRepository, threshold).pruneUpTo(4, 0);

        assertThat(pruned).isEqualTo(1);
        assertThat(logsRepository.logVersionsRange()).isEqualTo(LongRange.range(2, 4));
    }

    private void createFileWithHeader(long version) throws IOException {
        createFileWithHeader(version, UNSPECIFIED_CREATION_TIME);
    }

    private void createFileWithHeader(long version, long timestamp) throws IOException {
        try (LogChannelContext<StoreChannel> channel = logsRepository.createWriteChannel(version)) {
            LogFormat.writeLogHeader(
                    channel.channel(),
                    LogFormat.V11.newHeader(
                            version, (long) 0, 0, StoreIdentifier.UNKNOWN, 246, 1, KernelVersion.V2026_01, timestamp),
                    EmptyMemoryTracker.INSTANCE);
            channel.channel().flush();
        }
    }
}
