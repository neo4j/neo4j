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
package org.neo4j.wal.files;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.monitoring.HealthEventGenerator.NO_OP;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.extension.SkipOnSpd.Note.incompatible;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.wal.CompleteCommandBatch;
import org.neo4j.wal.LogAppendEvent;
import org.neo4j.wal.WALService;
import org.neo4j.wal.rotation.FileLogRotation;
import org.neo4j.wal.rotation.LogRotation;
import org.neo4j.wal.rotation.monitor.LogRotationMonitorAdapter;

@DbmsExtension
@ExtendWith(LifeExtension.class)
class TransactionLogFileIT {

    private static final StoreId STORE_ID = new StoreId(1, 2, "engine-1", "format-1", 3, 4);

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    @Inject
    private LogVersionRepository logVersionRepository;

    @Inject
    private LogMetadataProvider metadataProvider;

    @Test
    @EnabledOnOs(OS.LINUX)
    @SkipOnSpd(notes = incompatible, reason = "Deleting tx logs manually may cause property shard tx pulling to fail.")
    void doNotScanDirectoryOnRotate() throws IOException {
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withTransactionIdStore(metadataProvider)
                .withAppendIndexProvider(metadataProvider)
                .withLogVersionRepository(logVersionRepository)
                .withStoreId(STORE_ID)
                .build();
        life.add(walService);
        life.start();

        MutableLong rotationObservedVersion = new MutableLong();
        LogRotation logRotation = FileLogRotation.transactionLogRotation(
                walService.getLogFile(),
                Clock.systemUTC(),
                new DatabaseHealth(NO_OP, NullLog.getInstance()),
                new LogRotationMonitorAdapter() {
                    @Override
                    public void startRotation(LogType type, long currentLogVersion) {
                        rotationObservedVersion.setValue(currentLogVersion);
                    }
                },
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER);

        for (int i = 0; i < 6; i++) {
            for (Path path : walService.logFiles()) {
                FileUtils.deleteFile(path);
            }
            logRotation.rotateLogFile(LogAppendEvent.NULL);
        }

        assertEquals(5, rotationObservedVersion.getValue());
        assertEquals(6, walService.getLogFile().getCurrentLogVersion());
    }

    @Test
    void rotateUsesCorrectAppendIndexAndChecksum() throws IOException {
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withTransactionIdStore(metadataProvider)
                .withAppendIndexProvider(metadataProvider)
                .withLogVersionRepository(logVersionRepository)
                .withStoreId(STORE_ID)
                .build();
        life.add(walService);
        life.start();

        var appendIndex = metadataProvider.nextAppendIndex();
        var writer = walService.getLogFile().getTransactionLogWriter();
        var simpleTransaction = new CompleteCommandBatch(
                singletonList(new TestCommand(LatestVersions.LATEST_KERNEL_VERSION)),
                UNKNOWN_CONSENSUS_INDEX,
                -1,
                -1,
                -1,
                -1,
                Leases.NO_LEASES,
                LatestVersions.LATEST_KERNEL_VERSION,
                AUTH_DISABLED);
        var checksum = writer.append(
                simpleTransaction,
                appendIndex,
                UNKNOWN_CHUNK_ID,
                appendIndex,
                -1,
                UNKNOWN_APPEND_INDEX,
                LogAppendEvent.NULL);

        MutableLong rotationObservedVersion = new MutableLong();
        LogRotation logRotation = FileLogRotation.transactionLogRotation(
                walService.getLogFile(),
                Clock.systemUTC(),
                new DatabaseHealth(NO_OP, NullLog.getInstance()),
                new LogRotationMonitorAdapter() {
                    @Override
                    public void startRotation(LogType type, long currentLogVersion) {
                        rotationObservedVersion.setValue(currentLogVersion);
                    }
                },
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER);
        // rotate without explicit appendIndex should use metadataProvider for lastAppendIndex
        // and also match on checksum if available
        logRotation.rotateLogFile(LogAppendEvent.NULL);
        var header =
                walService.getLogFile().extractHeader(walService.getLogFile().getCurrentLogVersion());
        assertEquals(metadataProvider.getLastAppendIndex(), header.getLastAppendIndex());
        if (header.getLogFormatVersion().usesSegments()) { // no checksum before envelopes
            assertEquals(checksum, header.getPreviousLogFileChecksum());
        }
        long testAppendIndex = metadataProvider.getLastAppendIndex() + 101;
        int testCRC = 789;
        // rotate with explicit appendIndex should respect passed in value
        logRotation.locklessRotateLogFile(LogAppendEvent.NULL, testAppendIndex, testCRC, -1);
        var header2 =
                walService.getLogFile().extractHeader(walService.getLogFile().getCurrentLogVersion());
        assertEquals(testAppendIndex, header2.getLastAppendIndex());
        if (header2.getLogFormatVersion().usesSegments()) { // no checksum before envelopes
            assertEquals(testCRC, header2.getPreviousLogFileChecksum());
        }
    }

    @Test
    void trackTransactionLogFileMemory() throws IOException {
        var memoryTracker = new LocalMemoryTracker();
        var life = new LifeSupport();
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withTransactionIdStore(metadataProvider)
                .withLogVersionRepository(logVersionRepository)
                .withAppendIndexProvider(metadataProvider)
                .withStoreId(STORE_ID)
                .withMemoryTracker(memoryTracker)
                .build();

        life.add(walService);
        try {
            life.start();

            assertThat(memoryTracker.estimatedHeapMemory()).isZero();
            assertThat(memoryTracker.usedNativeMemory()).isGreaterThan(0);
        } finally {
            life.stop();
            life.shutdown();
        }

        assertThat(memoryTracker.usedNativeMemory()).isZero();
        assertThat(memoryTracker.estimatedHeapMemory()).isZero();
    }
}
