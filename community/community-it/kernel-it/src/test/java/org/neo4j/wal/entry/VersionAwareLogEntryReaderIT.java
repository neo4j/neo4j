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
package org.neo4j.wal.entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT_PROVIDER;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.ReadableLogChannel;
import org.neo4j.wal.WALService;
import org.neo4j.wal.files.LogFilesBuilder;

@DbmsExtension
class VersionAwareLogEntryReaderIT {
    // this offset includes log header and transaction that create node on test setup
    // Magic number represents number of bytes that log file is actually using (in form of header size + payload)
    // to be able to check that its like that or to update manually you can disable pre-allocation + some manual checks.
    private static final long HEADER_SIZE = LATEST_LOG_FORMAT.getDefaultDataStartByteOffset();
    private static final long AT_LEAST_END_OF_DATA_OFFSET = HEADER_SIZE + 1_000;
    private static final long AT_MOST_END_OF_DATA_OFFSET = HEADER_SIZE + kibiBytes(128);
    private static final StoreId STORE_ID = new StoreId(4, 5, "engine-1", "format-1", 1, 2);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseManagementService managementService;

    private DatabaseLayout databaseLayout;
    private VersionAwareLogEntryReader entryReader;
    private StorageEngineFactory storageEngineFactory;

    @BeforeEach
    void setUp() {
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        createNode(database);
        databaseLayout = database.databaseLayout();
        storageEngineFactory = database.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        entryReader = new VersionAwareLogEntryReader(
                storageEngineFactory.commandReaderFactory(),
                LatestVersions.BINARY_VERSIONS,
                EmptyMemoryTracker.INSTANCE);
        managementService.shutdown();
    }

    @SkipOnSpd(
            reason = "SPD doesn't seem to pre-allocated tx logs. Potentially this is true for clustering too. "
                    + "It seems to be because at the time of initially creating the tx log file the member is in some form of read-only state. "
                    + "See LogFilesBuilder#getTryToPreallocateTransactionLogs()")
    @Test
    @EnabledOnOs(OS.LINUX)
    void readOnlyLogFilesWhileCommandsAreAvailable() throws IOException {
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fs,
                        LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                        LatestVersions.LATEST_LOG_FORMAT_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(walService)) {
            getLastReadablePosition(walService);
            assertEquals(
                    kibiBytes(256),
                    Files.size(walService.getLogFile().getLogRangeInfo().highestFile()));
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals(0L, logPosition.getLogVersion());
            // this position in a log file before 0's are actually starting
            assertThat(logPosition.getByteOffset()).isGreaterThanOrEqualTo(AT_LEAST_END_OF_DATA_OFFSET);
            assertThat(logPosition.getByteOffset()).isLessThan(AT_MOST_END_OF_DATA_OFFSET);
        }
    }

    @Test
    void correctlyResetPositionWhenEndOfCommandsReached() throws IOException {
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER, LATEST_LOG_FORMAT_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(walService)) {
            long offset = 0;
            for (int i = 0; i < 10; i++) {
                var lastReadablePosition = getLastReadablePosition(walService);
                assertThat(lastReadablePosition).isGreaterThanOrEqualTo(300);
                assertThat(lastReadablePosition).isLessThan(AT_MOST_END_OF_DATA_OFFSET);
                if (i > 0) {
                    assertThat(lastReadablePosition).isEqualTo(offset);
                }
                offset = lastReadablePosition;
            }
        }
    }

    @Test
    @DisabledOnOs(OS.LINUX)
    void readTillTheEndOfNotPreallocatedFile() throws IOException {
        WALService walService = LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER, LATEST_LOG_FORMAT_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(walService)) {
            getLastReadablePosition(walService);
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals(0L, logPosition.getLogVersion());
            assertEquals(
                    Files.size(walService.getLogFile().getLogRangeInfo().highestFile()), logPosition.getByteOffset());
        }
    }

    private long getLastReadablePosition(WALService walService) throws IOException {
        var logFile = walService.getLogFile();
        try (ReadableLogChannel logChannel =
                logFile.getReader(logFile.extractHeader(0).getStartPosition())) {
            while (entryReader.readLogEntry(logChannel) != null) {
                // read to the end
            }
            return entryReader.lastPosition().getByteOffset();
        }
    }

    private static void createNode(GraphDatabaseService database) {
        try (Transaction transaction = database.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
    }
}
