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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

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
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class VersionAwareLogEntryReaderIT {
    // this offset includes log header and transaction that create node on test setup
    // Magic number represents number of bytes that log file is actually using (in form of header size + payload)
    // to be able to check that its like that or to update manually you can disable pre-allocation + some manual checks.
    private static final long AT_LEAST_END_OF_DATA_OFFSET = LATEST_LOG_FORMAT.getHeaderSize() + 1_000;
    private static final long AT_MOST_END_OF_DATA_OFFSET = kibiBytes(128);
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
                storageEngineFactory.commandReaderFactory(), LatestVersions.BINARY_VERSIONS);
        managementService.shutdown();
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    void readOnlyLogFilesWhileCommandsAreAvailable() throws IOException {
        LogFiles logFiles = LogFilesBuilder.builder(databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(logFiles)) {
            getLastReadablePosition(logFiles);
            assertEquals(kibiBytes(256), Files.size(logFiles.getLogFile().getHighestLogFile()));
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals(0L, logPosition.getLogVersion());
            // this position in a log file before 0's are actually starting
            assertThat(logPosition.getByteOffset()).isGreaterThanOrEqualTo(AT_LEAST_END_OF_DATA_OFFSET);
            assertThat(logPosition.getByteOffset()).isLessThan(AT_MOST_END_OF_DATA_OFFSET);
        }
    }

    @Test
    void correctlyResetPositionWhenEndOfCommandsReached() throws IOException {
        LogFiles logFiles = LogFilesBuilder.builder(databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(logFiles)) {
            long offset = 0;
            for (int i = 0; i < 10; i++) {
                var lastReadablePosition = getLastReadablePosition(logFiles);
                assertThat(lastReadablePosition).isGreaterThanOrEqualTo(1_000);
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
        LogFiles logFiles = LogFilesBuilder.builder(databaseLayout, fs, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withStorageEngineFactory(storageEngineFactory)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withAppendIndexProvider(new SimpleAppendIndexProvider())
                .withStoreId(STORE_ID)
                .build();
        try (Lifespan lifespan = new Lifespan(logFiles)) {
            getLastReadablePosition(logFiles);
            LogPosition logPosition = entryReader.lastPosition();
            assertEquals(0L, logPosition.getLogVersion());
            assertEquals(Files.size(logFiles.getLogFile().getHighestLogFile()), logPosition.getByteOffset());
        }
    }

    private long getLastReadablePosition(LogFiles logFiles) throws IOException {
        var logFile = logFiles.getLogFile();
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
