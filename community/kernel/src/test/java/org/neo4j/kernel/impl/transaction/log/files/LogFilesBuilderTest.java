/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.activeFilesBuilder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.builder;
import static org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder.logFilesBasedOnlyBuilder;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.PanicEventGenerator;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@Neo4jLayoutExtension
class LogFilesBuilderTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private Path storeDirectory;

    @BeforeEach
    void setUp() {
        storeDirectory = testDirectory.homePath();
    }

    @Test
    void buildActiveFilesOnlyContext() throws IOException {
        TransactionLogFilesContext context = activeFilesBuilder(databaseLayout, fileSystem, pageCache)
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .buildContext();

        assertEquals(fileSystem, context.getFileSystem());
        assertNotNull(context.getCommandReaderFactory());
        assertEquals(Long.MAX_VALUE, context.getRotationThreshold().get());
        assertEquals(
                TransactionIdStore.BASE_TX_ID,
                context.getLastCommittedTransactionIdProvider().getLastCommittedTransactionId(null));
        assertEquals(
                0,
                context.getLogVersionRepositoryProvider()
                        .logVersionRepository(null)
                        .getCurrentLogVersion());
    }

    @Test
    void buildFilesBasedContext() throws IOException {
        TransactionLogFilesContext context = logFilesBasedOnlyBuilder(storeDirectory, fileSystem)
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .buildContext();
        assertEquals(fileSystem, context.getFileSystem());
    }

    @Test
    void buildDefaultContext() throws IOException {
        TransactionLogFilesContext context = builder(databaseLayout, fileSystem)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .buildContext();
        assertEquals(fileSystem, context.getFileSystem());
        assertNotNull(context.getCommandReaderFactory());
        assertEquals(ByteUnit.mebiBytes(250), context.getRotationThreshold().get());
        assertEquals(1, context.getLastCommittedTransactionIdProvider().getLastCommittedTransactionId(null));
        assertEquals(
                2,
                context.getLogVersionRepositoryProvider()
                        .logVersionRepository(null)
                        .getCurrentLogVersion());
    }

    @Test
    void buildContextWithRotationThreshold() throws IOException {
        TransactionLogFilesContext context = builder(databaseLayout, fileSystem)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .buildContext();
        assertEquals(fileSystem, context.getFileSystem());
        assertNotNull(context.getCommandReaderFactory());
        assertEquals(ByteUnit.mebiBytes(1), context.getRotationThreshold().get());
        assertEquals(1, context.getLastCommittedTransactionIdProvider().getLastCommittedTransactionId(null));
        assertEquals(
                2,
                context.getLogVersionRepositoryProvider()
                        .logVersionRepository(null)
                        .getCurrentLogVersion());
    }

    @Test
    void buildDefaultContextWithDependencies() throws IOException {
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository(2);
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        DatabaseHealth databaseHealth = new DatabaseHealth(PanicEventGenerator.NO_OP, NullLog.getInstance());
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency(logVersionRepository);
        dependencies.satisfyDependency(transactionIdStore);
        dependencies.satisfyDependency(databaseHealth);

        TransactionLogFilesContext context = builder(databaseLayout, fileSystem)
                .withDependencies(dependencies)
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .buildContext();

        assertEquals(fileSystem, context.getFileSystem());
        assertNotNull(context.getCommandReaderFactory());
        assertEquals(ByteUnit.mebiBytes(250), context.getRotationThreshold().get());
        assertEquals(databaseHealth, context.getDatabaseHealth());
        assertEquals(1, context.getLastCommittedTransactionIdProvider().getLastCommittedTransactionId(null));
        assertEquals(
                2,
                context.getLogVersionRepositoryProvider()
                        .logVersionRepository(null)
                        .getCurrentLogVersion());
    }

    @Test
    void buildContextWithCustomAbsoluteLogFilesLocations() throws Throwable {
        Path customLogDirectory = testDirectory.directory("absoluteCustomLogDirectory");
        Config config = Config.newBuilder()
                .set(neo4j_home, testDirectory.homePath())
                .set(transaction_logs_root_path, customLogDirectory.toAbsolutePath())
                .build();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        LogFiles logFiles = builder(DatabaseLayout.of(config), fileSystem)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withLogVersionRepository(new SimpleLogVersionRepository())
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .withCommandReaderFactory(CommandReaderFactory.NO_COMMANDS)
                .withStoreId(storeId)
                .build();
        logFiles.init();
        logFiles.start();

        assertEquals(
                customLogDirectory.resolve(databaseLayout.getDatabaseName()),
                logFiles.getLogFile().getHighestLogFile().getParent());
        logFiles.shutdown();
    }

    @Test
    void failToBuildFullContextWithoutLogVersionRepo() {
        assertThrows(NullPointerException.class, () -> builderWithTestCommandReaderFactory(databaseLayout, fileSystem)
                .withTransactionIdStore(new SimpleTransactionIdStore())
                .buildContext());
    }

    @Test
    void failToBuildFullContextWithoutTransactionIdStore() {
        assertThrows(NullPointerException.class, () -> builderWithTestCommandReaderFactory(databaseLayout, fileSystem)
                .withLogVersionRepository(new SimpleLogVersionRepository(2))
                .buildContext());
    }

    @Test
    void fileBasedOperationsContextFailOnLastCommittedTransactionIdAccess() {
        assertThrows(UnsupportedOperationException.class, () -> logFilesBasedOnlyBuilder(storeDirectory, fileSystem)
                .withCommandReaderFactory(new TestCommandReaderFactory())
                .buildContext()
                .getLastCommittedTransactionIdProvider()
                .getLastCommittedTransactionId(null));
    }

    @Test
    void fileBasedOperationsContextFailOnLogVersionRepositoryAccess() {
        assertThrows(UnsupportedOperationException.class, () -> logFilesBasedOnlyBuilder(storeDirectory, fileSystem)
                .withCommandReaderFactory(new TestCommandReaderFactory())
                .buildContext()
                .getLogVersionRepositoryProvider()
                .logVersionRepository(null));
    }

    private static LogFilesBuilder builderWithTestCommandReaderFactory(
            DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem) {
        return builder(databaseLayout, fileSystem).withCommandReaderFactory(new TestCommandReaderFactory());
    }
}
