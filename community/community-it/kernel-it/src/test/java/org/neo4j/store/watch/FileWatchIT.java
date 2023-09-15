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
package org.neo4j.store.watch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionEventListener;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@DbmsExtension(configurationCallback = "configure")
@EnabledOnOs(OS.LINUX)
class FileWatchIT {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private GraphDatabaseService database;

    @Inject
    private DatabaseManagementService managementService;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.filewatcher_enabled, true);
        builder.setInternalLogProvider(logProvider);
    }

    @AfterEach
    void tearDown() {
        shutdownDatabaseSilently(managementService);
    }

    @Test
    void notifyAboutStoreFileDeletion() throws IOException, InterruptedException {
        String fileName = databaseLayout
                .pathForStore(CommonDatabaseStores.METADATA)
                .getFileName()
                .toString();
        FileWatcher fileWatcher = getFileWatcher(database);
        CheckPointer checkpointer = getCheckpointer(database);
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener(fileName);
        fileWatcher.addFileWatchEventListener(deletionListener);

        do {
            createNode(database);
            forceCheckpoint(checkpointer);
        } while (!deletionListener.awaitModificationNotification());

        deleteFile(databaseLayout.databaseDirectory(), fileName);
        deletionListener.awaitDeletionNotification();

        assertThat(logProvider)
                .containsMessages("'" + fileName + "' which belongs to the '"
                        + databaseLayout.databaseDirectory().getFileName().toString()
                        + "' database was deleted while it was running.");
    }

    @Test
    void notifyWhenFileWatchingFailToStart() {
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        DatabaseManagementService service = null;
        try {
            service = new TestDatabaseManagementServiceBuilder(testDirectory.homePath("failed-start-db"))
                    .setInternalLogProvider(logProvider)
                    .setConfig(GraphDatabaseSettings.filewatcher_enabled, true)
                    .setFileSystem(new NonWatchableFileSystemAbstraction())
                    .build();
            assertNotNull(managementService.database(DEFAULT_DATABASE_NAME));

            assertThat(logProvider)
                    .containsMessages("Can not create file watcher for current file system. "
                            + "File monitoring capabilities for store files will be disabled.");
        } finally {
            shutdownDatabaseSilently(service);
        }
    }

    @Test
    void doNotNotifyAboutIndexFilesDeletion() throws IOException, InterruptedException {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        FileWatcher fileWatcher = getFileWatcher(database);
        CheckPointer checkPointer = dependencyResolver.resolveDependency(CheckPointer.class);

        AccumulativeDeletionEventListener accumulativeListener = new AccumulativeDeletionEventListener();
        ModificationEventListener modificationListener = new ModificationEventListener();
        fileWatcher.addFileWatchEventListener(modificationListener);
        fileWatcher.addFileWatchEventListener(accumulativeListener);

        String labelName = "labelName";
        String propertyName = "propertyName";
        Label testLabel = Label.label(labelName);
        createIndexes(database, propertyName, testLabel);
        do {
            createNode(database, propertyName, testLabel);
            forceCheckpoint(checkPointer);
        } while (!modificationListener.awaitModificationNotification());

        fileWatcher.removeFileWatchEventListener(modificationListener);
        ModificationEventListener afterRemovalListener = new ModificationEventListener();
        fileWatcher.addFileWatchEventListener(afterRemovalListener);

        dropAllIndexes(database);
        do {
            createNode(database, propertyName, testLabel);
            forceCheckpoint(checkPointer);
        } while (!afterRemovalListener.awaitModificationNotification());

        accumulativeListener.assertDoesNotHaveAnyDeletions();
    }

    @Test
    void doNotMonitorTransactionLogFiles() throws IOException, InterruptedException {
        FileWatcher fileWatcher = getFileWatcher(database);
        CheckPointer checkpointer = getCheckpointer(database);
        ModificationEventListener modificationEventListener = new ModificationEventListener();
        fileWatcher.addFileWatchEventListener(modificationEventListener);

        do {
            createNode(database);
            forceCheckpoint(checkpointer);
        } while (!modificationEventListener.awaitModificationNotification());

        String fileName = TransactionLogFilesHelper.DEFAULT_NAME + ".0";
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener(fileName);
        fileWatcher.addFileWatchEventListener(deletionListener);
        deleteFile(databaseLayout.getTransactionLogsDirectory(), fileName);
        deletionListener.awaitDeletionNotification();

        assertThat(logProvider)
                .forClass(DefaultFileDeletionEventListener.class)
                .forLevel(INFO)
                .doesNotContainMessage(fileName);
    }

    @Test
    void notifyWhenWholeStoreDirectoryRemoved() throws IOException, InterruptedException {
        FileWatcher fileWatcher = getFileWatcher(database);
        CheckPointer checkpointer = getCheckpointer(database);

        ModificationEventListener modificationListener = new ModificationEventListener();
        fileWatcher.addFileWatchEventListener(modificationListener);
        do {
            createNode(database);
            forceCheckpoint(checkpointer);
        } while (!modificationListener.awaitModificationNotification());
        fileWatcher.removeFileWatchEventListener(modificationListener);

        String storeDirectoryName =
                databaseLayout.databaseDirectory().getFileName().toString();
        DeletionLatchEventListener eventListener = new DeletionLatchEventListener(storeDirectoryName);
        fileWatcher.addFileWatchEventListener(eventListener);
        FileUtils.deleteDirectory(databaseLayout.databaseDirectory());

        eventListener.awaitDeletionNotification();

        assertThat(logProvider)
                .containsMessages("'" + storeDirectoryName + "' which belongs to the '"
                        + databaseLayout.databaseDirectory().getFileName().toString()
                        + "' database was deleted while it was running.");
    }

    @Test
    void shouldLogWhenDisabled() {
        AssertableLogProvider logProvider = new AssertableLogProvider(true);
        DatabaseManagementService service = null;
        try {
            service = new TestDatabaseManagementServiceBuilder(testDirectory.homePath("failed-start-db"))
                    .setInternalLogProvider(logProvider)
                    .setFileSystem(new NonWatchableFileSystemAbstraction())
                    .setConfig(GraphDatabaseSettings.filewatcher_enabled, false)
                    .build();
            assertNotNull(managementService.database(DEFAULT_DATABASE_NAME));

            assertThat(logProvider).containsMessages("File watcher disabled by configuration.");
        } finally {
            shutdownDatabaseSilently(service);
        }
    }

    @Test
    void shouldLogWhenWatcherFails() throws Exception {
        FileWatcher fileWatcher = getFileWatcher(database);
        fileWatcher.addFileWatchEventListener(new FileWatchEventListener() {
            @Override
            public void fileDeleted(WatchKey key, String fileName) {
                throw new RuntimeException("Event listener failed");
            }
        });

        FileUtils.deleteFile(databaseLayout.metadataStore());
        assertThat(logProvider)
                .containsMessagesEventually(
                        TimeUnit.MINUTES.toMillis(1),
                        "File system event watching encountered an error and will stop monitoring file events",
                        "Event listener failed");
    }

    private static void shutdownDatabaseSilently(DatabaseManagementService managementService) {
        if (managementService != null) {
            try {
                managementService.shutdown();
            } catch (Exception expected) {
                // ignored
            }
        }
    }

    private static void dropAllIndexes(GraphDatabaseService database) {
        try (Transaction transaction = database.beginTx()) {
            for (IndexDefinition definition : transaction.schema().getIndexes()) {
                if (definition.getIndexType() != IndexType.LOOKUP) {
                    definition.drop();
                }
            }
            transaction.commit();
        }
    }

    private static void createIndexes(GraphDatabaseService database, String propertyName, Label testLabel) {
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().indexFor(testLabel).on(propertyName).create();
            transaction.commit();
        }

        try (Transaction tx = database.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }
    }

    private static void forceCheckpoint(CheckPointer checkPointer) throws IOException {
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("testForceCheckPoint"));
    }

    private static void createNode(GraphDatabaseService database, String propertyName, Label testLabel) {
        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode(testLabel);
            node.setProperty(propertyName, "value");
            transaction.commit();
        }
    }

    private static CheckPointer getCheckpointer(GraphDatabaseService database) {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(CheckPointer.class);
    }

    private static FileWatcher getFileWatcher(GraphDatabaseService database) {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver
                .resolveDependency(FileSystemWatcherService.class)
                .getFileWatcher();
    }

    private static void deleteFile(Path storeDir, String fileName) throws IOException {
        Path metadataStore = storeDir.resolve(fileName);
        FileUtils.deleteFile(metadataStore);
    }

    private static void createNode(GraphDatabaseService database) {
        try (Transaction transaction = database.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
    }

    private static class NonWatchableFileSystemAbstraction extends DefaultFileSystemAbstraction {
        @Override
        public FileWatcher fileWatcher() throws IOException {
            throw new IOException("You can't watch me!");
        }
    }

    private static class AccumulativeDeletionEventListener implements FileWatchEventListener {
        private final List<String> deletedFiles = new ArrayList<>();

        @Override
        public void fileDeleted(WatchKey key, String fileName) {
            deletedFiles.add(fileName);
        }

        void assertDoesNotHaveAnyDeletions() {
            assertThat(deletedFiles)
                    .as("Should not have any deletions registered")
                    .isEmpty();
        }
    }

    private static class ModificationEventListener implements FileWatchEventListener {
        private final CountDownLatch modificationLatch = new CountDownLatch(1);
        private int awaitTries;

        @Override
        public void fileModified(WatchKey key, String fileName) {
            modificationLatch.countDown();
        }

        boolean awaitModificationNotification() throws InterruptedException {
            if (awaitTries++ > 300) {
                throw new RuntimeException("Timed-out waiting for modification");
            }
            return modificationLatch.await(1, TimeUnit.SECONDS);
        }
    }

    private static class DeletionLatchEventListener extends ModificationEventListener {
        private final CountDownLatch deletionLatch = new CountDownLatch(1);
        final String expectedFileName;

        DeletionLatchEventListener(String expectedFileName) {
            this.expectedFileName = expectedFileName;
        }

        @Override
        public void fileDeleted(WatchKey key, String fileName) {
            if (fileName.endsWith(expectedFileName)) {
                deletionLatch.countDown();
            }
        }

        void awaitDeletionNotification() throws InterruptedException {
            deletionLatch.await(5, TimeUnit.MINUTES);
        }
    }
}
