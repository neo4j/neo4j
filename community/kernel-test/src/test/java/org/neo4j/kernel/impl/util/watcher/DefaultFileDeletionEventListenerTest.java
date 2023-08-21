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
package org.neo4j.kernel.impl.util.watcher;

import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.nio.file.WatchKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.watcher.resource.WatchedFile;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
class DefaultFileDeletionEventListenerTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private TestDirectory testDirectory;

    private final WatchKey databaseDirKey = mock(WatchKey.class);
    private final WatchKey databaseLogsDirKey = mock(WatchKey.class);
    private final WatchKey databaseRootKey = mock(WatchKey.class);
    private final WatchKey databaseLogsRootKey = mock(WatchKey.class);

    private final WatchKey anotherDatabaseDirKey = mock(WatchKey.class);
    private final WatchKey anotherDatabaseLogsDirKey = mock(WatchKey.class);

    private WatchedResource watchedDatabaseDirectory;
    private WatchedResource watchedLogsDirectory;
    private WatchedResource watchedDatabaseRootDirectory;
    private WatchedResource watchedLogsRootDirectory;

    @BeforeEach
    void setUp() {
        watchedDatabaseDirectory = new WatchedFile(databaseDirKey, databaseLayout.databaseDirectory());
        watchedLogsDirectory = new WatchedFile(databaseLogsDirKey, databaseLayout.getTransactionLogsDirectory());
        watchedDatabaseRootDirectory = new WatchedFile(databaseRootKey, neo4jLayout.databasesDirectory());
        watchedLogsRootDirectory = new WatchedFile(databaseLogsRootKey, neo4jLayout.transactionLogsRootDirectory());
    }

    @Test
    void notificationInLogAboutFileDeletion() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);
        listener.fileDeleted(databaseDirKey, "testFile");
        listener.fileDeleted(databaseDirKey, "anotherDirectory");

        var databaseName = databaseLayout.getDatabaseName();
        assertThat(internalLogProvider)
                .containsMessages(
                        "'testFile' which belongs to the '" + databaseName
                                + "' database was deleted while it was running.",
                        "'anotherDirectory' which belongs to the '" + databaseName
                                + "' database was deleted while it was running.");
    }

    @Test
    void notificationAboutDeletionInDatabaseDirectory() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        var databaseFile = "neostore.db";
        listener.fileDeleted(databaseDirKey, databaseFile);

        var databaseName = databaseLayout.getDatabaseName();
        assertThat(internalLogProvider)
                .containsMessages("'" + databaseFile + "' which belongs to the '" + databaseName
                        + "' database was deleted while it was running.");
    }

    @Test
    void noNotificationAboutDeletionInAnotherDatabaseDirectory() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        var databaseFile = "neostore.db";
        listener.fileDeleted(anotherDatabaseDirKey, databaseFile);

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    @Test
    void notificationAboutDeletionInTransactionLogsDirectory() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        var someFile = "someFile";
        listener.fileDeleted(databaseLogsDirKey, someFile);

        var databaseName = databaseLayout.getDatabaseName();
        assertThat(internalLogProvider)
                .containsMessages("'" + someFile + "' which belongs to the '" + databaseName
                        + "' database was deleted while it was running.");
    }

    @Test
    void noNotificationAboutDeletionInAnotherTransactionLogsDirectory() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        var someFile = "someFile";
        listener.fileDeleted(anotherDatabaseLogsDirKey, someFile);

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    @Test
    void notificationAboutDatabaseDirectoryRemoval() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        String databaseName = databaseLayout.getDatabaseName();
        listener.fileDeleted(databaseRootKey, databaseName);

        assertThat(internalLogProvider)
                .containsMessages("'" + databaseName + "' which belongs to the '" + databaseName
                        + "' database was deleted while it was running.");
    }

    @Test
    void noNotificationAboutAnotherDatabaseDirectoryRemoval() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        listener.fileDeleted(databaseRootKey, "foo");

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    @Test
    void notificationAboutLogsDirectoryRemoval() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        String databaseName = databaseLayout.getDatabaseName();
        listener.fileDeleted(databaseLogsRootKey, databaseName);

        assertThat(internalLogProvider)
                .containsMessages("'" + databaseName + "' which belongs to the '" + databaseName
                        + "' database was deleted while it was running.");
    }

    @Test
    void notificationAboutAnotherDatabaseLogsDirectoryRemoval() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);

        listener.fileDeleted(databaseLogsRootKey, "bar");

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    @Test
    void noNotificationForTransactionLogs() {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider(false);
        DefaultFileDeletionEventListener listener = buildListener(internalLogProvider);
        listener.fileDeleted(databaseLogsDirKey, TransactionLogFilesHelper.DEFAULT_NAME + ".0");
        listener.fileDeleted(databaseLogsDirKey, TransactionLogFilesHelper.DEFAULT_NAME + ".1");

        assertThat(internalLogProvider).doesNotHaveAnyLogs();
    }

    private DefaultFileDeletionEventListener buildListener(AssertableLogProvider internalLogProvider) {
        var logService = new SimpleLogService(NullLogProvider.getInstance(), internalLogProvider);
        var watchedResources = asSet(
                watchedDatabaseDirectory, watchedLogsDirectory, watchedDatabaseRootDirectory, watchedLogsRootDirectory);
        return new DefaultFileDeletionEventListener(
                databaseLayout,
                watchedResources,
                logService,
                filename -> filename.startsWith(TransactionLogFilesHelper.DEFAULT_NAME));
    }
}
