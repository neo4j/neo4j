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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@TestDirectoryExtension
class StoreInfoCommandTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fileSystem;

    private Path fooDbDirectory;
    private StoreInfoCommand command;
    private PrintStream out;
    private DatabaseLayout fooDbLayout;
    private Path homeDir;
    private StorageEngineFactory storageEngineFactory;
    private StorageEngineFactory.Selector storageEngineSelector;
    private final TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);

    @BeforeEach
    void setUp() throws Exception {
        homeDir = testDirectory.directory("home-dir");
        fooDbDirectory = homeDir.resolve("data/databases/foo");
        fooDbLayout = DatabaseLayout.ofFlat(fooDbDirectory);
        fileSystem.mkdirs(fooDbDirectory);

        out = mock(PrintStream.class);
        storageEngineFactory = mock(StorageEngineFactory.class);
        doReturn(transactionIdStore).when(storageEngineFactory).readOnlyTransactionIdStore(any());
        storageEngineSelector = mock(StorageEngineFactory.Selector.class);
        when(storageEngineSelector.selectStorageEngine(any(), any(), any())).thenReturn(Optional.empty());
        command = new StoreInfoCommand(
                new ExecutionContext(homeDir, homeDir, out, mock(PrintStream.class), testDirectory.getFileSystem()),
                storageEngineSelector);
    }

    @Test
    void printUsageHelp() {
        var baos = new ByteArrayOutputStream();
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Print information about a Neo4j database store.

                USAGE

                info [--all] [--expand-commands] [--structured] [--verbose] <path>

                DESCRIPTION

                Print information about a Neo4j database store, such as what version of Neo4j
                created it.

                PARAMETERS

                      <path>              Path to database store files, or databases directory
                                            if --all option is used

                OPTIONS

                      --all               Return store info for all databases at provided path
                      --expand-commands   Allow command expansion in config value evaluation.
                      --structured        Return result structured as json
                      --verbose           Enable verbose output.""");
    }

    @Test
    void nonExistingDatabaseShouldThrow() {
        var notADirArgs = args(Paths.get("yaba", "daba", "doo"), false, false);
        CommandLine.populateCommand(command, notADirArgs);
        var notADirException = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(notADirException.getMessage()).contains("must point to a directory");

        var dir = testDirectory.directory("not-a-db");
        var notADbArgs = args(dir, false, false);
        CommandLine.populateCommand(command, notADbArgs);
        var notADbException = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(notADbException.getMessage()).contains("does not contain the store files of a database");
    }

    @Test
    void readsLatestStoreVersionCorrectly() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", null, null, 5);
        CommandLine.populateCommand(command, fooDbDirectory.toAbsolutePath().toString());
        command.execute();

        verify(out)
                .println(Mockito.<String>argThat(
                        result -> result.contains(String.format("Store format version:         %s", "A"))
                                && result.contains(String.format("Store format introduced in:   %s", "v1"))));
    }

    @Test
    void readsOlderStoreVersionCorrectly() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", "B", "v2", 5);
        CommandLine.populateCommand(command, fooDbDirectory.toAbsolutePath().toString());
        command.execute();

        verify(out)
                .println(Mockito.<String>argThat(result -> result.contains("Store format version:         A")
                        && result.contains("Store format introduced in:   v1")
                        && result.contains("Store format superseded in:   v2")));
    }

    @Test
    void throwsOnUnknownVersion() throws IOException {
        prepareStore(fooDbLayout, "unknown", "v1", null, null, 3);
        when(storageEngineFactory.versionInformation(any(StoreId.class))).thenThrow(IllegalArgumentException.class);
        CommandLine.populateCommand(command, fooDbDirectory.toAbsolutePath().toString());
        var exception = assertThrows(Exception.class, () -> command.execute());
        assertThat(exception).hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectLockFiles() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", null, null, 4);
        try (Locker locker = new DatabaseLocker(fileSystem, fooDbLayout)) {
            locker.checkLock();
            CommandLine.populateCommand(command, fooDbDirectory.toAbsolutePath().toString());
            var exception = assertThrows(Exception.class, () -> command.execute());
            assertEquals(
                    "Failed to execute command as the database 'foo' is in use. Please stop it and try again.",
                    exception.getMessage());
        }
    }

    @Test
    void doesNotThrowWhenUsingAllAndSomeDatabasesLocked() throws IOException {
        // given
        var barDbDirectory = homeDir.resolve("data/databases/bar");
        var barDbLayout = DatabaseLayout.ofFlat(barDbDirectory);
        fileSystem.mkdirs(barDbDirectory);

        prepareStore(fooDbLayout, "A", "v1", null, null, 4);
        prepareStore(barDbLayout, "A", "v1", null, null, 5);
        var databasesRoot = homeDir.resolve("data/databases");

        var expectedBar = expectedStructuredResult("bar", false, "A", "v1", null, 5);

        var expectedFoo = expectedStructuredResult("foo", true, null, null, null, -1);

        var expected = String.format("[%s,%s]", expectedBar, expectedFoo);

        // when
        try (Locker locker = new DatabaseLocker(fileSystem, fooDbLayout)) {
            locker.checkLock();

            CommandLine.populateCommand(command, args(databasesRoot, true, true));
            command.execute();
        }

        verify(out).println(expected);
    }

    @Test
    void returnsInfoForAllDatabasesInDirectory() throws IOException {
        // given
        var barDbDirectory = homeDir.resolve("data/databases/bar");
        var barDbLayout = DatabaseLayout.ofFlat(barDbDirectory);
        fileSystem.mkdirs(barDbDirectory);

        prepareStore(fooDbLayout, "A", "v1", null, null, 9);
        var expectedFoo = expectedPrettyResult("foo", false, "A", "v1", null, 9);

        prepareStore(barDbLayout, "A", "v1", null, null, 10);
        var expectedBar = expectedPrettyResult("bar", false, "A", "v1", null, 10);

        var databasesRoot = homeDir.resolve("data/databases");
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn(10L, 9L);

        var expected = expectedBar + System.lineSeparator() + System.lineSeparator() + expectedFoo;

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, false));
        command.execute();

        // then
        verify(out).println(expected);
    }

    @Test
    void returnsInfoStructuredAsJson() throws IOException {
        // given
        prepareStore(fooDbLayout, "A", "v1", null, null, 13);
        var expectedFoo = expectedStructuredResult("foo", false, "A", "v1", null, 13);

        // when
        CommandLine.populateCommand(command, args(fooDbDirectory, false, true));
        command.execute();

        // then
        verify(out).println(expectedFoo);
    }

    @Test
    void prettyMultiStoreInfoResultHasTrailingLineSeparator() throws IOException {
        // given
        var barDbDirectory = homeDir.resolve("data/databases/bar");
        var barDbLayout = DatabaseLayout.ofFlat(barDbDirectory);
        fileSystem.mkdirs(barDbDirectory);

        prepareStore(fooDbLayout, "B", "v2", null, null, 2);
        var expectedFoo = expectedPrettyResult("foo", false, "B", "v2", null, 2);
        prepareStore(barDbLayout, "B", "v2", null, null, 3);
        var expectedBar = expectedPrettyResult("bar", false, "B", "v2", null, 3);
        var databasesRoot = homeDir.resolve("data/databases");
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn(3L, 2L);

        var expectedMulti = expectedBar + System.lineSeparator() + System.lineSeparator() + expectedFoo;

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, false));
        command.execute();

        // then
        verify(out).println(expectedMulti);
    }

    @Test
    void prettySingleStoreInfoResultHasTrailingLineSeparator() throws IOException {
        // given
        prepareStore(fooDbLayout, "B", "v2", null, null, 8);
        var expectedFoo = expectedPrettyResult("foo", false, "B", "v2", null, 8);

        // when
        CommandLine.populateCommand(command, args(fooDbDirectory, false, false));
        command.execute();

        // then
        verify(out).println(expectedFoo);
    }

    private static String expectedPrettyResult(
            String databaseName,
            boolean inUse,
            String version,
            String introduced,
            String superseded,
            long lastCommittedTxId) {
        var nullSafeSuperseded =
                superseded == null ? "" : "Store format superseded in:   " + superseded + System.lineSeparator();
        return "Database name:                " + databaseName + System.lineSeparator()
                + "Database in use:              "
                + inUse + System.lineSeparator() + "Store format version:         "
                + version + System.lineSeparator() + "Store format introduced in:   "
                + introduced + System.lineSeparator() + nullSafeSuperseded
                + "Last committed transaction id:"
                + lastCommittedTxId + System.lineSeparator() + "Store needs recovery:         true";
    }

    private static String expectedStructuredResult(
            String databaseName,
            boolean inUse,
            String version,
            String introduced,
            String superseded,
            long lastCommittedTxId) {
        return "{" + "\"databaseName\":\""
                + databaseName + "\"," + "\"inUse\":\""
                + inUse + "\"," + "\"storeFormat\":"
                + nullSafeField(version) + "," + "\"storeFormatIntroduced\":"
                + nullSafeField(introduced) + "," + "\"storeFormatSuperseded\":"
                + nullSafeField(superseded) + "," + "\"lastCommittedTransaction\":\""
                + lastCommittedTxId + "\"," + "\"recoveryRequired\":\"true\""
                + "}";
    }

    private static String nullSafeField(String value) {
        return value == null ? "null" : "\"" + value + "\"";
    }

    private void prepareStore(
            DatabaseLayout databaseLayout,
            String storeVersion,
            String introducedInVersion,
            String successorStoreVersion,
            String successorNeo4jVersion,
            long lastCommittedTxId)
            throws IOException {
        doReturn(Optional.of(storageEngineFactory))
                .when(storageEngineSelector)
                .selectStorageEngine(
                        any(),
                        argThat(dbLayout -> dbLayout.databaseDirectory().equals(databaseLayout.databaseDirectory())),
                        any());
        doReturn(true)
                .when(storageEngineFactory)
                .storageExists(
                        any(),
                        argThat(dbLayout -> dbLayout.databaseDirectory().equals(databaseLayout.databaseDirectory())),
                        any());
        doReturn(StorageFilesState.recoveredState())
                .when(storageEngineFactory)
                .checkStoreFileState(
                        any(),
                        argThat(dbLayout -> dbLayout.databaseDirectory().equals(databaseLayout.databaseDirectory())),
                        any());

        StoreVersion storeVersion2 = null;
        if (successorStoreVersion != null) {
            storeVersion2 = mockedStoreVersion(successorStoreVersion, successorNeo4jVersion, null);
        }

        StoreId storeId = mock(StoreId.class);
        when(storageEngineFactory.retrieveStoreId(any(), any(), any(), any())).thenReturn(storeId);
        StoreVersion storeVersion1 = mockedStoreVersion(storeVersion, introducedInVersion, storeVersion2);

        when(storageEngineFactory.versionInformation(storeId)).thenReturn(Optional.of(storeVersion1));
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn(lastCommittedTxId);
    }

    private StoreVersion mockedStoreVersion(
            String storeVersion, String introducedInVersion, StoreVersion supersededByStoreVersion) {
        StoreVersion version = mock(StoreVersion.class);
        when(version.getStoreVersionUserString()).thenReturn(storeVersion);
        when(version.introductionNeo4jVersion()).thenReturn(introducedInVersion);
        when(version.successorStoreVersion()).thenReturn(Optional.ofNullable(supersededByStoreVersion));
        return version;
    }

    private static String[] args(Path path, boolean all, boolean structured) {
        var args = new ArrayList<String>();
        args.add(path.toAbsolutePath().toString());

        if (all) {
            args.add("--all");
        }

        if (structured) {
            args.add("--structured");
        }

        return args.toArray(new String[0]);
    }
}
