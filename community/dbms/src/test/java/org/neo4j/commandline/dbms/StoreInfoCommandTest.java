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
    private Path databasesRoot;
    private StorageEngineFactory storageEngineFactory;
    private StorageEngineFactory.Selector storageEngineSelector;

    @BeforeEach
    void setUp() throws Exception {
        homeDir = testDirectory.directory("home-dir");
        databasesRoot = homeDir.resolve("data/databases");
        fooDbDirectory = homeDir.resolve("data/databases/foo");
        fooDbLayout = DatabaseLayout.ofFlat(fooDbDirectory);
        fileSystem.mkdirs(fooDbDirectory);

        out = mock(PrintStream.class);
        storageEngineFactory = mock(StorageEngineFactory.class);
        doReturn(fooDbLayout).when(storageEngineFactory).formatSpecificDatabaseLayout(any());
        storageEngineSelector = mock(StorageEngineFactory.Selector.class);
        when(storageEngineSelector.selectStorageEngine(any(), any())).thenReturn(Optional.empty());
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

                info [-h] [--expand-commands] [--verbose] [--additional-config=<file>]
                     [--format=text|json] [--from-path=<path>] [<database>]

                DESCRIPTION

                Print information about a Neo4j database store, such as what version of Neo4j
                created it.

                PARAMETERS

                      [<database>]         Name of the database to show info for. Can contain *
                                             and ? for globbing. Note that * and ? have special
                                             meaning in some shells and might need to be
                                             escaped or used with quotes.
                                             Default: *

                OPTIONS

                      --additional-config=<file>
                                           Configuration file with additional configuration.
                      --expand-commands    Allow command expansion in config value evaluation.
                      --format=text|json   The format of the returned information.
                                             Default: text
                      --from-path=<path>   Path to databases directory.
                  -h, --help               Show this help message and exit.
                      --verbose            Enable verbose output.""");
    }

    @Test
    void nonExistingDirShouldThrow() {
        var notADirArgs = args(Paths.get("yaba", "daba", "doo"), true, "foo");
        CommandLine.populateCommand(command, notADirArgs);
        var notADirException = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(notADirException.getMessage()).contains("must point to a directory");
    }

    @Test
    void databaseDirShouldThrow() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", null, null, 5);
        var dbDirArgs = args(fooDbDirectory, false, "");
        CommandLine.populateCommand(command, dbDirArgs);
        var dbDirException = assertThrows(CommandFailedException.class, () -> command.execute());
        assertThat(dbDirException.getMessage()).contains("should point to the databases directory");
    }

    @Test
    void readsLatestStoreVersionCorrectly() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", null, null, 5);
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo"));
        command.execute();

        verify(out)
                .println(Mockito.<String>argThat(
                        result -> result.contains(String.format("Store format version:         %s", "A"))
                                && result.contains(String.format("Store format introduced in:   %s", "v1"))));
    }

    @Test
    void readsOlderStoreVersionCorrectly() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", "B", "v2", 5);
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo"));
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
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo"));
        var exception = assertThrows(Exception.class, () -> command.execute());
        assertThat(exception).hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void respectLockFiles() throws IOException {
        prepareStore(fooDbLayout, "A", "v1", null, null, 4);
        try (Locker locker = new DatabaseLocker(fileSystem, fooDbLayout)) {
            locker.checkLock();
            CommandLine.populateCommand(command, args(databasesRoot, true, "foo"));
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

        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        prepareStore(barDbLayout, "A", "v1", null, null, 1);

        var expectedBar = expectedStructuredResult("bar", false, "A", "v1", null, 1);

        var expectedFoo = expectedStructuredResult("foo", true, null, null, null, -1);

        var expected = String.format("[%s,%s]", expectedBar, expectedFoo);

        // when
        try (Locker locker = new DatabaseLocker(fileSystem, fooDbLayout)) {
            locker.checkLock();

            CommandLine.populateCommand(command, args(databasesRoot, false, "", true, "json"));
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

        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "A", "v1", null, 1);

        prepareStore(barDbLayout, "A", "v1", null, null, 1);
        var expectedBar = expectedPrettyResult("bar", false, "A", "v1", null, 1);

        var expected = expectedBar + System.lineSeparator() + System.lineSeparator() + expectedFoo;

        // when
        CommandLine.populateCommand(command, args(databasesRoot, false, ""));
        command.execute();

        // then
        verify(out).println(expected);
    }

    @Test
    void globbingCanSelectAllDatabasesInDirectory() throws IOException {
        // given
        var barDbDirectory = homeDir.resolve("data/databases/bar");
        var barDbLayout = DatabaseLayout.ofFlat(barDbDirectory);
        fileSystem.mkdirs(barDbDirectory);

        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "A", "v1", null, 1);

        prepareStore(barDbLayout, "A", "v1", null, null, 1);
        var expectedBar = expectedPrettyResult("bar", false, "A", "v1", null, 1);

        var expected = expectedBar + System.lineSeparator() + System.lineSeparator() + expectedFoo;

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, "*"));
        command.execute();

        // then
        verify(out).println(expected);
    }

    @Test
    void globbingCanSelectSomeDatabasesInDirectory() throws IOException {
        // given
        var barDbDirectory = homeDir.resolve("data/databases/bar");
        var barDbLayout = DatabaseLayout.ofFlat(barDbDirectory);
        fileSystem.mkdirs(barDbDirectory);

        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "A", "v1", null, 1);

        prepareStore(barDbLayout, "A", "v1", null, null, 1);

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, "f*"));
        command.execute();

        // then
        verify(out).println(expectedFoo);
    }

    @Test
    void returnsInfoStructuredAsJson() throws IOException {
        // given
        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        var expectedFoo = expectedStructuredResult("foo", false, "A", "v1", null, 1);

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo", true, "json"));
        command.execute();

        // then
        verify(out).println(expectedFoo);
    }

    @Test
    void returnsInfoInTextFormat() throws IOException {
        // given
        prepareStore(fooDbLayout, "A", "v1", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "A", "v1", null, 1);

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo", true, "text"));
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

        prepareStore(fooDbLayout, "B", "v2", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "B", "v2", null, 1);
        prepareStore(barDbLayout, "B", "v2", null, null, 1);
        var expectedBar = expectedPrettyResult("bar", false, "B", "v2", null, 1);

        var expectedMulti = expectedBar + System.lineSeparator() + System.lineSeparator() + expectedFoo;

        // when
        CommandLine.populateCommand(command, args(databasesRoot, false, ""));
        command.execute();
        // then
        verify(out).println(expectedMulti);
    }

    @Test
    void prettySingleStoreInfoResultHasTrailingLineSeparator() throws IOException {
        // given
        prepareStore(fooDbLayout, "B", "v2", null, null, 1);
        var expectedFoo = expectedPrettyResult("foo", false, "B", "v2", null, 1);

        // when
        CommandLine.populateCommand(command, args(databasesRoot, true, "foo"));
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
                .selectStorageEngine(any(), argThat(dbLayout -> dbLayout.databaseDirectory()
                        .equals(databaseLayout.databaseDirectory())));
        doReturn(true).when(storageEngineFactory).storageExists(any(), argThat(dbLayout -> dbLayout.databaseDirectory()
                .equals(databaseLayout.databaseDirectory())));
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

        doReturn(Optional.of(storeVersion1)).when(storageEngineFactory).versionInformation(storeId);
    }

    private StoreVersion mockedStoreVersion(
            String storeVersion, String introducedInVersion, StoreVersion supersededByStoreVersion) {
        StoreVersion version = mock(StoreVersion.class);
        when(version.getStoreVersionUserString()).thenReturn(storeVersion);
        when(version.introductionNeo4jVersion()).thenReturn(introducedInVersion);
        when(version.successorStoreVersion(any())).thenReturn(Optional.ofNullable(supersededByStoreVersion));
        return version;
    }

    private static String[] args(Path path, boolean includeDatabase, String database) {
        return args(path, includeDatabase, database, false, "");
    }

    private static String[] args(
            Path path, boolean includeDatabase, String database, boolean includeFormat, String format) {
        var args = new ArrayList<String>();
        args.add("--from-path=" + path.toAbsolutePath());

        if (includeDatabase) {
            args.add(database);
        }

        if (includeFormat) {
            args.add("--format=" + format);
        }

        return args.toArray(new String[0]);
    }
}
