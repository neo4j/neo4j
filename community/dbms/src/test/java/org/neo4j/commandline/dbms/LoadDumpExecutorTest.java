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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.Locker;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
public class LoadDumpExecutorTest {
    @Inject
    private TestDirectory testDirectory;

    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Loader loader;

    @BeforeEach
    void setUp() throws IOException {
        homeDir = testDirectory.directory("home-dir");
        prepareFooDatabaseDirectory();
        configDir = testDirectory.directory("config-dir");
        archive = testDirectory.directory("some-archive-dir");
        loader = mock(Loader.class);
        doReturn(mock(StoreVersionLoader.Result.class)).when(loader).getStoreVersion(any(), any(), any(), any());
    }

    private void prepareFooDatabaseDirectory() throws IOException {
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .set(initial_default_database, "foo")
                .build();
        Path databaseDirectory = DatabaseLayout.of(config).databaseDirectory();
        testDirectory.getFileSystem().mkdirs(databaseDirectory);
    }

    @Test
    void shouldLoadTheDatabaseFromTheArchive() throws CommandFailedException, IOException, IncorrectFormat {
        execute("foo", archive);
        DatabaseLayout databaseLayout = createDatabaseLayout(
                homeDir.resolve("data"),
                homeDir.resolve("data/databases"),
                "foo",
                homeDir.resolve("data/" + DEFAULT_TX_LOGS_ROOT_DIR_NAME));
        verify(loader).load(eq(databaseLayout), anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldCalculateTheDatabaseDirectoryFromConfig() throws IOException, CommandFailedException, IncorrectFormat {
        Path dataDir = testDirectory.directory("some-other-path");
        Path databaseDir = dataDir.resolve("databases/foo");
        Path transactionLogsDir = dataDir.resolve(DEFAULT_TX_LOGS_ROOT_DIR_NAME);
        Files.createDirectories(databaseDir);
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                singletonList(formatProperty(data_directory, dataDir)));

        execute("foo", archive);
        DatabaseLayout databaseLayout =
                createDatabaseLayout(dataDir, databaseDir.getParent(), "foo", transactionLogsDir);
        verify(loader).load(eq(databaseLayout), anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldCalculateTheTxLogDirectoryFromConfig() throws Exception {
        Path dataDir = testDirectory.directory("some-other-path");
        Path txLogsDir = testDirectory.directory("txLogsPath");
        Path databaseDir = dataDir.resolve("databases/foo");
        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                asList(formatProperty(data_directory, dataDir), formatProperty(transaction_logs_root_path, txLogsDir)));

        execute("foo", archive);
        DatabaseLayout databaseLayout = createDatabaseLayout(dataDir, databaseDir.getParent(), "foo", txLogsDir);
        verify(loader).load(eq(databaseLayout), anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldHandleSymlinkToDatabaseDir() throws IOException, CommandFailedException, IncorrectFormat {
        Path symDir = testDirectory.directory("path-to-links");
        Path realDatabaseDir = symDir.resolve("foo");

        Path dataDir = testDirectory.directory("some-other-path");
        Path databaseDir = dataDir.resolve("databases/foo");
        Path txLogsDir = dataDir.resolve(DEFAULT_TX_LOGS_ROOT_DIR_NAME);
        Path databasesDir = dataDir.resolve("databases");

        Files.createDirectories(realDatabaseDir);
        Files.createDirectories(databasesDir);

        Files.createSymbolicLink(databaseDir, realDatabaseDir);

        Files.write(
                configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME),
                singletonList(formatProperty(data_directory, dataDir)));

        execute("foo", archive);
        DatabaseLayout databaseLayout = createDatabaseLayout(dataDir, databasesDir, "foo", txLogsDir);
        verify(loader).load(eq(databaseLayout), anyBoolean(), anyBoolean(), any(), any(), any());
    }

    @Test
    void shouldDeleteTheOldDatabaseIfForceArgumentIsProvided()
            throws CommandFailedException, IOException, IncorrectFormat {
        Path databaseDirectory = homeDir.resolve("data/databases/foo");
        Path marker = databaseDirectory.resolve("marker");
        Path txDirectory = homeDir.resolve("data/" + DEFAULT_TX_LOGS_ROOT_DIR_NAME);
        Files.createDirectories(databaseDirectory);
        Files.createDirectories(txDirectory);

        doAnswer(ignored -> {
                    assertThat(Files.exists(marker)).isEqualTo(false);
                    return null;
                })
                .when(loader)
                .load(any(), anyBoolean(), anyBoolean(), any(), any(), any());

        execute("foo", archive, true);
    }

    @Test
    void shouldNotDeleteTheOldDatabaseIfForceArgumentIsNotProvided()
            throws CommandFailedException, IOException, IncorrectFormat {
        Path databaseDirectory = homeDir.resolve("data/databases/foo").toAbsolutePath();
        Files.createDirectories(databaseDirectory);

        doAnswer(ignored -> {
                    assertThat(Files.exists(databaseDirectory)).isEqualTo(true);
                    return null;
                })
                .when(loader)
                .load(any(), anyBoolean(), anyBoolean(), any(), any(), any());

        execute("foo", archive);
    }

    @Test
    void shouldRespectTheDatabaseLock() throws IOException {
        Path databaseDirectory = homeDir.resolve("data/databases/foo");
        Files.createDirectories(databaseDirectory);
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(databaseDirectory);

        try (FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Locker locker = new DatabaseLocker(fileSystem, databaseLayout)) {
            locker.checkLock();
            CommandFailedException commandFailed =
                    assertThrows(CommandFailedException.class, () -> execute("foo", archive, true));
            assertEquals("The database is in use. Stop database 'foo' and try again.", commandFailed.getMessage());
        }
    }

    private DatabaseLayout createDatabaseLayout(
            Path dataPath, Path storePath, String databaseName, Path transactionLogsPath) {
        Config config = Config.newBuilder()
                .set(neo4j_home, homeDir.toAbsolutePath())
                .set(data_directory, dataPath.toAbsolutePath())
                .set(databases_root_path, storePath.toAbsolutePath())
                .set(transaction_logs_root_path, transactionLogsPath.toAbsolutePath())
                .build();
        return Neo4jLayout.of(config).databaseLayout(databaseName);
    }

    private void execute(String database, Path archive, boolean force) throws IOException {
        Config config = Config.newBuilder()
                .fromFileNoThrow(configDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .set(GraphDatabaseSettings.neo4j_home, homeDir)
                .build();

        LoadDumpExecutor loadDumpExecutor = new LoadDumpExecutor(
                config, testDirectory.getFileSystem(), System.err, loader, DumpFormatSelector::decompress);

        ThrowingSupplier<InputStream, IOException> dumpInputStreamSupplier = () -> Files.newInputStream(archive);

        loadDumpExecutor.execute(new LoadDumpExecutor.DumpInput(dumpInputStreamSupplier, ""), database, force);
    }

    private void execute(String database, Path archive) throws IOException {
        execute(database, archive, false);
    }

    private static String formatProperty(Setting setting, Path path) {
        return format("%s=%s", setting.name(), path.toString().replace('\\', '/'));
    }
}
