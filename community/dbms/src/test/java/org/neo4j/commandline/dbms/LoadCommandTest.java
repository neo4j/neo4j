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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@Neo4jLayoutExtension
class LoadCommandTest {
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
        archive = testDirectory.directory("some-archive.dump");
        loader = mock(Loader.class);
        doReturn(mock(StoreVersionLoader.Result.class)).when(loader).getStoreVersion(any(), any(), any(), any());
    }

    private void prepareFooDatabaseDirectory() throws IOException {
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath())
                .set(default_database, "foo")
                .build();
        Path databaseDirectory = DatabaseLayout.of(config).databaseDirectory();
        testDirectory.getFileSystem().mkdirs(databaseDirectory);
    }

    @Test
    void printUsageHelp() {
        var baos = new ByteArrayOutputStream();
        var command = new LoadCommand(new ExecutionContext(Path.of("."), Path.of(".")), loader);
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Load a database from an archive created with the dump command.

                USAGE

                load [--expand-commands] [--force] [--info] [--verbose] [--database=<database>]
                     --from=<path>

                DESCRIPTION

                Load a database from an archive. <archive-path> must be an archive created with
                the dump command. <database> is the name of the database to create. Existing
                databases can be replaced by specifying --force. It is not possible to replace
                a database that is mounted in a running Neo4j server. If --info is specified,
                then the database is not loaded, but information (i.e. file count, byte count,
                and format of load file) about the archive is printed instead.

                OPTIONS

                      --database=<database>
                                          Name of the database to load.
                                            Default: neo4j
                      --expand-commands   Allow command expansion in config value evaluation.
                      --force             If an existing database should be replaced.
                      --from=<path>       Path to archive created with the dump command or '-'
                                            to read from standard input.
                      --info              Print meta-data information about the archive file,
                                            instead of loading the contained database.
                      --verbose           Enable verbose output.""");
    }

    @Test
    void shouldGiveAClearMessageIfTheArchiveDoesntExist() throws IOException, IncorrectFormat {
        doThrow(new NoSuchFileException(archive.toString())).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Archive does not exist: " + archive, commandFailed.getMessage());
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseAlreadyExists() throws IOException, IncorrectFormat {
        doThrow(FileAlreadyExistsException.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Database already exists: foo", commandFailed.getMessage());
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabasesDirectoryIsNotWritable() throws IOException, IncorrectFormat {
        doThrow(AccessDeniedException.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("You do not have permission to load the database.", commandFailed.getMessage());
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectFormat {
        doThrow(new FileSystemException("the-message")).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Unable to load database: FileSystemException: the-message", commandFailed.getMessage());
    }

    @Test
    void shouldThrowIfTheArchiveFormatIsInvalid() throws IOException, IncorrectFormat {
        doThrow(IncorrectFormat.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertThat(commandFailed.getMessage()).contains(archive.toString());
        assertThat(commandFailed.getMessage()).contains("valid Neo4j archive");
    }

    @Test
    void infoMustPrintArchiveMetaData() throws IOException {
        when(loader.getMetaData(any())).thenReturn(new Loader.DumpMetaData("ZSTD", "42", "1337"));
        var baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos)) {
            Path dir = Path.of(".");
            var command =
                    new LoadCommand(new ExecutionContext(dir, dir, out, out, testDirectory.getFileSystem()), loader);
            CommandLine.populateCommand(
                    command, "--info", "--from", archive.toAbsolutePath().toString());
            command.execute();
            out.flush();
        }
        String output = baos.toString();
        assertThat(output).contains("ZSTD", "42", "1337");
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

    private void execute(String database, Path archive) {
        var command = buildCommand();
        CommandLine.populateCommand(command, "--from=" + archive, "--database=" + database);
        command.execute();
    }

    private void executeForce(String database) {
        var command = buildCommand();
        CommandLine.populateCommand(command, "--from=" + archive.toAbsolutePath(), "--database=" + database, "--force");
        command.execute();
    }

    private LoadCommand buildCommand() {
        PrintStream out = mock(PrintStream.class);
        PrintStream err = mock(PrintStream.class);
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        return new LoadCommand(new ExecutionContext(homeDir, configDir, out, err, fileSystem), loader);
    }

    private static String formatProperty(Setting setting, Path path) {
        return format("%s=%s", setting.name(), path.toString().replace('\\', '/'));
    }
}
