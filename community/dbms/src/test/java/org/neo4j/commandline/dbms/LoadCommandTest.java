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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.initial_default_database;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
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
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

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
    void printUsageHelp() {
        var baos = new ByteArrayOutputStream();
        var command = new LoadCommand(new ExecutionContext(Path.of("."), Path.of("."))) {
            @Override
            protected Loader createLoader(FileSystemAbstraction fs) {
                return loader;
            }
        };
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                Load a database from an archive created with the dump command.

                USAGE

                load [-h] [--expand-commands] [--info] [--verbose] [--overwrite-destination
                     [=true|false]] [--additional-config=<file>] [--from-path=<path> |
                     --from-stdin] <database>

                DESCRIPTION

                Load a database from an archive. <archive-path> must be a directory containing
                an archive(s) created with the dump command. If neither --from-path or
                --from-stdin is supplied `server.directories.dumps.root` setting will be
                searched for the archive. Existing databases can be replaced by specifying
                --overwrite-destination. It is not possible to replace a database that is
                mounted in a running Neo4j server. If --info is specified, then the database is
                not loaded, but information (i.e. file count, byte count, and format of load
                file) about the archive is printed instead.

                PARAMETERS

                      <database>           Name of the database to load. Can contain * and ?
                                             for globbing. Note that * and ? have special
                                             meaning in some shells and might need to be
                                             escaped or used with quotes.

                OPTIONS

                      --additional-config=<file>
                                           Configuration file with additional configuration.
                      --expand-commands    Allow command expansion in config value evaluation.
                      --from-path=<path>   Path to directory containing archive(s) created with
                                             the dump command.
                      --from-stdin         Read dump from standard input.
                  -h, --help               Show this help message and exit.
                      --info               Print meta-data information about the archive file,
                                             instead of loading the contained database.
                      --overwrite-destination[=true|false]
                                           If an existing database should be replaced.
                                             Default: false
                      --verbose            Enable verbose output.""");
    }

    @Test
    void shouldGiveAClearMessageIfTheArchiveDoesntExist() throws IOException, IncorrectFormat {
        String dumpName = archive.resolve("foo.dump").toString();
        doThrow(new NoSuchFileException(dumpName)).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Load failed for databases: 'foo'", commandFailed.getMessage());
        assertEquals(
                "Archive does not exist: " + dumpName, commandFailed.getCause().getMessage());
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseAlreadyExists() throws IOException, IncorrectFormat {
        createDummyDump("foo", archive);
        doThrow(FileAlreadyExistsException.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Load failed for databases: 'foo'", commandFailed.getMessage());
        assertEquals("Database already exists: foo", commandFailed.getCause().getMessage());
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabasesDirectoryIsNotWritable() throws IOException, IncorrectFormat {
        createDummyDump("foo", archive);
        doThrow(AccessDeniedException.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Load failed for databases: 'foo'", commandFailed.getMessage());
        assertEquals(
                "You do not have permission to load the database 'foo'.",
                commandFailed.getCause().getMessage());
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectFormat {
        createDummyDump("foo", archive);
        doThrow(new FileSystemException("the-message")).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Load failed for databases: 'foo'", commandFailed.getMessage());
        assertEquals(
                "Unable to load database: FileSystemException: the-message",
                commandFailed.getCause().getMessage());
    }

    @Test
    void shouldThrowIfTheArchiveFormatIsInvalid() throws IOException, IncorrectFormat {
        createDummyDump("foo", archive);
        doThrow(IncorrectFormat.class).when(loader).load(any(), any(), any());
        CommandFailedException commandFailed =
                assertThrows(CommandFailedException.class, () -> execute("foo", archive));
        assertEquals("Load failed for databases: 'foo'", commandFailed.getMessage());
        assertThat(commandFailed.getCause().getMessage()).contains(archive.toString());
        assertThat(commandFailed.getCause().getMessage()).contains("valid Neo4j archive");
    }

    @Test
    void infoMustPrintArchiveMetaData() throws IOException {
        when(loader.getMetaData(any())).thenReturn(new Loader.DumpMetaData("ZSTD", "42", "1337"));
        var baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos)) {
            var command =
                    new LoadCommand(new ExecutionContext(homeDir, homeDir, out, out, testDirectory.getFileSystem())) {
                        @Override
                        protected Loader createLoader(FileSystemAbstraction fs) {
                            return loader;
                        }
                    };
            CommandLine.populateCommand(
                    command,
                    "foo",
                    "--info",
                    "--from-path",
                    archive.toAbsolutePath().toString());
            command.execute();
            out.flush();
        }
        String output = baos.toString();
        assertThat(output).contains("ZSTD", "42", "1337");
    }

    @Test
    void shouldPrintWarningIfLoadingSystemDatabase() throws IOException {
        createDummyDump(SYSTEM_DATABASE_NAME, archive);
        execute(SYSTEM_DATABASE_NAME, archive);

        assertThat(LoadCommand.SYSTEM_ERR_MESSAGE).isEqualToIgnoringNewLines(output.toString());
    }

    private void execute(String database, Path archive) {
        var command = buildCommand();
        CommandLine.populateCommand(command, "--from-path=" + archive, database);
        command.execute();
    }

    private LoadCommand buildCommand() {
        PrintStream out = mock(PrintStream.class);
        PrintStream err = new PrintStream(output);
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        return new LoadCommand(new ExecutionContext(homeDir, configDir, out, err, fileSystem)) {
            @Override
            protected Loader createLoader(FileSystemAbstraction fs) {
                return loader;
            }
        };
    }

    private static void createDummyDump(String databaseName, Path destinationPath) throws IOException {
        Files.writeString(
                destinationPath.resolve(databaseName + ".dump"),
                "ignored",
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
    }
}
