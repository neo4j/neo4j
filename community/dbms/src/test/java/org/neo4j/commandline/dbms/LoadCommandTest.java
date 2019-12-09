/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

@Neo4jLayoutExtension
class LoadCommandTest
{
    @Inject
    private TestDirectory testDirectory;
    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Loader loader;

    @BeforeEach
    void setUp() throws IOException
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        prepareFooDatabaseDirectory();
        configDir = testDirectory.directory( "config-dir" ).toPath();
        archive = testDirectory.directory( "some-archive.dump" ).toPath();
        loader = mock( Loader.class );
    }

    private void prepareFooDatabaseDirectory() throws IOException
    {
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath() )
                .set( default_database, "foo" )
                .build();
        File databaseDirectory  = DatabaseLayout.of( config ).databaseDirectory();
        testDirectory.getFileSystem().mkdirs( databaseDirectory );
    }

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new LoadCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ), loader );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Load a database from an archive created with the dump command.%n" +
                        "%n" +
                        "USAGE%n" +
                        "%n" +
                        "load [--force] [--verbose] [--database=<database>] --from=<path>%n" +
                        "%n" +
                        "DESCRIPTION%n" +
                        "%n" +
                        "Load a database from an archive. <archive-path> must be an archive created with%n" +
                        "the dump command. <database> is the name of the database to create. Existing%n" +
                        "databases can be replaced by specifying --force. It is not possible to replace%n" +
                        "a database that is mounted in a running Neo4j server.%n" + "%n" + "OPTIONS%n" + "%n" +
                        "      --verbose       Enable verbose output.%n" + "      --from=<path>   Path to archive created with the dump command.%n" +
                        "      --database=<database>%n" + "                      Name of the database to load.%n" + "                        Default: neo4j%n" +
                        "      --force         If an existing database should be replaced." ) );
    }

    @Test
    void shouldLoadTheDatabaseFromTheArchive() throws CommandFailedException, IOException, IncorrectFormat
    {
        execute( "foo", archive );
        DatabaseLayout databaseLayout = createDatabaseLayout( homeDir.resolve( "data/databases" ), "foo",
                homeDir.resolve( "data/" + DEFAULT_TX_LOGS_ROOT_DIR_NAME ) );
        verify( loader ).load( archive, databaseLayout );
    }

    @Test
    void shouldCalculateTheDatabaseDirectoryFromConfig()
            throws IOException, CommandFailedException, IncorrectFormat
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Path transactionLogsDir = dataDir.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        Files.createDirectories( databaseDir );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ), singletonList( formatProperty( data_directory, dataDir ) ) );

        execute( "foo", archive );
        DatabaseLayout databaseLayout = createDatabaseLayout( databaseDir.getParent(), "foo", transactionLogsDir );
        verify( loader ).load( any(), eq( databaseLayout ) );
    }

    @Test
    void shouldCalculateTheTxLogDirectoryFromConfig() throws Exception
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path txLogsDir = testDirectory.directory( "txLogsPath" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ),
                asList( formatProperty( data_directory, dataDir ),
                        formatProperty( transaction_logs_root_path, txLogsDir ) ) );

        execute( "foo", archive );
        DatabaseLayout databaseLayout = createDatabaseLayout( databaseDir.getParent(), "foo", txLogsDir );
        verify( loader ).load( any(), eq( databaseLayout ) );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void shouldHandleSymlinkToDatabaseDir() throws IOException, CommandFailedException, IncorrectFormat
    {
        Path symDir = testDirectory.directory( "path-to-links" ).toPath();
        Path realDatabaseDir = symDir.resolve( "foo" );

        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo" );
        Path txLogsDir = dataDir.resolve( DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        Path databasesDir = dataDir.resolve( "databases" );

        Files.createDirectories( realDatabaseDir );
        Files.createDirectories( databasesDir );

        Files.createSymbolicLink( databaseDir, realDatabaseDir );

        Files.write( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ), singletonList( formatProperty( data_directory, dataDir ) ) );

        execute( "foo", archive );
        DatabaseLayout databaseLayout = createDatabaseLayout( databasesDir, "foo", txLogsDir );
        verify( loader ).load( any(), eq( databaseLayout ) );
    }

    @Test
    void shouldDeleteTheOldDatabaseIfForceArgumentIsProvided()
            throws CommandFailedException, IOException, IncorrectFormat
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo" );
        Path marker = databaseDirectory.resolve( "marker" );
        Path txDirectory = homeDir.resolve( "data/" + DEFAULT_TX_LOGS_ROOT_DIR_NAME );
        Files.createDirectories( databaseDirectory );
        Files.createDirectories( txDirectory );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( marker ) ).isEqualTo( false );
            return null;
        } ).when( loader ).load( any(), any() );

        executeForce( "foo" );
    }

    @Test
    void shouldNotDeleteTheOldDatabaseIfForceArgumentIsNotProvided()
            throws CommandFailedException, IOException, IncorrectFormat
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo" ).toAbsolutePath();
        Files.createDirectories( databaseDirectory );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ) ).isEqualTo( true );
            return null;
        } ).when( loader ).load( any(), any() );

        execute( "foo", archive );
    }

    @Test
    void shouldRespectTheDatabaseLock() throws IOException
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo" );
        Files.createDirectories( databaseDirectory );
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDirectory.toFile() );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              Locker locker = new DatabaseLocker( fileSystem, databaseLayout ) )
        {
            locker.checkLock();
            CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> executeForce( "foo" ) );
            assertEquals( "The database is in use. Stop database 'foo' and try again.", commandFailed.getMessage() );
        }
    }

    @Test
    void shouldGiveAClearMessageIfTheArchiveDoesntExist() throws IOException, IncorrectFormat
    {
        doThrow( new NoSuchFileException( archive.toString() ) ).when( loader ).load( any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo", archive ) );
        assertEquals( "Archive does not exist: " + archive, commandFailed.getMessage() );
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabaseAlreadyExists() throws IOException, IncorrectFormat
    {
        doThrow( FileAlreadyExistsException.class ).when( loader ).load( any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo", archive ) );
        assertEquals( "Database already exists: foo", commandFailed.getMessage() );
    }

    @Test
    void shouldGiveAClearMessageIfTheDatabasesDirectoryIsNotWritable()
            throws IOException, IncorrectFormat
    {
        doThrow( AccessDeniedException.class ).when( loader ).load( any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo", archive ) );
        assertEquals( "You do not have permission to load the database.", commandFailed.getMessage() );
    }

    @Test
    void shouldWrapIOExceptionsCarefullyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectFormat
    {
        doThrow( new FileSystemException( "the-message" ) ).when( loader ).load( any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo", archive ) );
        assertEquals( "Unable to load database: FileSystemException: the-message", commandFailed.getMessage() );
    }

    @Test
    void shouldThrowIfTheArchiveFormatIsInvalid() throws IOException, IncorrectFormat
    {
        doThrow( IncorrectFormat.class ).when( loader ).load( any(), any() );
        CommandFailedException commandFailed = assertThrows( CommandFailedException.class, () -> execute( "foo", archive ) );
        assertThat( commandFailed.getMessage() ).contains( archive.toString() );
        assertThat( commandFailed.getMessage() ).contains( "valid Neo4j archive" );
    }

    private DatabaseLayout createDatabaseLayout( Path storePath, String databaseName, Path transactionLogsPath )
    {
        Config config = Config.newBuilder()
                .set( neo4j_home, homeDir.toAbsolutePath() )
                .set( databases_root_path, storePath.toAbsolutePath() )
                .set( transaction_logs_root_path, transactionLogsPath.toAbsolutePath() )
                .build();
        return Neo4jLayout.of( config ).databaseLayout( databaseName );
    }

    private void execute( String database, Path archive )
    {
        final var command = new LoadCommand( new ExecutionContext( homeDir, configDir, mock( PrintStream.class ), mock( PrintStream.class ),
                testDirectory.getFileSystem() ), loader );
        CommandLine.populateCommand( command,
                "--from=" + archive,
                "--database=" + database );
        command.execute();
    }

    private void executeForce( String database )
    {
        final var command = new LoadCommand( new ExecutionContext( homeDir, configDir, mock( PrintStream.class ), mock( PrintStream.class ),
                testDirectory.getFileSystem() ), loader );
        CommandLine.populateCommand( command,
                "--from=" + archive.toAbsolutePath(),
                "--database=" + database,
                "--force");
        command.execute();
    }

    private static String formatProperty( Setting setting, Path path )
    {
        return format( "%s=%s", setting.name(), path.toString().replace( '\\', '/' ) );
    }
}
