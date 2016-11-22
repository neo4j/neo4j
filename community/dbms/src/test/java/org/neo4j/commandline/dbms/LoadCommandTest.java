/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
import java.nio.file.Paths;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.StoreLocker;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;

public class LoadCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Loader loader;

    @Before
    public void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        configDir = testDirectory.directory( "config-dir" ).toPath();
        archive = testDirectory.directory( "some-archive.dump" ).toPath();
        loader = mock( Loader.class );
    }

    @Test
    public void shouldLoadTheDatabaseFromTheArchive() throws CommandFailed, IncorrectUsage, IOException, IncorrectFormat
    {
        execute( "foo.db" );
        verify( loader ).load( archive, homeDir.resolve( "data/databases/foo.db" ) );
    }

    @Test
    public void shouldCalculateTheDatabaseDirectoryFromConfig()
            throws IOException, CommandFailed, IncorrectUsage, IncorrectFormat
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );
        Files.createDirectories( databaseDir );
        Files.write( configDir.resolve( "neo4j.conf" ),
                asList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );

        execute( "foo.db" );
        verify( loader ).load( any(), eq( databaseDir ) );
    }

    @Test
    public void shouldHandleSymlinkToDatabaseDir() throws IOException, CommandFailed, IncorrectUsage, IncorrectFormat
    {
        assumeFalse( "Can't reliably create symlinks on windows", SystemUtils.IS_OS_WINDOWS );

        Path symDir = testDirectory.directory( "path-to-links" ).toPath();
        Path realDatabaseDir = symDir.resolve( "foo.db" );

        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );

        Files.createDirectories( realDatabaseDir );
        Files.createDirectories( dataDir.resolve( "databases" ) );

        Files.createSymbolicLink( databaseDir, realDatabaseDir );

        Files.write( configDir.resolve( "neo4j.conf" ),
                asList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );

        execute( "foo.db" );
        verify( loader ).load( any(), eq( realDatabaseDir ) );
    }

    @Test
    public void shouldMakeFromCanonical() throws IOException, CommandFailed, IncorrectUsage, IncorrectFormat
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );
        Files.createDirectories( databaseDir );
        Files.write( configDir.resolve( "neo4j.conf" ),
                asList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );

        new LoadCommand( homeDir, configDir, loader )
                .execute( ArrayUtil.concat( new String[]{"--database=foo.db", "--from=foo.dump"} ) );

        verify( loader ).load( eq( Paths.get( new File( "foo.dump" ).getCanonicalPath() ) ), any() );
    }

    @Test
    public void shouldDeleteTheOldDatabaseIfForceArgumentIsProvided()
            throws CommandFailed, IncorrectUsage, IOException, IncorrectFormat
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ), equalTo( false ) );
            return null;
        } ).when( loader ).load( any(), any() );

        execute( "foo.db", "--force" );
    }

    @Test
    public void shouldNotDeleteTheOldDatabaseIfForceArgumentIsNotProvided()
            throws CommandFailed, IncorrectUsage, IOException, IncorrectFormat
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ), equalTo( true ) );
            return null;
        } ).when( loader ).load( any(), any() );

        execute( "foo.db" );
    }

    @Test
    public void shouldRespectTheStoreLock() throws IOException, IncorrectUsage
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              StoreLocker locker = new StoreLocker( fileSystem ) )
        {
            locker.checkLock( databaseDirectory.toFile() );
            execute( "foo.db", "--force" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "the database is in use -- stop Neo4j and try again" ) );
        }
    }

    @Test
    public void shouldDefaultToGraphDb() throws Exception
    {
        Path databaseDir = homeDir.resolve( "data/databases/graph.db" );
        Files.createDirectories( databaseDir );

        new LoadCommand( homeDir, configDir, loader ).execute( new String[]{"--from=something"} );
        verify( loader ).load( any(), eq( databaseDir ) );
    }

    @Test
    public void shouldObjectIfTheArchiveArgumentIsMissing() throws Exception
    {
        try
        {
            new LoadCommand( homeDir, configDir, loader ).execute( new String[]{"--database=something"} );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), equalTo( "Missing argument 'from'" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheArchiveDoesntExist() throws IOException, IncorrectFormat, IncorrectUsage
    {
        doThrow( new NoSuchFileException( archive.toString() ) ).when( loader ).load( any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "archive does not exist: " + archive ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheDatabaseAlreadyExists() throws IOException, IncorrectFormat, IncorrectUsage
    {
        doThrow( FileAlreadyExistsException.class ).when( loader ).load( any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "database already exists: foo.db" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheDatabasesDirectoryIsNotWritable()
            throws IOException, IncorrectFormat, IncorrectUsage
    {
        doThrow( AccessDeniedException.class ).when( loader ).load( any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo(
                    "you do not have permission to load a database -- is Neo4j running " + "as a different user?" ) );
        }
    }

    @Test
    public void shouldWrapIOExceptionsCarefulllyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectUsage, IncorrectFormat
    {
        doThrow( new FileSystemException( "the-message" ) ).when( loader ).load( any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "unable to load database: FileSystemException: the-message" ) );
        }
    }

    @Test
    public void shouldThrowIfTheArchiveFormatIsInvalid() throws IOException, IncorrectUsage, IncorrectFormat
    {
        doThrow( IncorrectFormat.class ).when( loader ).load( any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), containsString( archive.toString() ) );
            assertThat( e.getMessage(), containsString( "valid Neo4j archive" ) );
        }
    }

    @Test
    public void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new LoadCommand.Provider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin load --from=<archive-path> [--database=<name>]%n" +
                            "                        [--force[=<true|false>]]%n" +
                            "%n" +
                            "Load a database from an archive. <archive-path> must be an archive created with%n" +
                            "the dump command. <database> is the name of the database to create. Existing%n" +
                            "databases can be replaced by specifying --force. It is not possible to replace a%n" +
                            "database that is mounted in a running Neo4j server.%n" +
                            "%n" +
                            "options:%n" +
                            "  --from=<archive-path>   Path to archive created with the dump command.%n" +
                            "  --database=<name>       Name of database. [default:graph.db]%n" +
                            "  --force=<true|false>    If an existing database should be replaced.%n" +
                            "                          [default:false]%n" ),
                    baos.toString() );
        }
    }

    private void execute( String database, String... otherArgs ) throws IncorrectUsage, CommandFailed
    {
        new LoadCommand( homeDir, configDir, loader )
                .execute( ArrayUtil.concat( new String[]{"--database=" + database, "--from=" + archive}, otherArgs ) );
    }
}
