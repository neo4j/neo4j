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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.internal.StoreLocker;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
import static org.neo4j.dbms.archive.TestUtils.withPermissions;

public class DumpCommandTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private Path homeDir;
    private Path configDir;
    private Path archive;
    private Dumper dumper;

    @Before
    public void setUp() throws Exception
    {
        homeDir = testDirectory.directory( "home-dir" ).toPath();
        configDir = testDirectory.directory( "config-dir" ).toPath();
        archive = testDirectory.file( "some-archive.dump" ).toPath();
        dumper = mock( Dumper.class );
    }

    @Test
    public void shouldDumpTheDatabaseToTheArchive() throws CommandFailed, IncorrectUsage, IOException
    {
        execute( "foo.db" );
        verify( dumper ).dump( eq( homeDir.resolve( "data/databases/foo.db" ) ), eq( archive ), any() );
    }

    @Test
    public void shouldCalculateTheDatabaseDirectoryFromConfig() throws IOException, CommandFailed, IncorrectUsage
    {
        Path dataDir = testDirectory.directory( "some-other-path" ).toPath();
        Path databaseDir = dataDir.resolve( "databases/foo.db" );
        Files.createDirectories( databaseDir );
        Files.write( configDir.resolve( "neo4j.conf" ),
                asList( format( "%s=%s", data_directory.name(), dataDir.toString().replace( '\\', '/' ) ) ) );

        execute( "foo.db" );
        verify( dumper ).dump( eq( databaseDir ), any(), any() );
    }

    @Test
    public void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory()
            throws CommandFailed, IncorrectUsage, IOException
    {
        File to = testDirectory.directory( "some-dir" );
        new DumpCommand( homeDir, configDir, dumper ).execute( new String[]{"--database=" + "foo.db", "--to=" + to} );
        verify( dumper ).dump( any( Path.class ), eq( to.toPath().resolve( "foo.db.dump" ) ), any() );
    }

    @Test
    public void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile()
            throws CommandFailed, IncorrectUsage, IOException
    {
        Files.createFile( archive );
        execute( "foo.db" );
        verify( dumper ).dump( any(), eq( archive ), any() );
    }

    @Test
    public void shouldRespectTheStoreLock() throws IOException, IncorrectUsage, CommandFailed
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        try ( StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() ) )
        {
            storeLocker.checkLock( databaseDirectory.toFile() );

            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "the database is in use -- stop Neo4j and try again" ) );
        }
    }

    @Test
    public void shouldReleaseTheStoreLockAfterDumping() throws IOException, IncorrectUsage, CommandFailed
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        execute( "foo.db" );
        assertCanLockStore( databaseDirectory );
    }

    @Test
    public void shouldReleaseTheStoreLockEvenIfThereIsAnError() throws IOException, IncorrectUsage
    {
        doThrow( IOException.class ).when( dumper ).dump( any(), any(), any() );
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        try
        {
            execute( "foo.db" );
        }
        catch ( CommandFailed ignored )
        {
        }

        assertCanLockStore( databaseDirectory );
    }

    @Test
    public void shouldNotAccidentallyCreateTheDatabaseDirectoryAsASideEffectOfStoreLocking()
            throws CommandFailed, IncorrectUsage, IOException
    {
        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );

        doAnswer( ignored ->
        {
            assertThat( Files.exists( databaseDirectory ), equalTo( false ) );
            return null;
        } ).when( dumper ).dump( any(), any(), any() );

        execute( "foo.db" );
    }

    @Test
    public void shouldReportAHelpfulErrorIfWeDontHaveWritePermissionsForLock() throws IOException, IncorrectUsage
    {
        assumeFalse( "We haven't found a way to reliably tests permissions on Windows", SystemUtils.IS_OS_WINDOWS );

        Path databaseDirectory = homeDir.resolve( "data/databases/foo.db" );
        Files.createDirectories( databaseDirectory );

        try ( StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() ) )
        {
            storeLocker.checkLock( databaseDirectory.toFile() );

            try ( Closeable ignored = withPermissions( databaseDirectory.resolve( StoreLocker.STORE_LOCK_FILENAME ),
                    emptySet() ) )
            {
                execute( "foo.db" );
                fail( "expected exception" );
            }
            catch ( CommandFailed e )
            {
                assertThat( e.getMessage(), equalTo( "you do not have permission to dump the database -- is Neo4j " +
                        "running as a different user?" ) );
            }
        }
    }

    @Test
    public void shouldExcludeTheStoreLockFromTheArchiveToAvoidProblemsWithReadingLockedFilesOnWindows()
            throws CommandFailed, IncorrectUsage, IOException
    {
        doAnswer( invocation ->
        {
            //noinspection unchecked
            Predicate<Path> exclude = (Predicate<Path>) invocation.getArgumentAt( 2, Predicate.class );
            assertThat( exclude.test( Paths.get( StoreLocker.STORE_LOCK_FILENAME ) ), is( true ) );
            assertThat( exclude.test( Paths.get( "some-other-file" ) ), is( false ) );
            return null;
        } ).when( dumper ).dump( any(), any(), any() );

        execute( "foo.db" );
    }

    @Test
    public void shouldObjectIfTheDatabaseArgumentIsMissing() throws CommandFailed
    {
        try
        {
            new DumpCommand( null, null, null ).execute( new String[]{"--to=something"} );
            fail( "expected exception" );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), equalTo( "Missing argument 'database'" ) );
        }
    }

    @Test
    public void shouldObjectIfTheArchiveArgumentIsMissing() throws CommandFailed
    {
        try
        {
            new DumpCommand( homeDir, configDir, null ).execute( new String[]{"--database=something"} );
            fail( "expected exception" );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), equalTo( "Missing argument 'to'" ) );
        }
    }

    @Test
    public void shouldGiveAClearErrorIfTheArchiveAlreadyExists() throws IOException, IncorrectUsage
    {
        doThrow( new FileAlreadyExistsException( "the-archive-path" ) ).when( dumper ).dump( any(), any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "archive already exists: the-archive-path" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheDatabaseDoesntExist() throws IOException, IncorrectUsage
    {
        doThrow( new NoSuchFileException( homeDir.resolve( "data/databases/foo.db" ).toString() ) ).when( dumper )
                .dump( any(), any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "database does not exist: foo.db" ) );
        }
    }

    @Test
    public void shouldGiveAClearMessageIfTheArchivesParentDoesntExist() throws IOException, IncorrectUsage
    {
        doThrow( new NoSuchFileException( archive.getParent().toString() ) ).when( dumper ).dump( any(), any(), any() );
        try
        {
            execute( "foo.db" );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(),
                    equalTo( "unable to dump database: NoSuchFileException: " + archive.getParent() ) );
        }
    }

    @Test
    public void shouldWrapIOExceptionsCarefulllyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectUsage
    {
        doThrow( new IOException( "the-message" ) ).when( dumper ).dump( any(), any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), equalTo( "unable to dump database: IOException: the-message" ) );
        }
    }

    private void execute( final String database ) throws IncorrectUsage, CommandFailed, AccessDeniedException
    {
        new DumpCommand( homeDir, configDir, dumper )
                .execute( new String[]{"--database=" + database, "--to=" + archive} );
    }

    private void assertCanLockStore( Path databaseDirectory ) throws IOException
    {
        try ( StoreLocker storeLocker = new StoreLocker( new DefaultFileSystemAbstraction() ) )
        {
            storeLocker.checkLock( databaseDirectory.toFile() );
        }
    }
}
