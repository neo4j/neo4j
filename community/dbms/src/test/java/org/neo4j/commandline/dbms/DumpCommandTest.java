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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;

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
        verify( dumper ).dump( homeDir.resolve( "data/databases/foo.db" ), archive );
    }

    @Test
    public void shouldCalculateTheDatabaseDirectoryFromConfig() throws IOException, CommandFailed, IncorrectUsage
    {
        Files.write( configDir.resolve( "neo4j.conf" ), asList( data_directory.name() + "=/some/data/dir" ) );
        execute( "foo.db" );
        verify( dumper ).dump( eq( Paths.get( "/some/data/dir/databases/foo.db" ) ), any() );
    }

    @Test
    public void shouldCalculateTheArchiveNameIfPassedAnExistingDirectory()
            throws CommandFailed, IncorrectUsage, IOException
    {
        File to = testDirectory.directory( "some-dir" );
        new DumpCommand( homeDir, configDir, dumper ).execute( new String[]{"--database=" + "foo.db", "--to=" + to} );
        verify( dumper ).dump( any( Path.class ), eq( to.toPath().resolve( "foo.db.dump" ) ) );
    }

    @Test
    public void shouldNotCalculateTheArchiveNameIfPassedAnExistingFile()
            throws CommandFailed, IncorrectUsage, IOException
    {
        Files.createFile( archive );
        execute( "foo.db" );
        verify( dumper ).dump( homeDir.resolve( "data/databases/foo.db" ), archive );
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
        doThrow( new FileAlreadyExistsException( "the-archive-path" ) ).when( dumper ).dump( any(), any() );
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
        doThrow( new NoSuchFileException( homeDir.resolve( "data/databases/foo.db" ).toString() ) )
                .when( dumper ).dump( any(), any() );
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
        doThrow( new NoSuchFileException( archive.getParent().toString() ) ).when( dumper ).dump( any(), any() );
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
    public void
    shouldWrapIOExceptionsCarefulllyBecauseCriticalInformationIsOftenEncodedInTheirNameButMissingFromTheirMessage()
            throws IOException, IncorrectUsage
    {
        doThrow( new IOException( "the-message" ) ).when( dumper ).dump( any(), any() );
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

    private void execute( final String database ) throws IncorrectUsage, CommandFailed
    {
        new DumpCommand( homeDir, configDir, dumper )
                .execute( new String[]{"--database=" + database, "--to=" + archive} );
    }
}
