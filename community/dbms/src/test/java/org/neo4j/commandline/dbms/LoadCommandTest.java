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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        Files.write( configDir.resolve( "neo4j.conf" ), asList( data_directory.name() + "=/some/data/dir" ) );
        execute( "foo.db" );
        verify( loader ).load( any(), eq( Paths.get( "/some/data/dir/databases/foo.db" ) ) );
    }

    @Test
    public void shouldObjectIfTheDatabaseArgumentIsMissing() throws CommandFailed
    {
        try
        {
            new LoadCommand( null, null, null ).execute( new String[]{"--from=something"} );
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
            new LoadCommand( homeDir, configDir, null ).execute( new String[]{"--database=something"} );
            fail( "expected exception" );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), equalTo( "Missing argument 'from'" ) );
        }
    }

    @Test
    public void shouldThrowIfTheCommandFails() throws IOException, IncorrectUsage, IncorrectFormat
    {
        doThrow( IOException.class ).when( loader ).load( any(), any() );
        try
        {
            execute( null );
            fail( "expected exception" );
        }
        catch ( CommandFailed ignored )
        {
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

    private void execute( final String database ) throws IncorrectUsage, CommandFailed
    {
        new LoadCommand( homeDir, configDir, loader )
                .execute( new String[]{"--database=" + database, "--from=" + archive} );
    }
}
