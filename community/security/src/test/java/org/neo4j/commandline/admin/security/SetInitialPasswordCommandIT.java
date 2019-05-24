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
package org.neo4j.commandline.admin.security;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SetInitialPasswordCommandIT
{
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private File confDir;
    private File homeDir;
    private PrintStream out;
    private PrintStream err;

    @Before
    public void setup()
    {
        File graphDir = new File( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
    }

    @After
    public void tearDown() throws Exception
    {
        fileSystem.close();
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );

        verify( out ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );
        executeCommand( "muchBetter" );
        assertAuthIniFile( "muchBetter" );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldWorkWithSamePassword() throws Throwable
    {
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldErrorIfRealUsersAlreadyExistCommunity() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.write( authFile );

        // When
        try
        {
            executeCommand( "will-be-ignored" );
            fail("must fail");
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString(
                    "the provided initial password was not set because existing Neo4j users were detected" ) );
        }

        // Then
        assertNoAuthIniFile();
    }

    @Test
    public void shouldErrorIfRealUsersAlreadyExistEnterprise() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        File rolesFile = getAuthFile( "roles" );

        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.write( authFile );
        fileSystem.write( rolesFile );

        // When
        try
        {
            executeCommand( "will-be-ignored" );
            fail( "must fail" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString(
                    "the provided initial password was not set because existing Neo4j users were detected" ) );
        }

        // Then
        assertNoAuthIniFile();
    }

    @Test
    public void shouldErrorIfRealUsersAlreadyExistV2() throws Throwable
    {
        // Given
        // Create an `auth` file with the default neo4j user, but not the default password
        executeCommand( "not-the-default-password" );
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.renameFile( getAuthFile( "auth.ini" ), authFile );

        // When
        try
        {
            executeCommand( "will-be-ignored" );
            fail( "must fail" );
        }
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "the provided initial password was not set because existing Neo4j users were detected" ) );
        }

        // Then
        assertNoAuthIniFile();
        verify( out ).println( "Changed password for user 'neo4j'." ); // This is from the initial setup
    }

    @Test
    public void shouldNotErrorIfOnlyTheUnmodifiedDefaultNeo4jUserAlreadyExists() throws Throwable
    {
        // Given
        // Create an `auth` file with the default neo4j user
        executeCommand( UserManager.INITIAL_PASSWORD );
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.renameFile( getAuthFile( "auth.ini" ), authFile );

        // When
        executeCommand( "should-not-be-ignored" );

        // Then
        assertAuthIniFile( "should-not-be-ignored" );
        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        File authIniFile = getAuthFile( "auth.ini" );
        assertTrue( fileSystem.fileExists( authIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authIniFile, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( UserManager.INITIAL_USER_NAME );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( password ) );
        assertFalse( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }

    private void assertNoAuthIniFile()
    {
        assertFalse( fileSystem.fileExists( getAuthFile( "auth.ini" ) ) );
    }

    private File getAuthFile( String name )
    {
        return new File( new File( new File( homeDir, "data" ), "dbms" ), name );
    }

    private void executeCommand( String password )
    {
        final var ctx = new ExecutionContext( homeDir.toPath(), confDir.toPath(), out, err, fileSystem );
        final var command = new SetInitialPasswordCommand( ctx );
        CommandLine.populateCommand( command, password );
        command.execute();
    }
}
