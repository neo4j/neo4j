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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith( EphemeralFileSystemExtension.class )
class SetInitialPasswordCommandIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    private File confDir;
    private File homeDir;
    private PrintStream out;
    private PrintStream err;

    @BeforeEach
    void setup()
    {
        File graphDir = new File( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
        out = mock( PrintStream.class );
        err = mock( PrintStream.class );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fileSystem.close();
    }

    @Test
    void shouldSetPassword() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );

        verify( out ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc" );
        executeCommand( "muchBetter" );
        assertAuthIniFile( "muchBetter" );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldWorkWithSamePassword() throws Throwable
    {
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j" );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistCommunity() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.write( authFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage(), containsString( "the provided initial password was not set because existing Neo4j users were detected" ) );

        // Then
        assertNoAuthIniFile();
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistEnterprise() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        File rolesFile = getAuthFile( "roles" );

        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.write( authFile );
        fileSystem.write( rolesFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage(), containsString(
                    "the provided initial password was not set because existing Neo4j users were detected" ) );

        // Then
        assertNoAuthIniFile();
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistV2() throws Throwable
    {
        // Given
        // Create an `auth` file with the default neo4j user, but not the default password
        executeCommand( "not-the-default-password" );
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.renameFile( getAuthFile( "auth.ini" ), authFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage(), containsString( "the provided initial password was not set because existing Neo4j users were detected" ) );

        // Then
        assertNoAuthIniFile();
        verify( out ).println( "Changed password for user 'neo4j'." ); // This is from the initial setup
    }

    @Test
    void shouldNotErrorIfOnlyTheUnmodifiedDefaultNeo4jUserAlreadyExists() throws Throwable
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
