/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
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
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;

    @BeforeEach
    void setup()
    {
        Path graphDir = Path.of( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        confDir = graphDir.resolve( "conf" );
        homeDir = graphDir.resolve( "home" );
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
        assertAuthIniFile( "abc", false );

        verify( out ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldSetPasswordWithRequirePasswordChange() throws Throwable
    {
        executeCommand( "abc", "--require-password-change" );
        assertAuthIniFile( "abc", true );

        verify( out ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldSetPasswordWithRequirePasswordChangeOtherOrder() throws Throwable
    {
        executeCommand( "--require-password-change", "abc" );
        assertAuthIniFile( "abc", true );

        verify( out ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        executeCommand( "abc" );
        assertAuthIniFile( "abc", false );
        executeCommand( "muchBetter" );
        assertAuthIniFile( "muchBetter", false );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldWorkWithSamePassword() throws Throwable
    {
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j", false );
        executeCommand( "neo4j" );
        assertAuthIniFile( "neo4j", false );

        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistCommunity() throws Throwable
    {
        // Given
        Path authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParent() );
        fileSystem.write( authFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage() ).contains( "the provided initial password was not set because existing Neo4j users were detected" );

        // Then
        assertNoAuthIniFile();
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistEnterprise() throws Throwable
    {
        // Given
        Path authFile = getAuthFile( "auth" );
        Path rolesFile = getAuthFile( "roles" );

        fileSystem.mkdirs( authFile.getParent() );
        fileSystem.write( authFile );
        fileSystem.write( rolesFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage() ).contains( "the provided initial password was not set because existing Neo4j users were detected" );

        // Then
        assertNoAuthIniFile();
    }

    @Test
    void shouldErrorIfRealUsersAlreadyExistV2() throws Throwable
    {
        // Given
        // Create an `auth` file with the default neo4j user, but not the default password
        executeCommand( "not-the-default-password" );
        Path authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParent() );
        fileSystem.renameFile( getAuthFile( "auth.ini" ), authFile );

        // When
        var e = assertThrows( Exception.class, () -> executeCommand( "will-be-ignored" ) );
        assertThat( e.getMessage() ).contains( "the provided initial password was not set because existing Neo4j users were detected" );

        // Then
        assertNoAuthIniFile();
        verify( out ).println( "Changed password for user 'neo4j'." ); // This is from the initial setup
    }

    @Test
    void shouldNotErrorIfOnlyTheUnmodifiedDefaultNeo4jUserAlreadyExists() throws Throwable
    {
        // Given
        // Create an `auth` file with the default neo4j user
        executeCommand( AuthManager.INITIAL_PASSWORD );
        Path authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParent() );
        fileSystem.renameFile( getAuthFile( "auth.ini" ), authFile );

        // When
        executeCommand( "should-not-be-ignored" );

        // Then
        assertAuthIniFile( "should-not-be-ignored", false );
        verify( out, times( 2 ) ).println( "Changed password for user 'neo4j'." );
    }

    private void assertAuthIniFile( String password, boolean passwordChangeRequired ) throws Throwable
    {
        Path authIniFile = getAuthFile( "auth.ini" );
        assertTrue( fileSystem.fileExists( authIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authIniFile, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( AuthManager.INITIAL_USER_NAME );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( UTF8.encode( password ) ) );
        assertThat( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) ).isEqualTo( passwordChangeRequired );
    }

    private void assertNoAuthIniFile()
    {
        assertFalse( fileSystem.fileExists( getAuthFile( "auth.ini" ) ) );
    }

    private Path getAuthFile( String name )
    {
        return homeDir.resolve( "data" ).resolve( "dbms" ).resolve( name );
    }

    private void executeCommand( String... args )
    {
        final var ctx = new ExecutionContext( homeDir, confDir, out, err, fileSystem );
        final var command = new SetInitialPasswordCommand( ctx );
        CommandLine.populateCommand( command, args );
        command.execute();
    }
}
