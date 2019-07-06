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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SetDefaultAdminCommandIT
{
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
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

    @Test
    void shouldSetDefaultAdmin() throws Throwable
    {
        insertUser( "jane", false );
        execute( "jane" );
        assertAdminIniFile( "jane" );

        verify( out ).println( "default admin user set to 'jane'" );
    }

    @Test
    void shouldSetDefaultAdminForInitialUser() throws Throwable
    {
        insertUser( "jane", true );
        execute( "jane" );
        assertAdminIniFile( "jane" );

        verify( out ).println( "default admin user set to 'jane'" );
    }

    @Test
    void shouldOverwrite() throws Throwable
    {
        insertUser( "jane", false );
        insertUser( "janette", false );
        execute( "jane" );
        assertAdminIniFile( "jane" );
        execute( "janette" );
        assertAdminIniFile( "janette" );

        verify( out ).println( "default admin user set to 'jane'" );
        verify( out ).println( "default admin user set to 'janette'" );
    }

    @Test
    void shouldErrorWithNoSuchUser()
    {
        var e = assertThrows( CommandFailedException.class, () -> execute( "bob" ) );
        assertThat( e.getMessage(), containsString( "no such user: 'bob'" ) );
        verify( out, never() ).println( anyString() );
    }

    @Test
    void shouldIgnoreInitialUserIfUsersExist() throws Throwable
    {
        insertUser( "jane", false );
        insertUser( "janette", true );
        execute( "jane" );
        assertAdminIniFile( "jane" );

        var e = assertThrows( CommandFailedException.class, () -> execute( "janette" ) );
        assertThat( e.getMessage(), containsString( "no such user: 'janette'" ) );

        verify( out ).println( "default admin user set to 'jane'" );
    }

    private void insertUser( String username, boolean initial ) throws Throwable
    {
        File userFile = getAuthFile( initial ? CommunitySecurityModule.INITIAL_USER_STORE_FILENAME :
                CommunitySecurityModule.USER_STORE_FILENAME );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, userFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        userRepository.create( new User.Builder( username, LegacyCredential.INACCESSIBLE ).build() );
        Assertions.assertTrue( userRepository.getAllUsernames().contains( username ) );
        userRepository.stop();
        userRepository.shutdown();
    }

    private void assertAdminIniFile( String username ) throws Throwable
    {
        File adminIniFile = getAuthFile( SetDefaultAdminCommand.ADMIN_INI );
        Assertions.assertTrue( fileSystem.fileExists( adminIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, adminIniFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.getAllUsernames(), containsInAnyOrder( username ) );
        userRepository.stop();
        userRepository.shutdown();
    }

    private File getAuthFile( String name )
    {
        return new File( new File( new File( homeDir, "data" ), "dbms" ), name );
    }

    private void execute( String username )
    {
        final var command = new SetDefaultAdminCommand( new ExecutionContext( homeDir.toPath(), confDir.toPath(), out, err, fileSystem ) );
        CommandLine.populateCommand( command, username );
        command.execute();
    }
}
