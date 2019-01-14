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

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.BlockerLocator;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SetDefaultAdminCommandIT
{
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private File confDir;
    private File homeDir;
    private OutsideWorld out;
    private AdminTool tool;

    private static final String SET_ADMIN = "set-default-admin";

    @Before
    public void setup()
    {
        File graphDir = new File( "graph-db" );
        confDir = new File( graphDir, "conf" );
        homeDir = new File( graphDir, "home" );
        out = mock( OutsideWorld.class );
        resetOutsideWorldMock();
        tool = new AdminTool( CommandLocator.fromServiceLocator(), BlockerLocator.fromServiceLocator(), out, true );
    }

    @Test
    public void shouldSetDefaultAdmin() throws Throwable
    {
        insertUser( "jane", false );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "jane" );
        assertAdminIniFile( "jane" );

        verify( out ).stdOutLine( "default admin user set to 'jane'" );
    }

    @Test
    public void shouldSetDefaultAdminForInitialUser() throws Throwable
    {
        insertUser( "jane", true );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "jane" );
        assertAdminIniFile( "jane" );

        verify( out ).stdOutLine( "default admin user set to 'jane'" );
    }

    @Test
    public void shouldOverwrite() throws Throwable
    {
        insertUser( "jane", false );
        insertUser( "janette", false );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "jane" );
        assertAdminIniFile( "jane" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "janette" );
        assertAdminIniFile( "janette" );

        verify( out ).stdOutLine( "default admin user set to 'jane'" );
        verify( out ).stdOutLine( "default admin user set to 'janette'" );
    }

    @Test
    public void shouldErrorWithNoSuchUser()
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "bob" );
        verify( out ).stdErrLine( "command failed: no such user: 'bob'" );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldIgnoreInitialUserIfUsersExist() throws Throwable
    {
        insertUser( "jane", false );
        insertUser( "janette", true );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "jane" );
        assertAdminIniFile( "jane" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "janette" );

        verify( out ).stdOutLine( "default admin user set to 'jane'" );
        verify( out ).stdErrLine( "command failed: no such user: 'janette'" );
        verify( out ).exit( 1 );
    }

    @Test
    public void shouldGetUsageOnWrongArguments1()
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN );
        assertNoAuthIniFile();

        verify( out ).stdErrLine( "not enough arguments" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( "usage: neo4j-admin set-default-admin <username>" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( String.format( "environment variables:" ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_CONF    Path to directory which contains neo4j.conf." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_DEBUG   Set to anything to enable debug output." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_HOME    Neo4j home directory." ) );
        verify( out ).stdErrLine( String.format( "    HEAP_SIZE     Set JVM maximum heap size during command execution." ) );
        verify( out ).stdErrLine( String.format( "                  Takes a number and a unit, for example 512m." ) );
        verify( out ).stdErrLine(
                String.format( "Sets the user to become admin if users but no roles are present, for example%n" +
                        "when upgrading to neo4j 3.1 enterprise." ) );
        verify( out ).exit( 1 );
        verifyNoMoreInteractions( out );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldGetUsageOnWrongArguments2()
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_ADMIN, "foo", "bar" );
        assertNoAuthIniFile();

        verify( out ).stdErrLine( "unrecognized arguments: 'bar'" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( "usage: neo4j-admin set-default-admin <username>" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( String.format( "environment variables:" ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_CONF    Path to directory which contains neo4j.conf." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_DEBUG   Set to anything to enable debug output." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_HOME    Neo4j home directory." ) );
        verify( out ).stdErrLine( String.format( "    HEAP_SIZE     Set JVM maximum heap size during command execution." ) );
        verify( out ).stdErrLine( String.format( "                  Takes a number and a unit, for example 512m." ) );
        verify( out ).stdErrLine(
                String.format( "Sets the user to become admin if users but no roles are present, for example%n" +
                        "when upgrading to neo4j 3.1 enterprise." ) );
        verify( out ).exit( 1 );
        verifyNoMoreInteractions( out );
        verify( out, never() ).stdOutLine( anyString() );
    }

    private void insertUser( String username, boolean initial ) throws Throwable
    {
        File userFile = getAuthFile( initial ? CommunitySecurityModule.INITIAL_USER_STORE_FILENAME :
                CommunitySecurityModule.USER_STORE_FILENAME );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, userFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        userRepository.create( new User.Builder( username, Credential.INACCESSIBLE ).build() );
        assertTrue( userRepository.getAllUsernames().contains( username ) );
        userRepository.stop();
        userRepository.shutdown();
    }

    private void assertAdminIniFile( String username ) throws Throwable
    {
        File adminIniFile = getAuthFile( SetDefaultAdminCommand.ADMIN_INI );
        assertTrue( fileSystem.fileExists( adminIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, adminIniFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        assertThat( userRepository.getAllUsernames(), containsInAnyOrder( username ) );
        userRepository.stop();
        userRepository.shutdown();
    }

    private void assertNoAuthIniFile()
    {
        assertFalse( fileSystem.fileExists( getAuthFile( SetDefaultAdminCommand.ADMIN_INI ) ) );
    }

    private File getAuthFile( String name )
    {
        return new File( new File( new File( homeDir, "data" ), "dbms" ), name );
    }

    private void resetOutsideWorldMock()
    {
        reset( out );
        when( out.fileSystem() ).thenReturn( fileSystem );
    }
}
