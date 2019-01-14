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

import java.io.File;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.BlockerLocator;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SetInitialPasswordCommandIT
{
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private File confDir;
    private File homeDir;
    private OutsideWorld out;
    private AdminTool tool;

    private static final String SET_PASSWORD = "set-initial-password";

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

    @After
    public void tearDown() throws Exception
    {
        fileSystem.close();
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "abc" );
        assertAuthIniFile( "abc" );

        verify( out ).stdOutLine( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "abc" );
        assertAuthIniFile( "abc" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "muchBetter" );
        assertAuthIniFile( "muchBetter" );

        verify( out, times( 2 ) ).stdOutLine( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldWorkWithSamePassword() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "neo4j" );
        assertAuthIniFile( "neo4j" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "neo4j" );
        assertAuthIniFile( "neo4j" );

        verify( out, times( 2 ) ).stdOutLine( "Changed password for user 'neo4j'." );
    }

    @Test
    public void shouldGetUsageOnWrongArguments1()
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD );
        assertNoAuthIniFile();

        verify( out ).stdErrLine( "not enough arguments" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( "usage: neo4j-admin set-initial-password <password>" );
        verify( out ).stdErrLine( String.format( "environment variables:" ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_CONF    Path to directory which contains neo4j.conf." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_DEBUG   Set to anything to enable debug output." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_HOME    Neo4j home directory." ) );
        verify( out ).stdErrLine( String.format( "    HEAP_SIZE     Set JVM maximum heap size during command execution." ) );
        verify( out ).stdErrLine( String.format( "                  Takes a number and a unit, for example 512m." ) );
        verify( out ).stdErrLine( "Sets the initial password of the initial admin user ('neo4j')." );
        verify( out ).exit( 1 );
        verifyNoMoreInteractions( out );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldGetUsageOnWrongArguments2()
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "foo", "bar" );
        assertNoAuthIniFile();

        verify( out ).stdErrLine( "unrecognized arguments: 'bar'" );
        verify( out, times( 3 ) ).stdErrLine( "" );
        verify( out ).stdErrLine( "usage: neo4j-admin set-initial-password <password>" );
        verify( out ).stdErrLine( String.format( "environment variables:" ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_CONF    Path to directory which contains neo4j.conf." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_DEBUG   Set to anything to enable debug output." ) );
        verify( out ).stdErrLine( String.format( "    NEO4J_HOME    Neo4j home directory." ) );
        verify( out ).stdErrLine( String.format( "    HEAP_SIZE     Set JVM maximum heap size during command execution." ) );
        verify( out ).stdErrLine( String.format( "                  Takes a number and a unit, for example 512m." ) );

        verify( out ).stdErrLine( "Sets the initial password of the initial admin user ('neo4j')." );
        verify( out ).exit( 1 );
        verifyNoMoreInteractions( out );
        verify( out, never() ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorIfRealUsersAlreadyExistCommunity() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.create( authFile );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "will-be-ignored" );

        // Then
        assertNoAuthIniFile();
        verify( out, times( 1 ) )
                .stdErrLine( "command failed: the provided initial password was not set because existing Neo4j users were " +
                        "detected at `" + authFile.getAbsolutePath() + "`. Please remove the existing `auth` file if you " +
                        "want to reset your database to only have a default user with the provided password." );
        verify( out ).exit( 1 );
        verify( out, times( 0 ) ).stdOutLine( anyString() );
    }

    @Test
    public void shouldErrorIfRealUsersAlreadyExistEnterprise() throws Throwable
    {
        // Given
        File authFile = getAuthFile( "auth" );
        File rolesFile = getAuthFile( "roles" );

        fileSystem.mkdirs( authFile.getParentFile() );
        fileSystem.create( authFile );
        fileSystem.create( rolesFile );

        // When
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "will-be-ignored" );

        // Then
        assertNoAuthIniFile();
        verify( out, times( 1 ) )
                .stdErrLine( "command failed: the provided initial password was not set because existing Neo4j users were " +
                        "detected at `" + authFile.getAbsolutePath() + "`. Please remove the existing `auth` and `roles` files if you " +
                        "want to reset your database to only have a default user with the provided password." );
        verify( out ).exit( 1 );
        verify( out, never() ).stdOutLine( anyString() );
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

    private void resetOutsideWorldMock()
    {
        reset(out);
        when( out.fileSystem() ).thenReturn( fileSystem );
    }
}
