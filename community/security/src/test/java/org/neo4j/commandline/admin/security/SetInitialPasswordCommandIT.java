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
package org.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserManager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
        tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
    }

    @Test
    public void shouldSetPassword() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "abc" );
        verify( out ).stdOutLine( "Changed password for user 'neo4j'." );
        assertAuthIniFile( "abc" );
    }

    @Test
    public void shouldOverwriteIfSetPasswordAgain() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "abc" );
        assertAuthIniFile( "abc" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "muchBetter" );
        verify( out, times( 2 ) ).stdOutLine( "Changed password for user 'neo4j'." );
        assertAuthIniFile( "muchBetter" );
    }

    @Test
    public void shouldWorkWithSamePassword() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "neo4j" );
        assertAuthIniFile( "neo4j" );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "neo4j" );
        verify( out, times( 2 ) ).stdOutLine( "Changed password for user 'neo4j'." );
        assertAuthIniFile( "neo4j" );
    }

    @Test
    public void shouldGetUsageOnWrongArguments() throws Throwable
    {
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD );
        tool.execute( homeDir.toPath(), confDir.toPath(), SET_PASSWORD, "foo", "bar" );
        verify( out, times( 2 ) ).stdErrLine( "neo4j-admin set-initial-password <password>" );
        verify( out, times( 0 ) ).stdOutLine( anyString() );
    }

    private void assertAuthIniFile(String password) throws Throwable
    {
        File authIniFile = new File( new File( new File( homeDir, "data" ), "dbms" ), "auth.ini" );
        assertTrue( fileSystem.fileExists( authIniFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authIniFile, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( UserManager.INITIAL_USER_NAME );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( password ) );
        assertFalse( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }

    private void resetOutsideWorldMock()
    {
        reset(out);
        when( out.fileSystem() ).thenReturn( fileSystem );
    }
}
