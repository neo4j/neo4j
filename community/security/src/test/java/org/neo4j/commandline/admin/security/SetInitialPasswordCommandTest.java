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
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;
import org.neo4j.server.security.auth.UserManager;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.neo4j.test.assertion.Assert.assertException;

public class SetInitialPasswordCommandTest
{
    private SetInitialPasswordCommand setPasswordCommand;
    private File authInitFile;
    private FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory(fileSystem);

    @Before
    public void setup()
    {
        OutsideWorld mock = Mockito.mock( OutsideWorld.class );
        when(mock.fileSystem()).thenReturn( fileSystem );
        setPasswordCommand = new SetInitialPasswordCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock );
        authInitFile = CommunitySecurityModule.getInitialUserRepositoryFile( setPasswordCommand.loadNeo4jConfig() );
    }

    @Test
    public void shouldFailSetPasswordWithNoArguments() throws Exception
    {
        assertException( () -> setPasswordCommand.execute( new String[0] ), IncorrectUsage.class,
                "No password specified." );
    }

    @Test
    public void shouldFailSetPasswordWithTooManyArguments() throws Exception
    {
        String[] arguments = {"", "123", "321"};
        assertException( () -> setPasswordCommand.execute( arguments ), IncorrectUsage.class, "Too many arguments." );
    }

    @Test
    public void shouldSetInitialPassword() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( authInitFile ) );

        // When
        String[] arguments = {"123"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    public void shouldOverwriteInitialPasswordFileIfExists() throws Throwable
    {
        // Given
        fileSystem.mkdirs( authInitFile.getParentFile() );
        fileSystem.create( authInitFile );

        // When
        String[] arguments = {"123"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "123" );
    }

    @Test
    public void shouldWorkAlsoWithSamePassword() throws Throwable
    {
        String[] arguments = {"neo4j"};
        setPasswordCommand.execute( arguments );

        // Then
        assertAuthIniFile( "neo4j" );
    }

    private void assertAuthIniFile( String password ) throws Throwable
    {
        assertTrue( fileSystem.fileExists( authInitFile ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, authInitFile,
                NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( UserManager.INITIAL_USER_NAME );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( password ) );
        assertFalse( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }
}
