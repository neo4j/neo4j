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

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.BasicAuthManagerFactory;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.User;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.assertion.Assert.assertException;

public class SetPasswordCommandTest extends UsersCommandTestBase
{

    private UsersCommand usersCommand;
    private Config config;
    private File file;

    @Before
    @Override
    public void setup()
    {
        super.setup();
        usersCommand = new UsersCommand( homeDir.toPath(), confDir.toPath(), out );
        config = usersCommand.loadNeo4jConfig();
        file = BasicAuthManagerFactory.getInitialUserRepositoryFile( config );
    }

    @Test
    public void shouldFailSetPasswordWithNoArguments() throws Exception
    {
        String[] arguments = {"set-password"};
        assertException( () -> usersCommand.execute( arguments ), IncorrectUsage.class, "username" );
    }

    @Test
    public void shouldFailSetPasswordWithOnlyOneArgument() throws Exception
    {
        String[] arguments = {"set-password", "neo4j"};
        assertException( () -> usersCommand.execute( arguments ), IncorrectUsage.class, "password" );
    }

    @Test
    public void shouldAllowSetPasswordWithPasswordChangeRequiredFlag() throws Exception
    {
        String[] arguments = {"set-password", "neo4j", "123", "--requires-password-change"};
        usersCommand.execute( arguments );
    }

    @Test
    public void shouldCreateInitialUserWithPasswordChangeRequired() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( file ) );

        // When
        String[] arguments = {"set-password", "another", "123", "--requires-password-change=false "};
        usersCommand.execute( arguments );

        // Then
        assertTrue( fileSystem.fileExists( file ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, file, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( "another" );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( "123" ) );
        assertTrue( !neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Test
    public void shouldCreateInitialUserFile() throws Throwable
    {
        // Given
        assertFalse( fileSystem.fileExists( file ) );

        // When
        String[] arguments = {"set-password", "neo4j", "123"};
        usersCommand.execute( arguments );

        // Then
        assertTrue( fileSystem.fileExists( file ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, file, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( "neo4j" );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( "123" ) );
        assertTrue( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }

    @Test
    public void shouldFailToCreateInitialUserFileIfExists() throws Throwable
    {
        // Given
        fileSystem.mkdirs( file.getParentFile() );
        fileSystem.create( file );

        // When
        String[] arguments = {"set-password", "neo4j", "123"};
        assertException( () -> usersCommand.execute( arguments ), IncorrectUsage.class,
                "Initial user already set. Overwrite this user with --force" );
    }

    @Test
    public void shouldCreateInitialUserFileEvenIfExistsWhenForced() throws Throwable
    {
        // Given
        fileSystem.mkdirs( file.getParentFile() );
        fileSystem.create( file );

        // When
        String[] arguments = {"set-password", "neo4j", "123", "--force"};
        usersCommand.execute( arguments );

        // Then
        assertTrue( fileSystem.fileExists( file ) );
        FileUserRepository userRepository = new FileUserRepository( fileSystem, file, NullLogProvider.getInstance() );
        userRepository.start();
        User neo4j = userRepository.getUserByName( "neo4j" );
        assertNotNull( neo4j );
        assertTrue( neo4j.credentials().matchesPassword( "123" ) );
        assertTrue( neo4j.hasFlag( User.PASSWORD_CHANGE_REQUIRED ) );
    }
}
