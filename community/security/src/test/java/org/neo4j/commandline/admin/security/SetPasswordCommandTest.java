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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class SetPasswordCommandTest extends CommandTestBase
{

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailSetPasswordWithNoArguments() throws Exception
    {
        UsersCommand usersCommand = new UsersCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( OutsideWorld.class ) );

        String[] arguments = {"set-password"};
        try
        {
            usersCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "username" ) );
        }
    }

    @Test
    public void shouldFailSetPasswordWithOnlyOneArgument() throws Exception
    {
        UsersCommand usersCommand = new UsersCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( OutsideWorld.class ) );

        String[] arguments = {"set-password", "neo4j"};
        try
        {
            usersCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "password" ) );
        }
    }

    @Test
    public void shouldFailSetPasswordWithNonExistingUser() throws Exception
    {
        UsersCommand usersCommand = new UsersCommand( testDir.directory( "home" ).toPath(),
                testDir.directory( "conf" ).toPath(), mock( OutsideWorld.class ) );

        String[] arguments = {"set-password", "nosuchuser", "whatever"};
        try
        {
            usersCommand.execute( arguments );
            fail( "Should have thrown an exception." );
        }
        catch ( CommandFailed e )
        {
            assertThat( e.getMessage(), containsString( "does not exist" ) );
        }
    }

    @Test
    public void shouldRunSetPasswordWithDefaultUser() throws Throwable
    {
        // Given - no created user

        // When - the admin command sets the password
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        UsersCommand usersCommand =
                new UsersCommand( graphDir.toPath(), confDir.toPath(), mock( OutsideWorld.class ) );
        usersCommand.execute( new String[]{"set-password", "neo4j", "abc"} );

        // Then - the default user does not require a password change
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldRunSetPasswordWithExistingUser() throws Throwable
    {
        // Given - new user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When - the admin command sets the password
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        UsersCommand usersCommand =
                new UsersCommand( graphDir.toPath(), confDir.toPath(), mock( OutsideWorld.class ) );
        usersCommand.execute( new String[]{"set-password", "another", "abc"} );

        // Then - the new user no longer requires a password change
        assertUserDoesNotRequirePasswordChange( "another" );
    }
}
