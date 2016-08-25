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
import org.neo4j.commandline.admin.OutsideWorld;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CreateCommandTest extends CommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldFailToCreateExistingUser() throws Throwable
    {
        // Given - existing user
        createTestUser( "another", "abc" );

        // When - trying to create it again
        try
        {
            File graphDir = testDir.graphDbDir();
            File confDir = new File( graphDir, "conf" );
            OutsideWorld out = mock( OutsideWorld.class );
            UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
            usersCommand.execute( new String[]{"create", "another", "abc"} );
            fail( "Should not have succeeded without exception" );
        }
        catch ( CommandFailed e )
        {
            // Expect failure message
            assertThat( e.getMessage(), containsString( "The specified user 'another' already exists" ) );
        }
    }

    @Test
    public void shouldCreateNewUser() throws Throwable
    {
        // Given - no existing user

        // When - creating new user
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        OutsideWorld out = mock( OutsideWorld.class );
        UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
        usersCommand.execute( new String[]{"create", "another", "abc"} );

        // Then - the specified user is found
        verify( out ).stdOutLine( "Created new user 'another'" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );

        // And does not require password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateNewUserWithRequiresPasswordChange() throws Throwable
    {
        // Given - no existing user

        // When - creating new user with password change requirement
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        OutsideWorld out = mock( OutsideWorld.class );
        UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
        usersCommand.execute( new String[]{"create", "another", "abc", "--requires-password-change=true"} );

        // Then - the specified user is found
        verify( out ).stdOutLine( "Created new user 'another'" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );

        // And does not require password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateNewUserWithRequiresPasswordChangeAlternativeOder() throws Throwable
    {
        // Given - no existing user

        // When - creating new user with password change requirement
        File graphDir = testDir.graphDbDir();
        File confDir = new File( graphDir, "conf" );
        OutsideWorld out = mock( OutsideWorld.class );
        UsersCommand usersCommand = new UsersCommand( graphDir.toPath(), confDir.toPath(), out );
        usersCommand.execute( new String[]{"create", "--requires-password-change=true", "another", "abc"} );

        // Then - the specified user is found
        verify( out ).stdOutLine( "Created new user 'another'" );
        verify( out, times( 1 ) ).stdOutLine( anyString() );

        // And does not require password change
        assertUserRequiresPasswordChange( "another" );
    }
}
