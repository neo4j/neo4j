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

public class UsersCommandIT extends UsersCommandTestBase
{
    @Before
    public void setup()
    {
        super.setup();
        // the following line ensures that the test setup code (like creating test users) works on the same initial
        // environment that the actual tested commands will encounter. In particular some auth state is created
        // on demand in both the UserCommand and in the real server. We want that state created before the tests
        // are run.
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( "users", "list" ) );
        resetOutsideWorldMock();
    }

    @Test
    public void shouldGetUsageErrorsWithNoSubCommand() throws Throwable
    {
        // When running 'users' with no subcommand, expect usage errors
        assertFailedUserCommand( "", new String[0], "Missing arguments: expected sub-command argument 'set-password'",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Sets the initial (admin) user." );
    }

    //
    // Tests for set-password command
    //

    @Test
    public void shouldGetUsageErrorsWithSetPasswordCommandAndNoArgs() throws Throwable
    {
        // When running 'set-password' with arguments, expect usage errors
        assertFailedUserCommand( "set-password", new String[0],
                "Missing arguments: 'users set-password' expects username and password arguments",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Sets the initial (admin) user." );
    }

    @Test
    public void shouldNotSetPasswordOnNonExistentUser() throws Throwable
    {
        // Given no previously existing user

        // When running 'set-password' with correct parameters, expect an error
        assertFailedUserCommand( "set-password", args( "another", "abc" ), "User 'another' does not exist" );
    }

    @Test
    public void shouldSetPasswordOnDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args( "neo4j", "abc" ), "Changed password for user 'neo4j'" );

        // And the user requires password change
        assertUsersPasswordMatches( "neo4j", "abc" );
        assertUserRequiresPasswordChange( "neo4j" );
    }

    @Test
    public void shouldSetPasswordWithPasswordChangeRequired() throws Throwable
    {
        // Given default state (only default user)

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args( "neo4j", "abc", "--requires-password-change=true" ),
                "Changed password for user 'neo4j'" );

        // And the user requires password change
        assertUsersPasswordMatches( "neo4j", "abc" );
        assertUserRequiresPasswordChange( "neo4j" );
    }

    @Test
    public void shouldSetPasswordWithPasswordChangeRequiredFalse() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulUserCommand( "set-password", args( "neo4j", "abc", "--requires-password-change=false" ),
                "Changed password for user 'neo4j'" );

        // And the user requires no password change
        assertUsersPasswordMatches( "neo4j", "abc" );
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldSetPasswordOnNewUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args( "another", "abc" ), "Changed password for user 'another'" );

        // And the user requires password change
        assertUsersPasswordMatches( "another", "abc" );
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldSetPasswordOnNewUserButNotChangeToSamePassword() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args( "another", "abc" ), "Changed password for user 'another'" );

        // And password change is required
        assertUsersPasswordMatches( "another", "abc" );
        assertUserRequiresPasswordChange( "another" );

        // Then when running another password set to same password expect error
        assertFailedUserCommand( "set-password", args( "another", "abc" ),
                "Old password and new password cannot be the same" );
    }

    @Test
    public void shouldSetPasswordOnNewUserAndChangeToAnotherPassword() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password",
                args( args( "another", "abc", "--requires-password-change=false" ) ),
                "Changed password for user 'another'" );

        // And password change is no longer required
        assertUsersPasswordMatches( "another", "abc" );
        assertUserDoesNotRequirePasswordChange( "another" );

        // And then when changing to a different password, expect correct output
        assertSuccessfulUserCommand( "set-password", args( "another", "123" ), "Changed password for user 'another'" );
    }

    //
    // Utilities for testing AdminTool
    //

    private void assertSuccessfulUserCommand( String subCommand, String[] args, String... messages )
    {
        assertSuccessfulSubCommand( "users", subCommand, args, messages );
    }

    private void assertFailedUserCommand( String subCommand, String[] args, String... messages )
    {
        assertFailedSubCommand( "users", subCommand, args, messages );
    }
}
