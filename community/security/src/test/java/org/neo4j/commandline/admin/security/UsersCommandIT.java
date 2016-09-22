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
import org.junit.rules.RuleChain;

public class UsersCommandIT extends UsersCommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

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
        assertFailedUserCommand( "", new String[0],
                "Missing arguments: expected at least one sub-command as argument",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    //
    // Tests for list command
    //

    @Test
    public void shouldListDefaultUser() throws Throwable
    {
        // Given only default user

        // When running 'list', expect default user
        assertSuccessfulUserCommand( "list", args(), "neo4j" );
    }

    @Test
    public void shouldListNewUser() throws Throwable
    {
        // Given a new user
        createTestUser( "another", "neo4j" );

        // When running 'list', expect only new user (default not created if a new user is created first)
        assertSuccessfulUserCommand( "list", args(), "another" );
    }

    @Test
    public void shouldListSpecifiedUser() throws Throwable
    {
        // Given a new user
        createTestUser( "another", "neo4j" );

        // When running 'list' with filter, expect specified user
        assertSuccessfulUserCommand( "list", args("other"), "another" );
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
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldNotSetPasswordOnNonExistentUser() throws Throwable
    {
        // Given no previously existing user

        // When running 'set-password' with correct parameters, expect an error
        assertFailedUserCommand( "set-password", args("another", "abc"), "User 'another' does not exist" );
    }

    @Test
    public void shouldSetPasswordOnDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args("neo4j", "abc"), "Changed password for user 'neo4j'" );

        // And the user no longer requires password change
        assertUserDoesNotRequirePasswordChange( "neo4j" );
    }

    @Test
    public void shouldSetPasswordOnNewUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args("another", "abc"), "Changed password for user 'another'" );

        // And the user no longer requires password change
        assertUserDoesNotRequirePasswordChange( "another" );
    }

    @Test
    public void shouldSetPasswordOnNewUserButNotChangeToSamePassword() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args("another", "abc"), "Changed password for user 'another'" );

        // And password change is no longer required
        assertUserDoesNotRequirePasswordChange( "another" );

        // Then when running another password set to same password expect error
        assertFailedUserCommand( "set-password", args("another", "abc"), "Old password and new password cannot be the same" );
    }

    @Test
    public void shouldSetPasswordOnNewUserAndChangeToAnotherPassword() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "set-password", args(args("another", "abc")), "Changed password for user 'another'" );

        // And password change is no longer required
        assertUserDoesNotRequirePasswordChange( "another" );

        // And then when changing to a different password, expect correct output
        assertSuccessfulUserCommand( "set-password", args("another", "123"), "Changed password for user 'another'" );
    }

    //
    // Tests for create command
    //

    @Test
    public void shouldGetUsageErrorsWithCreateCommandAndNoArgs() throws Throwable
    {
        // When running 'create' with arguments, expect usage errors
        assertFailedUserCommand( "create", new String[0],
                "Missing arguments: 'users create' expects username and password arguments",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldCreateMissingUserWithoutPasswordChangeRequired() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulUserCommand( "create", args("another", "abc"), "Created new user 'another'" );

        // And the user requires password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateMissingUserWithPasswordChangeRequired() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulUserCommand( "create", args("another", "abc", "--requires-password-change=true"), "Created new user 'another'" );

        // And the user requires password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateMissingUserWithPasswordChangeRequiredFalse() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulUserCommand( "create", args("another", "abc", "--requires-password-change=false"), "Created new user 'another'" );

        // And the user requires password change
        assertUserDoesNotRequirePasswordChange( "another" );
    }

    @Test
    public void shouldNotCreateDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'create' with correct parameters, expect error
        assertFailedUserCommand( "create", args("neo4j", "abc"), "The specified user 'neo4j' already exists" );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'create' with correct parameters, expect correct output
        assertFailedUserCommand( "create", args("another", "abc"), "The specified user 'another' already exists" );

        // And the user still requires password change
        assertUserRequiresPasswordChange( "another" );
    }

    //
    // Tests for delete command
    //

    @Test
    public void shouldGetUsageErrorsWithDeleteCommandAndNoArgs() throws Throwable
    {
        // When running 'create' with arguments, expect usage errors
        assertFailedUserCommand( "delete", new String[0],
                "Missing arguments: 'users delete' expects username argument",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldNotDeleteMissingUser() throws Throwable
    {
        // Given no previously existing user

        // When running 'delete' with correct parameters, expect error
        assertFailedUserCommand( "delete", args("another"), "User 'another' does not exist" );
    }

    @Test
    public void shouldNotDeleteDefaultUserIfNoOtherUserExists() throws Throwable
    {
        // Given default state (only default user)

        // When running 'delete' with correct parameters, expect error
        assertFailedUserCommand( "delete", args("neo4j"), "Deleting the only remaining user 'neo4j' is not allowed" );
    }

    @Test
    public void shouldDeleteDefaultUserIfAnotherUserExists() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );

        // When running 'delete' with correct parameters, expect success
        assertSuccessfulUserCommand( "delete", args("neo4j"), "Deleted user 'neo4j'" );
    }

    @Test
    public void shouldNotDeleteExistingUserIfNoOtherUserExists() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );

        // When running 'delete' with correct parameters, expect success
        assertSuccessfulUserCommand( "delete", args("neo4j"), "Deleted user 'neo4j'" );

        // When running 'delete' with correct parameters, expect error
        assertFailedUserCommand( "delete", args("another"), "Deleting the only remaining user 'another' is not allowed" );
    }

    @Test
    public void shouldDeleteExistingUser() throws Throwable
    {
        // Given the default user and another new user
        createTestUser( "another", "neo4j" );

        // When running 'create' with correct parameters, expect correct output
        assertSuccessfulUserCommand( "delete", args("another"), "Deleted user 'another'" );
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
