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

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class UsersCommandIT extends UsersCommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Test
    public void shouldGetUsageErrorsWithNoSubCommand() throws Throwable
    {
        // When running 'users' with no subcommand, expect usage errors
        assertFailedUserCommand( null,
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
        assertSuccessfulSubCommand( "list", args(), "neo4j" );
    }

    @Test
    public void shouldListNewUser() throws Throwable
    {
        // Given a new user
        createTestUser( "another", "neo4j" );

        // When running 'list', expect only new user (default not created if a new user is created first)
        assertSuccessfulSubCommand( "list", args(), "another" );
    }

    @Test
    public void shouldListSpecifiedUser() throws Throwable
    {
        // Given a new user
        createTestUser( "another", "neo4j" );

        // When running 'list' with filter, expect specified user
        assertSuccessfulSubCommand( "list", args("other"), "another" );
    }

    //
    // Tests for set-password command
    //

    @Test
    public void shouldGetUsageErrorsWithSetPasswordCommandAndNoArgs() throws Throwable
    {
        // When running 'set-password' with arguments, expect usage errors
        assertFailedUserCommand( "set-password",
                "Missing arguments: 'users set-password' expects username and password arguments",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldNotSetPasswordOnNonExistentUser() throws Throwable
    {
        // Given no previously existing user

        // When running 'set-password' with correct parameters, expect an error
        assertFailedSubCommand( "set-password", args("another", "abc"), "User 'another' does not exist" );
    }

    @Test
    public void shouldSetPasswordOnDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulSubCommand( "set-password", args("neo4j", "abc"), "Changed password for user 'neo4j'" );

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
        assertSuccessfulSubCommand( "set-password", args("another", "abc"), "Changed password for user 'another'" );

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
        assertSuccessfulSubCommand( "set-password", args("another", "abc"), "Changed password for user 'another'" );

        // And password change is no longer required
        assertUserDoesNotRequirePasswordChange( "another" );

        // Then when running another password set to same password expect error
        assertFailedSubCommand( "set-password", args("another", "abc"), "Old password and new password cannot be the same" );
    }

    @Test
    public void shouldSetPasswordOnNewUserAndChangeToAnotherPassword() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'set-password' with correct parameters, expect correct output
        assertSuccessfulSubCommand( "set-password", args(args("another", "abc")), "Changed password for user 'another'" );

        // And password change is no longer required
        assertUserDoesNotRequirePasswordChange( "another" );

        // And then when changing to a different password, expect correct output
        assertSuccessfulSubCommand( "set-password", args("another", "123"), "Changed password for user 'another'" );
    }

    //
    // Tests for create command
    //

    @Test
    public void shouldGetUsageErrorsWithCreateCommandAndNoArgs() throws Throwable
    {
        // When running 'create' with arguments, expect usage errors
        assertFailedUserCommand( "create",
                "Missing arguments: 'users create' expects username and password arguments",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldCreateMissingUserWithoutPasswordChangeRequired() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulSubCommand( "create", args("another", "abc"), "Created new user 'another'" );

        // And the user requires password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateMissingUserWithPasswordChangeRequired() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulSubCommand( "create", args("another", "abc", "--requires-password-change=true"), "Created new user 'another'" );

        // And the user requires password change
        assertUserRequiresPasswordChange( "another" );
    }

    @Test
    public void shouldCreateMissingUserWithPasswordChangeRequiredFalse() throws Throwable
    {
        // Given no previously existing user

        // When running 'create' with correct parameters, expect success
        assertSuccessfulSubCommand( "create", args("another", "abc", "--requires-password-change=false"), "Created new user 'another'" );

        // And the user requires password change
        assertUserDoesNotRequirePasswordChange( "another" );
    }

    @Test
    public void shouldNotCreateDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'create' with correct parameters, expect error
        assertFailedSubCommand( "create", args("neo4j", "abc"), "The specified user 'neo4j' already exists" );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );
        assertUserRequiresPasswordChange( "another" );

        // When running 'create' with correct parameters, expect correct output
        assertFailedSubCommand( "create", args("another", "abc"), "The specified user 'another' already exists" );

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
        assertFailedUserCommand( "delete",
                "Missing arguments: 'users delete' expects username argument",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Runs several possible sub-commands for managing the native users" );
    }

    @Test
    public void shouldNotDeleteMissingUser() throws Throwable
    {
        // Given no previously existing user

        // When running 'delete' with correct parameters, expect error
        assertFailedSubCommand( "delete", args("another"), "User 'another' does not exist" );
    }

    @Test
    public void shouldDeleteDefaultUser() throws Throwable
    {
        // Given default state (only default user)

        // When running 'delete' with correct parameters, expect success
        assertSuccessfulSubCommand( "delete", args("neo4j"), "Deleted user 'neo4j'" );
    }

    @Test
    public void shouldDeleteExistingUser() throws Throwable
    {
        // Given a user that requires password change
        createTestUser( "another", "neo4j" );

        // When running 'create' with correct parameters, expect correct output
        assertSuccessfulSubCommand( "delete", args("another"), "Deleted user 'another'" );
    }

    //
    // Utilities for testing AdminTool
    //

    private void assertFailedUserCommand( String command, String... errors )
    {
        // When running users command without a command or with incorrect command
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, fileSystem, true );
        if ( command == null )
        {
            tool.execute( graphDir.toPath(), confDir.toPath(), "users" );
        }
        else
        {
            tool.execute( graphDir.toPath(), confDir.toPath(), "users", command );
        }

        // Then we get the expected error
        for ( String error : errors )
        {
            verify( out ).stdErrLine( contains( error ) );
        }
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).exit( 1 );
    }
}
