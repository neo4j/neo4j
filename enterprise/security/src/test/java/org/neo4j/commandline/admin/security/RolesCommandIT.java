/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin.security;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthManagerFactory;

import static java.util.Arrays.stream;
import static java.util.stream.Stream.concat;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.commandline.admin.security.RolesCommand.loadNeo4jConfig;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

public class RolesCommandIT extends RolesCommandTestBase
{
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDir );

    @Before
    public void setUp() throws Throwable
    {
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();

        JobScheduler jobScheduler = mock(JobScheduler.class);

        // create default roles
        new EnterpriseAuthManagerFactory().newInstance(
                loadNeo4jConfig( homeDir, configDir ), NullLogProvider.getInstance(),
                NullLog.getInstance(), fileSystem, jobScheduler ).start();
    }

    @Test
    public void shouldGetUsageErrorsWithNoSubCommand() throws Throwable
    {
        // When running 'users' with no subcommand, expect usage errors
        assertFailedRolesCommand( null,
                "Missing arguments: expected at least one sub-command as argument",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles repository" );
    }

    //
    // Tests for list command
    //

    @Test
    public void shouldListDefaultRoles() throws Throwable
    {
        // Given only default roles

        // When running 'list', expect nothing except default roles
        assertSuccessWithDefaultRoles( "list", args() );
    }

    @Test
    public void shouldListNewRole() throws Throwable
    {
        // Given a new role
        createTestRole( "test_role" );

        // When running 'list', expect default roles as well as new role
        assertSuccessWithDefaultRoles( "list", args(), "test_role" );
    }

    @Test
    public void shouldListSpecifiedRole() throws Throwable
    {
        // Given default roles

        // When running 'list' with filter, expect subset of roles
        assertSuccessfulSubCommand( "list", args("ad"), "admin", "reader" );
    }

    //
    // Tests for create command
    //

    @Test
    public void shouldGetUsageErrorsWithCreateCommandAndNoArgs() throws Throwable
    {
        // When running 'create' with arguments, expect usage errors
        assertFailedRolesCommand( "create",
                "Missing arguments: 'roles create' expects roleName argument",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldCreateNewRole() throws Throwable
    {
        // Given no previously existing role

        // When running 'create' with correct parameters, expect success
        assertSuccessfulSubCommand( "create", args("another"), "Created new role 'another'" );

        // And the user requires password change
        assertSuccessWithDefaultRoles( "list", args(), "another" );
    }

    @Test
    public void shouldNotCreateDefaultRole() throws Throwable
    {
        // Given default state

        // When running 'create' with correct parameters, expect error
        assertFailedSubCommand( "create", args("architect"), "The specified role 'architect' already exists" );
    }

    @Test
    public void shouldNotCreateExistingRole() throws Throwable
    {
        // Given a custom pre-existing role
        createTestRole( "another_role" );

        // When running 'create' with correct parameters, expect correct output
        assertFailedSubCommand( "create", args("another_role"), "The specified role 'another_role' already exists" );
    }

    //
    // Tests for delete command
    //

    @Test
    public void shouldGetUsageErrorsWithDeleteCommandAndNoArgs() throws Throwable
    {
        assertFailedRolesCommand( "delete",
                "Missing arguments: 'roles delete' expects roleName argument",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldNotDeleteNonexistentRole() throws Throwable
    {
        // Given default state

        // When running 'delete' with correct parameters, expect error
        assertFailedSubCommand( "delete", args( "another" ), "Role 'another' does not exist" );
    }

    @Test
    public void shouldDeleteCustomRole() throws Throwable
    {
        createTestRole( "test_role" );

        // When running 'delete' with correct parameters, expect success
        assertSuccessfulSubCommand( "delete", args( "test_role" ), "Deleted role 'test_role'" );
    }

    @Test
    public void shouldNotDeletePredefinedRole()
    {
        // given default test

        assertFailedSubCommand( "delete", args( "admin" ), "'admin' is a predefined role and can not be deleted" );
    }

    //
    // Tests for assign command
    //

    @Test
    public void shouldGetUsageErrorsWithAssignCommandAndNoArgs() throws Throwable
    {
        assertFailedRolesCommand( "assign",
                "Missing arguments: 'roles assign' expects roleName and username arguments",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldGetUsageErrorsWithAssignCommandAndNoUsername() throws Throwable
    {
        assertFailedSubCommand( "assign", args("reader"),
                "Missing arguments: 'roles assign' expects roleName and username arguments",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldNotAssignNonexistentRole() throws Throwable
    {
        // Given default state

        // When running 'assign' with correct parameters, expect error
        assertFailedSubCommand( "assign", args( "another", "neo4j" ), "Role 'another' does not exist" );
    }

    @Test
    public void shouldNotAssignToNonexistentUser() throws Throwable
    {
        // Given default state

        // When running 'assign' with correct parameters, expect error
        assertFailedSubCommand( "assign", args( "reader", "another" ), "User 'another' does not exist" );
    }

    @Test
    public void shouldAssignCustomRole() throws Throwable
    {
        createTestRole( "test_role" );
        createTestUser( "another", "abc" );

        // When running 'assign' with correct parameters, expect success
        assertSuccessfulSubCommand( "assign", args( "test_role", "another" ), "Assigned role 'test_role' to user 'another'" );
    }

    //
    // Tests for remove command
    //

    @Test
    public void shouldGetUsageErrorsWithRemoveCommandAndNoArgs() throws Throwable
    {
        assertFailedRolesCommand( "remove",
                "Missing arguments: 'roles remove' expects roleName and username arguments",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldGetUsageErrorsWithRemoveCommandAndNoUsername() throws Throwable
    {
        assertFailedSubCommand( "remove", args("reader"),
                "Missing arguments: 'roles remove' expects roleName and username arguments",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldNotRemoveNonexistentRole() throws Throwable
    {
        // Given default state

        // When running 'remove' with correct parameters, expect error
        assertFailedSubCommand( "remove", args( "another", "neo4j" ), "Role 'another' does not exist" );
    }

    @Test
    public void shouldNotRemoveFromNonexistentUser() throws Throwable
    {
        // Given default state

        // When running 'remove' with correct parameters, expect error
        assertFailedSubCommand( "remove", args( "reader", "another" ), "User 'another' does not exist" );
    }

    @Test
    public void shouldAssignAndRemoveCustomRole() throws Throwable
    {
        createTestRole( "test_role" );
        createTestUser( "another", "abc" );

        // When running 'remove' on non-assigned role, expect error
        assertFailedSubCommand( "remove", args( "test_role", "another" ), "Role 'test_role' was not assigned to user 'another'" );
        // When running 'assign' with correct parameters, expect success
        assertSuccessfulSubCommand( "assign", args( "test_role", "another" ), "Assigned role 'test_role' to user 'another'" );
        // When running 'assign' on already assigned role, expect error
        assertFailedSubCommand( "assign", args( "test_role", "another" ), "Role 'test_role' was already assigned to user 'another'" );
        // When running 'remove' with correct parameters, expect success
        assertSuccessfulSubCommand( "remove", args( "test_role", "another" ), "Removed role 'test_role' from user 'another'" );
        // When running 'assign' on already assigned role, expect error
        assertFailedSubCommand( "remove", args( "test_role", "another" ), "Role 'test_role' was not assigned to user 'another'" );
    }

    //
    // Tests for 'for' and 'users' commands
    //

    @Test
    public void shouldGetUsageErrorsWithForCommandAndNoArgs() throws Throwable
    {
        assertFailedRolesCommand( "for",
                "Missing arguments: 'roles for' expects username argument",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldGetUsageErrorsWithUsersCommandAndNoArgs() throws Throwable
    {
        assertFailedRolesCommand( "users",
                "Missing arguments: 'roles users' expects roleName argument",
                "neo4j-admin roles <subcommand> [<roleName>] [<username>]",
                "Runs several possible sub-commands for managing the native roles" );
    }

    @Test
    public void shouldNotListUsersForNonexistentRole() throws Throwable
    {
        // Given default state

        // When running 'for' with correct parameters, expect error
        assertFailedSubCommand( "for", args( "another" ), "User 'another' does not exist" );
    }

    @Test
    public void shouldNotListRolesForNonexistentUser() throws Throwable
    {
        // Given default state

        // When running 'users' with correct parameters, expect error
        assertFailedSubCommand( "users", args( "another" ), "Role 'another' does not exist" );
    }

    @Test
    public void shouldListDefaultRolesAssignments() throws Throwable
    {
        assertSuccessfulSubCommand( "for", args( "neo4j" ), "admin" );
        assertSuccessfulSubCommand( "users", args( "admin" ), "neo4j" );
        assertSuccessfulSubCommand( "users", args( "reader" ) );
        assertSuccessfulSubCommand( "users", args( "publisher" ) );
        assertSuccessfulSubCommand( "users", args( "architect" ) );
    }

    @Test
    public void shouldListCustomRoleAssignments() throws Throwable
    {
        createTestRole( "test_role" );
        createTestUser( "another", "abc" );

        // When running 'assign' with correct parameters, expect success
        assertSuccessfulSubCommand( "assign", args( "test_role", "another" ), "Assigned role 'test_role' to user 'another'" );
        // When running 'for' on already assigned user
        assertSuccessfulSubCommand( "for", args( "another" ), "test_role" );
        // When running 'for' on already assigned user
        assertSuccessfulSubCommand( "users", args( "test_role" ), "another" );
    }

    //
    // Utilities for testing AdminTool
    //

    private void assertFailedRolesCommand( String command, String... errors )
    {
        // When running users command without a command or with incorrect command
        Path homeDir = testDir.graphDbDir().toPath();
        Path configDir = testDir.directory( "conf" ).toPath();
        OutsideWorld out = mock( OutsideWorld.class );
        AdminTool tool = new AdminTool( CommandLocator.fromServiceLocator(), out, true );
        if ( command == null )
        {
            tool.execute( homeDir, configDir, "roles" );
        }
        else
        {
            tool.execute( homeDir, configDir, "roles", command );
        }

        // Then we get the expected error
        for ( String error : errors )
        {
            verify( out ).stdErrLine( contains( error ) );
        }
        verify( out, times( 0 ) ).stdOutLine( anyString() );
        verify( out ).exit( 1 );
    }

    private void assertSuccessWithDefaultRoles( String command, String[] args, String... messages )
    {
        assertSuccessfulSubCommand( command, args, concat( stream( messages ),
                Stream.of( ADMIN, ARCHITECT, PUBLISHER, READER ) ).toArray( String[]::new ) );
    }

    @Override
    protected String command()
    {
        return "roles";
    }
}
