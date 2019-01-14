/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.test.assertion.Assert.assertException;

public class UserManagementProceduresLoggingTest
{
    private TestUserManagementProcedures authProcedures;
    private AssertableLogProvider log;
    private EnterpriseSecurityContext matsContext;
    private EnterpriseUserManager generalUserManager;

    @Before
    public void setUp() throws Throwable
    {
        log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        authProcedures = new TestUserManagementProcedures();
        authProcedures.graph = mock( GraphDatabaseAPI.class );
        authProcedures.securityLog = securityLog;

        generalUserManager = getUserManager();
        EnterpriseSecurityContext adminContext =
                new EnterpriseSecurityContext( new MockAuthSubject( "admin" ), AccessMode.Static.FULL, Collections.emptySet(), true );
        matsContext =
                new EnterpriseSecurityContext( new MockAuthSubject( "mats" ), AccessMode.Static.NONE, Collections.emptySet(), false );

        setSubject( adminContext );
    }

    private void setSubject( EnterpriseSecurityContext securityContext )
    {
        authProcedures.securityContext = securityContext;
        authProcedures.userManager = new PersonalUserManager( generalUserManager, securityContext.subject(),
                authProcedures.securityLog, securityContext.isAdmin() );
    }

    private EnterpriseUserManager getUserManager() throws Throwable
    {
        InternalFlatFileRealm realm = new InternalFlatFileRealm(
                                            new InMemoryUserRepository(),
                                            new InMemoryRoleRepository(),
                                            new BasicPasswordPolicy(),
                                            mock( AuthenticationStrategy.class ),
                                            mock( JobScheduler.class ),
                                            new InMemoryUserRepository(),
                                            new InMemoryUserRepository()
                                        );
        realm.start(); // creates default user and roles
        return realm;
    }

    @Test
    public void shouldLogCreatingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", true );
        authProcedures.createUser( "mats", "el password", false );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", ", with password change required" ),
                info( "[admin]: created user `%s`%s", "mats", "" )
            );
    }

    @Test
    public void shouldLogFailureToCreateUser()
    {
        catchInvalidArguments( () -> authProcedures.createUser( null, "pw", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "", "pw", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "andres", "", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "mats", null, true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "neo4j", "nonEmpty", true ) );

        log.assertExactly(
                error( "[admin]: tried to create user `%s`: %s", null, "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "", "The provided username is empty." ),
                error( "[admin]: tried to create user `%s`: %s", "andres", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "mats", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "neo4j", "The specified user 'neo4j' already exists." )
        );
    }

    @Test
    public void shouldLogUnauthorizedCreatingUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.createUser( "andres", "", true ) );

        log.assertExactly( error( "[mats]: tried to create user `%s`: %s", "andres", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogDeletingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", false );
        authProcedures.deleteUser( "andres" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "andres", "" ),
                info( "[admin]: deleted user `%s`", "andres" ) );
    }

    @Test
    public void shouldLogDeletingNonExistentUser()
    {
        catchInvalidArguments( () -> authProcedures.deleteUser( "andres" ) );

        log.assertExactly( error( "[admin]: tried to delete user `%s`: %s", "andres", "User 'andres' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedDeleteUser()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.deleteUser( ADMIN ) );

        log.assertExactly( error( "[mats]: tried to delete user `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogAddingRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( ARCHITECT, "mats" );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                info( "[admin]: added role `%s` to user `%s`", ARCHITECT, "mats" ) );
    }

    @Test
    public void shouldLogFailureToAddRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        catchInvalidArguments( () -> authProcedures.addRoleToUser( "null", "mats" ) );

        log.assertExactly(
                info( "[admin]: created user `%s`%s", "mats", "" ),
                error( "[admin]: tried to add role `%s` to user `%s`: %s", "null", "mats", "Role 'null' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedAddingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.addRoleToUser( ADMIN, "mats" ) );

        log.assertExactly( error( "[mats]: tried to add role `%s` to user `%s`: %s", ADMIN, "mats", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogRemovalOfRoleFromUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        authProcedures.removeRoleFromUser( READER, "mats" );

        // Then
        log.assertExactly( info( "[admin]: removed role `%s` from user `%s`", READER, "mats" ) );
    }

    @Test
    public void shouldLogFailureToRemoveRoleFromUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( READER, "mats" );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.removeRoleFromUser( "notReader", "mats" ) );
        catchInvalidArguments( () -> authProcedures.removeRoleFromUser( READER, "notMats" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", "notReader", "mats", "Role 'notReader' does not exist." ),
                error( "[admin]: tried to remove role `%s` from user `%s`: %s", READER, "notMats", "User 'notMats' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedRemovingRole()
    {
        setSubject( matsContext );
        catchAuthorizationViolation( () -> authProcedures.removeRoleFromUser( ADMIN, ADMIN ) );

        log.assertExactly( error( "[mats]: tried to remove role `%s` from user `%s`: %s", ADMIN, ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogUserPasswordChanges() throws IOException, InvalidArgumentsException
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        log.clear();

        // When
        authProcedures.changeUserPassword( "mats", "longPassword", false );
        authProcedures.changeUserPassword( "mats", "longerPassword", true );

        setSubject( matsContext );
        authProcedures.changeUserPassword( "mats", "evenLongerPassword", false );

        authProcedures.changePassword( "superLongPassword", false );
        authProcedures.changePassword( "infinitePassword", true );

        // Then
        log.assertExactly(
                info( "[admin]: changed password for user `%s`%s", "mats", "" ),
                info( "[admin]: changed password for user `%s`%s", "mats", ", with password change required" ),
                info( "[mats]: changed password%s", "" ),
                info( "[mats]: changed password%s", "" ),
                info( "[mats]: changed password%s", ", with password change required" )
        );
    }

    @Test
    public void shouldLogFailureToChangeUserPassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "andres", "neo4j", true );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "andres", "neo4j", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "andres", "", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "notAndres", "good password", false ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to change password for user `%s`: %s",
                        "andres", "Old password and new password cannot be the same." ),
                error( "[admin]: tried to change password for user `%s`: %s",
                        "andres", "A password cannot be empty." ),
                error( "[admin]: tried to change password for user `%s`: %s",
                        "notAndres", "User 'notAndres' does not exist." )
        );
    }

    @Test
    public void shouldLogFailureToChangeOwnPassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        setSubject( matsContext );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "neo4j", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "", false ) );

        catchInvalidArguments( () -> authProcedures.changePassword( null, false ) );
        catchInvalidArguments( () -> authProcedures.changePassword( "", false ) );
        catchInvalidArguments( () -> authProcedures.changePassword( "neo4j", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change password: %s", "Old password and new password cannot be the same." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "A password cannot be empty." ),
                error( "[mats]: tried to change password: %s", "Old password and new password cannot be the same." )
        );
    }

    @Test
    public void shouldLogUnauthorizedChangePassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "andres", "neo4j", true );
        log.clear();
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.changeUserPassword( "andres", "otherPw", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change password for user `%s`: %s", "andres", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogSuspendUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        authProcedures.suspendUser( "mats" );
        authProcedures.suspendUser( "mats" );

        // Then
        log.assertExactly(
                info( "[admin]: suspended user `%s`", "mats" ),
                info( "[admin]: suspended user `%s`", "mats" )
        );
    }

    @Test
    public void shouldLogFailureToSuspendUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.suspendUser( "notMats" ) );
        catchInvalidArguments( () -> authProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to suspend user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to suspend user `%s`: %s", "admin", "Suspending yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    public void shouldLogUnauthorizedSuspendUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to suspend user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogActivateUser() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.suspendUser( "mats" );
        log.clear();

        // When
        authProcedures.activateUser( "mats", false );
        authProcedures.activateUser( "mats", false );

        // Then
        log.assertExactly(
                info( "[admin]: activated user `%s`", "mats" ),
                info( "[admin]: activated user `%s`", "mats" )
        );
    }

    @Test
    public void shouldLogFailureToActivateUser()
    {
        // When
        catchInvalidArguments( () -> authProcedures.activateUser( "notMats", false ) );
        catchInvalidArguments( () -> authProcedures.activateUser( ADMIN, false ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to activate user `%s`: %s", "notMats", "User 'notMats' does not exist." ),
                error( "[admin]: tried to activate user `%s`: %s", ADMIN, "Activating yourself (user 'admin') is not allowed." )
        );
    }

    @Test
    public void shouldLogUnauthorizedActivateUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.activateUser( "admin", true ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to activate user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    @Test
    public void shouldLogCreatingRole() throws Throwable
    {
        // When
        authProcedures.createRole( "role" );

        // Then
        log.assertExactly( info( "[admin]: created role `%s`", "role" ) );
    }

    @Test
    public void shouldLogFailureToCreateRole() throws Throwable
    {
        // Given
        authProcedures.createRole( "role" );
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.createRole( null ) );
        catchInvalidArguments( () -> authProcedures.createRole( "" ) );
        catchInvalidArguments( () -> authProcedures.createRole( "role" ) );
        catchInvalidArguments( () -> authProcedures.createRole( "!@#$" ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to create role `%s`: %s", null, "The provided role name is empty." ),
                error( "[admin]: tried to create role `%s`: %s", "", "The provided role name is empty." ),
                error( "[admin]: tried to create role `%s`: %s", "role", "The specified role 'role' already exists." ),
                error( "[admin]: tried to create role `%s`: %s", "!@#$",
                        "Role name '!@#$' contains illegal characters. Use simple ascii characters and numbers." )
        );
    }

    @Test
    public void shouldLogUnauthorizedCreateRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.createRole( "role" ) );

        // Then
        log.assertExactly( error("[mats]: tried to create role `%s`: %s", "role", PERMISSION_DENIED) );
    }

    @Test
    public void shouldLogDeletingRole() throws Exception
    {
        // Given
        authProcedures.createRole( "foo" );
        log.clear();

        // When
        authProcedures.deleteRole( "foo" );

        // Then
        log.assertExactly( info( "[admin]: deleted role `%s`", "foo" ) );
    }

    @Test
    public void shouldLogFailureToDeleteRole()
    {
        // When
        catchInvalidArguments( () -> authProcedures.deleteRole( null ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( "" ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( "foo" ) );
        catchInvalidArguments( () -> authProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[admin]: tried to delete role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", "foo", "Role 'foo' does not exist." ),
                error( "[admin]: tried to delete role `%s`: %s", ADMIN, "'admin' is a predefined role and can not be deleted." )
        );
    }

    @Test
    public void shouldLogUnauthorizedDeletingRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.deleteRole( ADMIN ) );

        // Then
        log.assertExactly( error( "[mats]: tried to delete role `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogIfUnexpectedErrorTerminatingTransactions() throws Exception
    {
        // Given
        authProcedures.createUser( "johan", "neo4j", false );
        authProcedures.failTerminateTransaction();
        log.clear();

        // When
        assertException( () -> authProcedures.deleteUser( "johan" ), RuntimeException.class, "Unexpected error" );

        // Then
        log.assertExactly(
                info( "[admin]: deleted user `%s`", "johan" ),
                error( "[admin]: failed to terminate running transaction and bolt connections for user `%s` following %s: %s",
                        "johan", "deletion", "Unexpected error" )
        );
    }

    @Test
    public void shouldLogUnauthorizedListUsers()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listUsers() );

        log.assertExactly( error( "[mats]: tried to list users: %s", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogUnauthorizedListRoles()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listRoles() );

        log.assertExactly( error( "[mats]: tried to list roles: %s", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogFailureToListRolesForUser()
    {
        // Given

        // When
        catchInvalidArguments( () -> authProcedures.listRolesForUser( null ) );
        catchInvalidArguments( () -> authProcedures.listRolesForUser( "" ) );
        catchInvalidArguments( () -> authProcedures.listRolesForUser( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list roles for user `%s`: %s", null, "User 'null' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "", "User '' does not exist." ),
                error( "[admin]: tried to list roles for user `%s`: %s", "nonExistent", "User 'nonExistent' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedListRolesForUser()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listRolesForUser( "user" ) );

        log.assertExactly( error( "[mats]: tried to list roles for user `%s`: %s", "user", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogFailureToListUsersForRole()
    {
        // Given

        // When
        catchInvalidArguments( () -> authProcedures.listUsersForRole( null ) );
        catchInvalidArguments( () -> authProcedures.listUsersForRole( "" ) );
        catchInvalidArguments( () -> authProcedures.listUsersForRole( "nonExistent" ) );

        log.assertExactly(
                error( "[admin]: tried to list users for role `%s`: %s", null, "Role 'null' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "", "Role '' does not exist." ),
                error( "[admin]: tried to list users for role `%s`: %s", "nonExistent", "Role 'nonExistent' does not exist." )
        );
    }

    @Test
    public void shouldLogUnauthorizedListUsersForRole()
    {
        // Given
        setSubject( matsContext );

        // When
        catchAuthorizationViolation( () -> authProcedures.listUsersForRole( "role" ) );

        log.assertExactly( error( "[mats]: tried to list users for role `%s`: %s", "role", PERMISSION_DENIED ) );
    }

    private void catchInvalidArguments( ThrowingAction<Exception> f )
    {
        assertException( f, InvalidArgumentsException.class );
    }

    private void catchAuthorizationViolation( ThrowingAction<Exception> f )
    {
        assertException( f, AuthorizationViolationException.class );
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, (Object[]) arguments );
    }

    private static class MockAuthSubject implements AuthSubject
    {
        private final String name;

        private MockAuthSubject( String name )
        {
            this.name = name;
        }

        @Override
        public void logout()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return name.equals( username );
        }

        @Override
        public String username()
            {
                return name;
            }
    }

    private static class TestUserManagementProcedures extends UserManagementProcedures
    {
        private boolean failTerminateTransactions;

        void failTerminateTransaction()
        {
            failTerminateTransactions = true;
        }

        @Override
        protected void terminateTransactionsForValidUser( String username )
        {
            if ( failTerminateTransactions )
            {
                throw new RuntimeException( "Unexpected error" );
            }
        }

        @Override
        protected void terminateConnectionsForValidUser( String username )
        {
        }
    }
}
