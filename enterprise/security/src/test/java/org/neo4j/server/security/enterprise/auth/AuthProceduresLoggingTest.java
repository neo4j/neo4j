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
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.shiro.mgt.SecurityManager;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.impl.enterprise.SecurityLog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.PasswordPolicy;
import org.neo4j.server.security.auth.UserRepository;

import static org.mockito.Mockito.mock;

import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.enterprise.auth.AuthProcedures.PERMISSION_DENIED;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

public class AuthProceduresLoggingTest
{
    private AuthProcedures authProcedures;
    private AssertableLogProvider log = null;
    private AuthSubject matsSubject = null;

    @Before
    public void setUp() throws Throwable
    {
        EnterpriseUserManager userManager = getUserManager();
        AuthSubject adminSubject = new TestAuthSubject( "admin", true, userManager );
        matsSubject = new TestAuthSubject( "mats", false, userManager );

        log = new AssertableLogProvider();
        authProcedures = new TestAuthProcedures();
        authProcedures.authSubject = adminSubject;
        authProcedures.securityLog = new SecurityLog( log.getLog( getClass() ) );
        GraphDatabaseAPI api = mock( GraphDatabaseAPI.class );
        authProcedures.graph = api;
    }

    private static class TestAuthProcedures extends AuthProcedures
    {
        @Override
        void kickoutUser( String username, String reason )
        {
        }

        @Override
        Stream<TransactionTerminationResult> terminateTransactionsForValidUser( String username )
        {
            return Stream.empty();
        }

        @Override
        Stream<ConnectionResult> terminateConnectionsForValidUser( String username )
        {
            return Stream.empty();
        }
    }

    private EnterpriseUserManager getUserManager() throws Throwable
    {
        InMemoryRoleRepository roles = new InMemoryRoleRepository();
        PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
        UserRepository users = new InMemoryUserRepository();
        AuthenticationStrategy authStrategy = mock( AuthenticationStrategy.class );
        InternalFlatFileRealm realm = new InternalFlatFileRealm( users, roles, passwordPolicy, authStrategy );
        realm.start(); // creates default user and roles
        return realm;
    }

    @Test
    public void shouldLogCreatingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", true );
        authProcedures.createUser( "mats", "el password", false );

        log.assertExactly(
                info( "[admin]: created user `%s`", "andres" ),
                info( "[admin]: created user `%s`", "mats" ) );
    }

    @Test
    public void shouldLogCreatingUserWithBadPassword() throws Throwable
    {
        catchInvalidArguments( () -> authProcedures.createUser( "andres", "", true ) );
        catchInvalidArguments( () -> authProcedures.createUser( "mats", null, true ) );

        log.assertExactly(
                error( "[admin]: tried to create user `%s`: %s", "andres", "A password cannot be empty." ),
                error( "[admin]: tried to create user `%s`: %s", "mats", "A password cannot be empty." ) );
    }

    @Test
    public void shouldLogUnauthorizedCreatingUser() throws Throwable
    {
        authProcedures.authSubject = matsSubject;
        catchAuthorizationViolation( () -> authProcedures.createUser( "andres", "", true ) );

        log.assertExactly( error( "[mats]: tried to create user `%s`: %s", "andres", PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogDeletingUser() throws Throwable
    {
        authProcedures.createUser( "andres", "el password", false );
        authProcedures.deleteUser( "andres" );

        log.assertExactly(
                info( "[admin]: created user `%s`", "andres" ),
                info( "[admin]: deleted user `%s`", "andres" ) );
    }

    @Test
    public void shouldLogDeletingNonExistentUser() throws Throwable
    {
        catchInvalidArguments( () -> authProcedures.deleteUser( "andres" ) );

        log.assertExactly( error( "[admin]: tried to delete user `%s`: %s", "andres", "User 'andres' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedDeleteUser() throws Throwable
    {
        authProcedures.authSubject = matsSubject;
        catchAuthorizationViolation( () -> authProcedures.deleteUser( ADMIN ) );

        log.assertExactly( error( "[mats]: tried to delete user `%s`: %s", ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogAddingRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        authProcedures.addRoleToUser( ARCHITECT, "mats" );

        log.assertExactly(
                info( "[admin]: created user `%s`", "mats" ),
                info( "[admin]: added role `%s` to user `%s`", ARCHITECT, "mats" ) );
    }

    @Test
    public void shouldLogFailureToAddRoleToUser() throws Throwable
    {
        authProcedures.createUser( "mats", "neo4j", false );
        catchInvalidArguments( () -> authProcedures.addRoleToUser( "null", "mats" ) );

        log.assertExactly(
                info( "[admin]: created user `%s`", "mats" ),
                error( "[admin]: tried to add role `%s` to user `%s`: %s", "null", "mats", "Role 'null' does not exist." ) );
    }

    @Test
    public void shouldLogUnauthorizedAddingRole() throws Throwable
    {
        authProcedures.authSubject = matsSubject;
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
    public void shouldLogUnauthorizedRemovingRole() throws Throwable
    {
        authProcedures.authSubject = matsSubject;
        catchAuthorizationViolation( () -> authProcedures.removeRoleFromUser( ADMIN, ADMIN ) );

        log.assertExactly( error( "[mats]: tried to remove role `%s` from user `%s`: %s", ADMIN, ADMIN, PERMISSION_DENIED ) );
    }

    @Test
    public void shouldLogPasswordChanges() throws IOException, InvalidArgumentsException
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        log.clear();

        // When
        authProcedures.changeUserPassword( "mats", "longerPassword", false );
        authProcedures.authSubject = matsSubject;
        authProcedures.changeUserPassword( "mats", "evenLongerPassword", false );

        // Then
        log.assertExactly(
                info( "[admin]: changed password for user `%s`", "mats" ),
                info( "[mats]: changed own password" )
        );
    }

    @Test
    public void shouldLogFailureToChangePassword() throws Throwable
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
                error( "[admin]: tried to change %s: %s", "password for user `andres`", "Old password and new password cannot be the same." ),
                error( "[admin]: tried to change %s: %s", "password for user `andres`", "A password cannot be empty." ),
                error( "[admin]: tried to change %s: %s", "password for user `notAndres`", "User 'notAndres' does not exist." )
        );
    }

    @Test
    public void shouldLogFailureToChangeOwnPassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "mats", "neo4j", true );
        authProcedures.authSubject = matsSubject;
        log.clear();

        // When
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "neo4j", false ) );
        catchInvalidArguments( () -> authProcedures.changeUserPassword( "mats", "", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change %s: %s", "own password", "Old password and new password cannot be the same." ),
                error( "[mats]: tried to change %s: %s", "own password", "A password cannot be empty." )
        );
    }

    @Test
    public void shouldLogUnauthorizedChangePassword() throws Throwable
    {
        // Given
        authProcedures.createUser( "andres", "neo4j", true );
        log.clear();
        authProcedures.authSubject = matsSubject;

        // When
        catchAuthorizationViolation( () -> authProcedures.changeUserPassword( "andres", "otherPw", false ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to change %s: %s", "password for user `andres`", PERMISSION_DENIED )
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
    public void shouldLogUnauthorizedSuspendUser() throws Throwable
    {
        // Given
        authProcedures.authSubject = matsSubject;

        // When
        catchAuthorizationViolation( () -> authProcedures.suspendUser( ADMIN ) );

        // Then
        log.assertExactly(
                error( "[mats]: tried to suspend user `%s`: %s", "admin", PERMISSION_DENIED )
        );
    }

    private void catchInvalidArguments( CheckedFunction f ) throws Throwable
    {
        try { f.apply(); } catch (InvalidArgumentsException e) { /*ignore*/ }
    }

    private void catchAuthorizationViolation( CheckedFunction f ) throws Throwable
    {
        try { f.apply(); } catch (AuthorizationViolationException e) { /*ignore*/ }
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, arguments );
    }

    private AssertableLogProvider.LogMatcher error( String message, String... arguments )
    {
        return inLog( this.getClass() ).error( message, arguments );
    }

    @FunctionalInterface
    private interface CheckedFunction {
        void apply() throws Throwable;
    }

    private static class TestAuthSubject extends EnterpriseAuthSubject
    {
        private final String name;
        private final boolean isAdmin;
        private final EnterpriseUserManager userManager;

        TestAuthSubject( String name, boolean isAdmin, EnterpriseUserManager userManager )
        {
            super(null, new TestShiroSubject( name ));
            this.name = name;
            this.isAdmin = isAdmin;
            this.userManager = userManager;
        }

        @Override
        public String username()
        {
            return name;
        }

        @Override
        public boolean isAdmin()
        {
            return isAdmin;
        }

        @Override
        public EnterpriseUserManager getUserManager()
        {
            return userManager;
        }

        @Override
        public boolean doesUsernameMatch( String username )
        {
            return name.equals( username );
        }
    }

    private static class TestShiroSubject extends ShiroSubject
    {
        private final String name;

        TestShiroSubject( String name )
        {
            super( mock( SecurityManager.class ), null );
            this.name = name;
        }

        @Override
        public Object getPrincipal()
        {
            return name;
        }
    }

}
