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

import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.api.security.AuthenticationResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

// TODO: homogenize "'' does not exist" type error messages. In short, add quotes in the right places
public class AuthProceduresTest extends AuthProcedureTestBase
{

    //---------- Change own password -----------

    // Enterprise version of test in BuiltInProceduresIT.callChangePasswordWithAccessModeInDbmsMode.
    // Uses community edition procedure in BuiltInProcedures
    @Test
    public void shouldChangeOwnPassword() throws Exception
    {
        testCallEmpty( readSubject, "CALL dbms.changePassword( '321' )" );
        testUnAunthenticated( readSubject );

        ShiroAuthSubject subject = manager.login( authToken( "readSubject", "321" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldNotChangeOwnPasswordIfNewPasswordInvalid() throws Exception
    {
        testCallFail( readSubject, "CALL dbms.changePassword( '' )", QueryExecutionException.class,
                "Password cannot be empty" );
        testCallFail( readSubject, "CALL dbms.changePassword( '321' )", QueryExecutionException.class,
                "Old password and new password cannot be the same" );
    }
    */

    //---------- change user password -----------

    // Should change password for admin subject and valid user
    @Test
    public void shouldChangeUserPassword() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )" );
        assertEquals( AuthenticationResult.FAILURE, manager.login( authToken( "readSubject", "123" ) )
                .getAuthenticationResult() );
        assertEquals( AuthenticationResult.SUCCESS, manager.login( authToken( "readSubject", "321" ) )
                .getAuthenticationResult() );
    }

    // Should fail vaguely to change password for non-admin subject, regardless of user and password
    @Test
    public void shouldNotChangeUserPasswordIfNotAdmin() throws Exception
    {
        testCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'jake', '321' )",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'readSubject', '' )",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    // Should change own password for non-admin or admin subject
    @Test
    public void shouldChangeUserPasswordIfSameUser() throws Exception
    {
        testCallEmpty( readSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )" );
        assertEquals( AuthenticationResult.FAILURE, manager.login( authToken( "readSubject", "123" ) )
                .getAuthenticationResult() );
        assertEquals( AuthenticationResult.SUCCESS, manager.login( authToken( "readSubject", "321" ) )
                .getAuthenticationResult() );

        testCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'adminSubject', 'cba' )" );
        assertEquals( AuthenticationResult.FAILURE, manager.login( authToken( "adminSubject", "abc" ) )
                .getAuthenticationResult() );
        assertEquals( AuthenticationResult.SUCCESS, manager.login( authToken( "adminSubject", "cba" ) )
                .getAuthenticationResult() );
    }

    // Should fail nicely to change own password for non-admin or admin subject if password invalid
    @Test
    public void shouldFailToChangeUserPasswordIfSameUserButInvalidPassword() throws Exception
    {
        testCallFail( readSubject, "CALL dbms.changeUserPassword( 'readSubject', '123' )",
                QueryExecutionException.class, "Old password and new password cannot be the same" );

        testCallFail( adminSubject, "CALL dbms.changeUserPassword( 'adminSubject', 'abc' )",
                QueryExecutionException.class, "Old password and new password cannot be the same" );
    }

    // Should fail nicely to change password for admin subject and non-existing user
    @Test
    public void shouldNotChangeUserPasswordIfNonExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.changeUserPassword( 'jake', '321' )",
                QueryExecutionException.class, "User jake does not exist" );
    }

    // Should fail nicely to change password for admin subject and empty password
    @Test
    public void shouldNotChangeUserPasswordIfEmptyPassword() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '' )",
                QueryExecutionException.class, "Password cannot be empty" );
    }

    // Should fail to change password for admin subject and same password
    @Test
    public void shouldNotChangeUserPasswordIfSamePassword() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '123' )",
                QueryExecutionException.class, "Old password and new password cannot be the same" );
    }

    //---------- create user -----------

    @Test
    public void shouldCreateUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('craig', '1234', true)" );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
    }

    /*
    TODO: uncomment and fix
    @Test
<<<<<<< 061656f5c3e1c3445f51c0387f2876d6c19e437d
    public void shouldNotCreateUserWithEmptyPassword() throws Exception
    {
        testCallFail( db, adminSubject, "CALL dbms.createUser('craig', '', true)", QueryExecutionException.class,
                "Password cannot be empty." );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
=======
    public void shouldNotCreateUserIfInvalidUsername() throws Exception
>>>>>>> Vastly increased AuthProcedures unit test coverage.
    {
        testCallFail( adminSubject, "CALL dbms.createUser('', '1234', true)", QueryExecutionException.class,
                "Username cannot be empty" );
        testCallFail( adminSubject, "CALL dbms.createUser('&%ss!', '1234', true)", QueryExecutionException.class,
                "Username cannot be empty" );
        testCallFail( adminSubject, "CALL dbms.createUser('&%ss!', '', true)", QueryExecutionException.class,
                "Username cannot be empty" );
    }
    */

    /*
    TODO: uncomment and fix
    @Test
    public void shouldNotCreateUserIfInvalidPassword() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.createUser('craig', '', true)", QueryExecutionException.class,
                "Password cannot be empty" );
    }
    */

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.createUser('readSubject', '1234', true)",
                QueryExecutionException.class, "The specified user already exists" );
        testCallFail( adminSubject, "CALL dbms.createUser('readSubject', '', true)",
                QueryExecutionException.class, "The specified user already exists" );
    }

    @Test
    public void shouldNotAllowNonAdminCreateUser() throws Exception
    {
        testFailCreateUser( pwdSubject );
        testFailCreateUser( readSubject );
        testFailCreateUser( writeSubject );
        testFailCreateUser( schemaSubject );
    }

    //---------- delete user -----------

    @Test
    public void shouldDeleteUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('readSubject')" );
        assertNull( "User readSubject should not exist", manager.getUser( "readSubject" ) );
    }

    @Test
    public void shouldNotDeleteUserIfNotAdmin() throws Exception
    {
        testFailDeleteUser( pwdSubject );
        testFailDeleteUser( readSubject );
        testFailDeleteUser( writeSubject );
        testFailDeleteUser( schemaSubject );
    }

    @Test
    public void shouldNotAllowDeletingNonExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.deleteUser('Craig')",
                QueryExecutionException.class, "The user 'Craig' does not exist" );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldNotAllowDeletingYourself() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.deleteUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Deleting yourself is not allowed" );
    }
    */

    //---------- suspend user -----------

    @Test
    public void shouldSuspendUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertTrue( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldSuspendSuspendedUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertTrue( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldFailToSuspendNonExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.suspendUser('Craig')",
                QueryExecutionException.class, "User Craig does not exist" );
    }

    @Test
    public void shouldFailToSuspendIfNotAdmin() throws Exception
    {
        testCallFail( schemaSubject, "CALL dbms.suspendUser('readSubject')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.suspendUser('Craig')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.suspendUser('')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToSuspendYourself() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.suspendUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Suspending yourself is not allowed" );
    }
    */

    //---------- activate user -----------

    @Test
    public void shouldActivateUser() throws Exception
    {
        manager.suspendUser( "readSubject" );
        testCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertFalse( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldActivateActiveUser() throws Exception
    {
        manager.suspendUser( "readSubject" );
        testCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        testCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertFalse( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToActivateNonExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.activateUser('Craig')",
                QueryExecutionException.class, "User 'Craig' does not exist" );
    }
     */

    @Test
    public void shouldFailToActivateIfNotAdmin() throws Exception
    {
        manager.suspendUser( "readSubject" );
        testCallFail( schemaSubject, "CALL dbms.activateUser('readSubject')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.activateUser('Craig')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.activateUser('')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToActivateYourself() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.activateUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Activating yourself is not allowed" );
    }
     */

    //---------- add user to role -----------

    @Test
    public void shouldAddUserToRole() throws Exception
    {
        assertFalse( "Should not have role publisher", readSubject.getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertTrue( "Should have role publisher", readSubject.getSubject().hasRole( PUBLISHER ) );
    }

    @Test
    public void shouldAddRetainUserInRole() throws Exception
    {
        assertTrue( "Should have role reader", readSubject.getSubject().hasRole( READER ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + READER + "')" );
        assertTrue( "Should have still have role reader", readSubject.getSubject().hasRole( READER ) );
    }

    @Test
    public void shouldFailToAddNonExistingUserToRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.addUserToRole('Olivia', '" + PUBLISHER + "')",
                QueryExecutionException.class, "User Olivia does not exist" );
        testCallFail( adminSubject, "CALL dbms.addUserToRole('Olivia', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, "User Olivia does not exist" );
        testCallFail( adminSubject, "CALL dbms.addUserToRole('Olivia', '')",
                QueryExecutionException.class, "User Olivia does not exist" );
    }

    @Test
    public void shouldFailToAddUserToNonExistingRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.addUserToRole('readSubject', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, "Role thisRoleDoesNotExist does not exist" );
        testCallFail( adminSubject, "CALL dbms.addUserToRole('readSubject', '')",
                QueryExecutionException.class, "Role  does not exist" );
    }

    @Test
    public void shouldFailToAddUserToRoleIfNotAdmin() throws Exception
    {
        testFailAddUserToRole( pwdSubject );
        testFailAddUserToRole( readSubject );
        testFailAddUserToRole( writeSubject );

        testCallFail( schemaSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.addUserToRole('Olivia', '" + PUBLISHER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.addUserToRole('Olivia', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    //---------- remove user from role -----------

    @Test
    public void shouldRemoveUserFromRole() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + READER + "')" );
        assertFalse( "Should not have role reader", readSubject.getSubject().hasRole( READER ) );
    }

    @Test
    public void shouldKeepUserOutOfRole() throws Exception
    {
        assertFalse( "Should not have role publisher", readSubject.getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        assertFalse( "Should not have role publisher", readSubject.getSubject().hasRole( PUBLISHER ) );
    }

    @Test
    public void shouldFailToRemoveNonExistingUserFromRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('Olivia', '" + PUBLISHER + "')",
                QueryExecutionException.class, "User Olivia does not exist" );
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('Olivia', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, "User Olivia does not exist" );
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('Olivia', '')",
                QueryExecutionException.class, "User Olivia does not exist" );
    }

    @Test
    public void shouldFailToRemoveUserFromNonExistingRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('readSubject', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, "Role thisRoleDoesNotExist does not exist" );
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '')",
                QueryExecutionException.class, "Role  does not exist" );
    }

    @Test
    public void shouldFailToRemoveUserFromRoleIfNotAdmin() throws Exception
    {
        testFailRemoveUserFromRole( pwdSubject );
        testFailRemoveUserFromRole( readSubject );
        testFailRemoveUserFromRole( writeSubject );

        testCallFail( schemaSubject, "CALL dbms.removeUserFromRole('readSubject', '" + READER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.removeUserFromRole('Olivia', '" + READER + "')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallFail( schemaSubject, "CALL dbms.removeUserFromRole('Olivia', 'thisRoleDoesNotExist')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToRemoveYourselfFromAdminRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('" + adminSubject.name() + "', '" + ADMIN + "')",
                QueryExecutionException.class, "Remove yourself from admin role is not allowed" );
    }
    */

    //---------- manage multiple roles -----------

    @Test
    public void shouldAllowAddingAndRemovingUserFromMultipleRoles() throws Exception
    {
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        assertFalse( "Should not have role architect",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( ARCHITECT ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + ARCHITECT + "')" );
        assertTrue( "Should have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        assertTrue( "Should have role architect",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( ARCHITECT ) );

        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + ARCHITECT + "')" );
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        assertFalse( "Should not have role architect",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( ARCHITECT ) );
    }

    //---------- list users -----------

    @Test
    public void shouldListUsers() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listUsers() YIELD username",
                r -> resultKeyIs( r, "username", initialUsers ) );
    }

    @Test
    public void shouldReturnUsersWithRoles() throws Exception
    {
        Map<String, Object> expected = map(
                "adminSubject", listOf( ADMIN ),
                "readSubject", listOf( READER ),
                "schemaSubject", listOf( ARCHITECT ),
                "readWriteSubject", listOf( READER, PUBLISHER ),
                "noneSubject", listOf( ),
                "neo4j", listOf( ADMIN )
        );
        manager.addUserToRole( "readWriteSubject", READER );
        testResult( adminSubject, "CALL dbms.listUsers()",
                r -> resultContainsMap( r, "username", "roles", expected ) );
    }

    @Test
    public void shouldShowCurrentUser() throws Exception
    {
        manager.addUserToRole( "readWriteSubject", READER );
        testResult( adminSubject, "CALL dbms.showCurrentUser()",
                r -> resultContainsMap( r, "username", "roles", map( "adminSubject", listOf( ADMIN ) ) ) );
        testResult( readSubject, "CALL dbms.showCurrentUser()",
                r -> resultContainsMap( r, "username", "roles", map( "readSubject", listOf( READER ) ) ) );
        testResult( schemaSubject, "CALL dbms.showCurrentUser()",
                r -> resultContainsMap( r, "username", "roles", map( "schemaSubject", listOf( ARCHITECT ) ) ) );
        testResult( writeSubject, "CALL dbms.showCurrentUser()",
                r -> resultContainsMap( r, "username", "roles",
                        map( "readWriteSubject", listOf( READER, PUBLISHER ) ) ) );
        testResult( noneSubject, "CALL dbms.showCurrentUser()",
                r -> resultContainsMap( r, "username", "roles", map( "noneSubject", listOf() ) ) );
    }

    @Test
    public void shouldNotAllowNonAdminListUsers() throws Exception
    {
        testFailListUsers( pwdSubject, 5 );
        testFailListUsers( readSubject, 5 );
        testFailListUsers( writeSubject, 5 );
        testFailListUsers( schemaSubject, 5 );
    }

    //---------- list roles -----------

    @Test
    public void shouldListRoles() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRoles() YIELD role AS roles RETURN roles",
                r -> resultKeyIs( r, "roles", initialRoles ) );
    }

    @Test
    public void shouldReturnRolesWithUsers() throws Exception
    {
        Map<String,Object> expected = map(
                ADMIN, listOf( "adminSubject", "neo4j" ),
                READER, listOf( "readSubject" ),
                ARCHITECT, listOf( "schemaSubject" ),
                PUBLISHER, listOf( "readWriteSubject" ),
                "empty", listOf()
        );
        testResult( adminSubject, "CALL dbms.listRoles()",
                r -> resultContainsMap( r, "role", "users", expected ) );
    }

    @Test
    public void shouldNotAllowNonAdminListRoles() throws Exception
    {
        testFailListRoles( pwdSubject );
        testFailListRoles( readSubject );
        testFailListRoles( writeSubject );
        testFailListRoles( schemaSubject );
    }

    //---------- list roles for user -----------

    @Test
    public void shouldListRolesForUser() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> resultKeyIs( r, "roles", ADMIN ) );
        testResult( adminSubject, "CALL dbms.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> resultKeyIs( r, "roles", READER ) );
    }

    @Test
    public void shouldListNoRolesForUserWithNoRoles() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.listRolesForUser('Henrik') YIELD value as roles RETURN roles" );
    }

    @Test
    public void shouldNotListRolesForNonExistingUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.listRolesForUser('Petra') YIELD value as roles RETURN roles",
                QueryExecutionException.class, "User Petra does not exist" );
        testCallFail( adminSubject, "CALL dbms.listRolesForUser('') YIELD value as roles RETURN roles",
                QueryExecutionException.class, "User  does not exist" );
    }

    /*
    TODO: uncommend and fix
    @Test
    public void shouldListOwnRolesRoles() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> resultKeyIs( r, "roles", ADMIN ) );
        testResult( readSubject, "CALL dbms.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> resultKeyIs( r, "roles", READER ) );
    }
     */

    @Test
    public void shouldNotAllowNonAdminListUserRoles() throws Exception
    {
        testFailListUserRoles( pwdSubject, "adminSubject" );
        testFailListUserRoles( readSubject, "adminSubject" );
        testFailListUserRoles( writeSubject, "adminSubject" );
        testFailListUserRoles( schemaSubject, "adminSubject" );
    }

    //---------- list users for role -----------

    @Test
    public void shouldListUsersForRole() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listUsersForRole('admin') YIELD value as users RETURN users",
                r -> resultKeyIs( r, "users", adminSubject.name(), "neo4j" ) );
    }

    @Test
    public void shouldListNoUsersForRoleWithNoUsers() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.listUsersForRole('empty') YIELD value as users RETURN users" );
    }

    @Test
    public void shouldNotListUsersForNonExistingRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.listUsersForRole('poodle') YIELD value as users RETURN users",
                QueryExecutionException.class, "Role poodle does not exist" );
        testCallFail( adminSubject, "CALL dbms.listUsersForRole('') YIELD value as users RETURN users",
                QueryExecutionException.class, "Role  does not exist" );
    }

    @Test
    public void shouldNotListUsersForRoleIfNotAdmin() throws Exception
    {
        testFailListRoleUsers( pwdSubject, ADMIN );
        testFailListRoleUsers( readSubject, ADMIN );
        testFailListRoleUsers( writeSubject, ADMIN );
        testFailListRoleUsers( schemaSubject, ADMIN );
    }

    //---------- permissions -----------

    /*
    TODO: uncomment and fix un-authentication handling
    @Test
    public void shouldSetCorrectUnAuthenticatedPermissions() throws Exception
    {
        pwdSubject.logout();
        testUnAunthenticated( pwdSubject, "MATCH (n) RETURN n" );
        testUnAunthenticated( pwdSubject, "CREATE (:Node)" );
        testUnAunthenticated( pwdSubject, "CREATE INDEX ON :Node(number)" );
        testUnAunthenticated( pwdSubject, "CALL dbms.changePassword( '321' )" );
        testUnAunthenticated( pwdSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
    }
    */

    @Test
    public void shouldSetCorrectPasswordChangeRequiredPermissions() throws Exception
    {
        testFailRead( pwdSubject, 3 );
        testFailWrite( pwdSubject );
        testFailSchema( pwdSubject );
        testCallEmpty( pwdSubject, "CALL dbms.changePassword( '321' )" );

        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ARCHITECT + "')" );
        ShiroAuthSubject henrik = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, henrik.getAuthenticationResult() );
        testFailRead( henrik, 3 );
        testFailWrite( henrik );
        testFailSchema( henrik );
        testCallEmpty( henrik, "CALL dbms.changePassword( '321' )" );

        testCallEmpty( adminSubject, "CALL dbms.createUser('Olivia', 'bar', true)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Olivia', '" + ADMIN + "')" );
        ShiroAuthSubject olivia = manager.login( authToken( "Olivia", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, olivia.getAuthenticationResult() );
        testFailRead( olivia, 3 );
        testFailWrite( olivia );
        testFailSchema( olivia );
        testCallFail( olivia, "CALL dbms.createUser('OliviasFriend', 'bar', false)",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
        testCallEmpty( olivia, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectNoRolePermissions() throws Exception
    {
        testFailRead( noneSubject, 3 );
        testFailWrite( noneSubject );
        testFailSchema( noneSubject );
        testFailCreateUser( noneSubject );
        testCallEmpty( noneSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectReaderPermissions() throws Exception
    {
        testSuccessfulRead( readSubject, 3 );
        testFailWrite( readSubject );
        testFailSchema( readSubject );
        testFailCreateUser( readSubject );
        testCallEmpty( readSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectPublisherPermissions() throws Exception
    {
        testSuccessfulRead( writeSubject, 3 );
        testSuccessfulWrite( writeSubject );
        testFailSchema( writeSubject );
        testFailCreateUser( writeSubject );
        testCallEmpty( writeSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectSchemaPermissions() throws Exception
    {
        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject );
        testCallEmpty( schemaSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectAdminPermissions() throws Exception
    {
        testSuccessfulRead( adminSubject, 3 );
        testSuccessfulWrite( adminSubject );
        testSuccessfulSchema( adminSubject );
        testCallEmpty( adminSubject, "CALL dbms.createUser('Olivia', 'bar', true)" );
        testCallEmpty( adminSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectMultiRolePermissions() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('schemaSubject', '" + READER + "')" );

        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject );
        testCallEmpty( schemaSubject, "CALL dbms.changePassword( '321' )" );
    }
}
