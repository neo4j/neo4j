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
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.kernel.api.security.AuthenticationResult.*;
import static org.neo4j.server.security.enterprise.auth.AuthProcedures.*;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

// TODO: homogenize "'' does not exist" type error messages. In short, add quotes in the right places
public abstract class AuthProceduresTestLogic<S> extends AuthTestBase<S>
{
    //---------- Change own password -----------

    // Enterprise version of test in BuiltInProceduresIT.callChangePasswordWithAccessModeInDbmsMode.
    // Uses community edition procedure in BuiltInProcedures
    @Test
    public void shouldChangeOwnPassword() throws Throwable
    {
        assertCallEmpty( readSubject, "CALL dbms.changePassword( '321' )" );
        testUnAuthenticated( readSubject );

        readSubject = neo.login( "readSubject", "321" );
        assertEquals( SUCCESS, neo.authenticationResult( readSubject ) );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldNotChangeOwnPasswordIfNewPasswordInvalid() throws Exception
    {
        assertCallFail( readSubject, "CALL dbms.changePassword( '' )", QueryExecutionException.class,
                "Password cannot be empty" );
        assertCallFail( readSubject, "CALL dbms.changePassword( '321' )", QueryExecutionException.class,
                "Old password and new password cannot be the same" );
    }
    */

    //---------- change user password -----------

    // Should change password for admin subject and valid user
    @Test
    public void shouldChangeUserPassword() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )" );

        // TODO: uncomment and fix
        // testUnAuthenticated( readSubject );

        assertEquals( FAILURE, neo.authenticationResult( neo.login( "readSubject", "123" ) ) );
        assertEquals( SUCCESS, neo.authenticationResult( neo.login( "readSubject", "321" ) ) );

    }

    // Should fail vaguely to change password for non-admin subject, regardless of user and password
    @Test
    public void shouldNotChangeUserPasswordIfNotAdmin() throws Exception
    {
        assertCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'jake', '321' )", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.changeUserPassword( 'readSubject', '' )", PERMISSION_DENIED );
    }

    // Should change own password for non-admin or admin subject
    @Test
    public void shouldChangeUserPasswordIfSameUser() throws Throwable
    {
        assertCallEmpty( readSubject, "CALL dbms.changeUserPassword( 'readSubject', '321' )" );
        testUnAuthenticated( readSubject );
        assertEquals( SUCCESS, neo.authenticationResult( neo.login( "readSubject", "321" ) ) );

        assertCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'adminSubject', 'cba' )" );
        testUnAuthenticated( adminSubject );
        assertEquals( SUCCESS, neo.authenticationResult( neo.login( "adminSubject", "cba" ) ) );
    }

    // Should fail nicely to change own password for non-admin or admin subject if password invalid
    @Test
    public void shouldFailToChangeUserPasswordIfSameUserButInvalidPassword() throws Exception
    {
        assertCallFail( readSubject, "CALL dbms.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same" );

        assertCallFail( adminSubject, "CALL dbms.changeUserPassword( 'adminSubject', 'abc' )",
                "Old password and new password cannot be the same" );
    }

    // Should fail nicely to change password for admin subject and non-existing user
    @Test
    public void shouldNotChangeUserPasswordIfNonExistingUser() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.changeUserPassword( 'jake', '321' )", "User jake does not exist" );
    }

    // Should fail nicely to change password for admin subject and empty password
    @Test
    public void shouldNotChangeUserPasswordIfEmptyPassword() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '' )", "Password cannot be empty" );
    }

    // Should fail to change password for admin subject and same password
    @Test
    public void shouldNotChangeUserPasswordIfSamePassword() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same" );
    }

    //---------- create user -----------

    @Test
    public void shouldCreateUser() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('craig', '1234', true)" );
        assertNotNull( "User craig should exist", userManager.getUser( "craig" ) );
    }

    /*
    TODO: This test is not valid for InMemoryUserRepository, as this allows all usernames. Find a way to flag it
    @Test
    public void shouldNotCreateUserIfInvalidUsername() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.createUser('', '1234', true)", QueryExecutionException.class,
                "Username cannot be empty" );
        assertCallFail( adminSubject, "CALL dbms.createUser('&%ss!', '1234', true)", QueryExecutionException.class,
                "Username cannot be empty" );
        assertCallFail( adminSubject, "CALL dbms.createUser('&%ss!', '', true)", QueryExecutionException.class,
                "Username cannot be empty" );
    }
    */

    @Test
    public void shouldNotCreateUserIfInvalidPassword() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.createUser('craig', '', true)", "Password cannot be empty" );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.createUser('readSubject', '1234', true)",
                "The specified user already exists" );
        assertCallFail( adminSubject, "CALL dbms.createUser('readSubject', '', true)", "Password cannot be empty" );
    }

    @Test
    public void shouldNotAllowNonAdminCreateUser() throws Exception
    {
        testFailCreateUser( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailCreateUser( readSubject, PERMISSION_DENIED );
        testFailCreateUser( writeSubject, PERMISSION_DENIED );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
    }

    //---------- delete user -----------

    @Test
    public void shouldDeleteUser() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('noneSubject')" );
        assertNull( "User noneSubject should not exist", userManager.getUser( "noneSubject" ) );

        userManager.addUserToRole( "readSubject", PUBLISHER );
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('readSubject')" );
        assertNull( "User readSubject should not exist", userManager.getUser( "readSubject" ) );
        assertFalse( userManager.getUsernamesForRole( READER ).contains( "readSubject" ) );
        assertFalse( userManager.getUsernamesForRole( PUBLISHER ).contains( "readSubject" ) );
    }

    @Test
    public void shouldNotDeleteUserIfNotAdmin() throws Exception
    {
        testFailDeleteUser( pwdSubject, "readSubject", CHANGE_PWD_ERR_MSG );
        testFailDeleteUser( readSubject, "readSubject", PERMISSION_DENIED );
        testFailDeleteUser( writeSubject, "readSubject", PERMISSION_DENIED );

        testFailDeleteUser( schemaSubject, "readSubject", PERMISSION_DENIED );
        testFailDeleteUser( schemaSubject, "Craig", PERMISSION_DENIED );
        testFailDeleteUser( schemaSubject, "", PERMISSION_DENIED );
    }

    @Test
    public void shouldNotAllowDeletingNonExistingUser() throws Exception
    {
        testFailDeleteUser( adminSubject, "Craig", "The user 'Craig' does not exist" );
        testFailDeleteUser( adminSubject, "", "The user '' does not exist" );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldNotAllowDeletingYourself() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.deleteUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Deleting yourself is not allowed" );
    }
    */

    //---------- suspend user -----------

    @Test
    public void shouldSuspendUser() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldSuspendSuspendedUser() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldFailToSuspendNonExistingUser() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.suspendUser('Craig')", "User Craig does not exist" );
    }

    @Test
    public void shouldFailToSuspendIfNotAdmin() throws Exception
    {
        assertCallFail( schemaSubject, "CALL dbms.suspendUser('readSubject')", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.suspendUser('Craig')", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.suspendUser('')", PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToSuspendYourself() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.suspendUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Suspending yourself is not allowed" );
    }
    */

    //---------- activate user -----------

    @Test
    public void shouldActivateUser() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldActivateActiveUser() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( InternalFlatFileRealm.IS_SUSPENDED ) );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToActivateNonExistingUser() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.activateUser('Craig')",
                QueryExecutionException.class, "User 'Craig' does not exist" );
    }
     */

    @Test
    public void shouldFailToActivateIfNotAdmin() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertCallFail( schemaSubject, "CALL dbms.activateUser('readSubject')", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.activateUser('Craig')", PERMISSION_DENIED );
        assertCallFail( schemaSubject, "CALL dbms.activateUser('')", PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToActivateYourself() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.activateUser('" + adminSubject.name() + "')",
                QueryExecutionException.class, "Activating yourself is not allowed" );
    }
     */

    //---------- add user to role -----------

    @Test
    public void shouldAddUserToRole() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertTrue( "Should have role publisher", userHasRole( "readSubject", PUBLISHER ) );
    }

    @Test
    public void shouldAddRetainUserInRole() throws Exception
    {
        assertTrue( "Should have role reader", userHasRole( "readSubject", READER ) );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + READER + "')" );
        assertTrue( "Should have still have role reader", userHasRole( "readSubject", READER ) );
    }

    @Test
    public void shouldFailToAddNonExistingUserToRole() throws Exception
    {
        testFailAddUserToRole( adminSubject, "Olivia", PUBLISHER, "User Olivia does not exist" );
        testFailAddUserToRole( adminSubject, "Olivia", "thisRoleDoesNotExist", "User Olivia does not exist" );
        testFailAddUserToRole( adminSubject, "Olivia", "",
                HAS_ILLEGAL_ARGS_CHECK ? "Role name contains illegal characters" : "User Olivia does not exist" );
    }

    @Test
    public void shouldFailToAddUserToNonExistingRole() throws Exception
    {
        testFailAddUserToRole( adminSubject, "readSubject", "thisRoleDoesNotExist",
                "Role thisRoleDoesNotExist does not exist" );
        testFailAddUserToRole( adminSubject, "readSubject", "",
                HAS_ILLEGAL_ARGS_CHECK ? "Role name contains illegal characters" : "Role  does not exist" );
    }

    @Test
    public void shouldFailToAddUserToRoleIfNotAdmin() throws Exception
    {
        testFailAddUserToRole( pwdSubject, "readSubject", PUBLISHER, CHANGE_PWD_ERR_MSG );
        testFailAddUserToRole( readSubject, "readSubject", PUBLISHER, PERMISSION_DENIED );
        testFailAddUserToRole( writeSubject, "readSubject", PUBLISHER, PERMISSION_DENIED );

        testFailAddUserToRole( schemaSubject, "readSubject", PUBLISHER, PERMISSION_DENIED );
        testFailAddUserToRole( schemaSubject, "Olivia", PUBLISHER, PERMISSION_DENIED );
        testFailAddUserToRole( schemaSubject, "Olivia", "thisRoleDoesNotExist", PERMISSION_DENIED );
    }

    //---------- remove user from role -----------

    @Test
    public void shouldRemoveUserFromRole() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + READER + "')" );
        assertFalse( "Should not have role reader", userHasRole( "readSubject", READER ) );
    }

    @Test
    public void shouldKeepUserOutOfRole() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
    }

    @Test
    public void shouldFailToRemoveNonExistingUserFromRole() throws Exception
    {
        testFailRemoveUserFromRole( adminSubject, "Olivia", PUBLISHER, "User Olivia does not exist" );
        testFailRemoveUserFromRole( adminSubject, "Olivia", "thisRoleDoesNotExist", "User Olivia does not exist" );
        testFailRemoveUserFromRole( adminSubject, "Olivia", "",
                HAS_ILLEGAL_ARGS_CHECK ? "Role name contains illegal characters" : "User Olivia does not exist" );
        testFailRemoveUserFromRole( adminSubject, "", "",
                HAS_ILLEGAL_ARGS_CHECK ? "User name contains illegal characters" : "User  does not exist" );
    }

    @Test
    public void shouldFailToRemoveUserFromNonExistingRole() throws Exception
    {
        testFailRemoveUserFromRole( adminSubject, "readSubject", "thisRoleDoesNotExist", "Role thisRoleDoesNotExist does not exist" );
        testFailRemoveUserFromRole( adminSubject, "readSubject", "",
                HAS_ILLEGAL_ARGS_CHECK ? "Role name contains illegal characters" : "Role  does not exist" );
    }

    @Test
    public void shouldFailToRemoveUserFromRoleIfNotAdmin() throws Exception
    {
        testFailRemoveUserFromRole( pwdSubject, "readSubject", PUBLISHER,CHANGE_PWD_ERR_MSG );
        testFailRemoveUserFromRole( readSubject, "readSubject", PUBLISHER, PERMISSION_DENIED );
        testFailRemoveUserFromRole( writeSubject, "readSubject", PUBLISHER, PERMISSION_DENIED );

        testFailRemoveUserFromRole( schemaSubject, "readSubject", READER, PERMISSION_DENIED );
        testFailRemoveUserFromRole( schemaSubject, "Olivia", READER, PERMISSION_DENIED );
        testFailRemoveUserFromRole( schemaSubject, "Olivia", "thisRoleDoesNotExist", PERMISSION_DENIED );
    }

    /*
    TODO: uncomment and fix
    @Test
    public void shouldFailToRemoveYourselfFromAdminRole() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.removeUserFromRole('" + adminSubject.name() + "', '" + ADMIN + "')",
                QueryExecutionException.class, "Remove yourself from admin role is not allowed" );
    }
    */

    //---------- manage multiple roles -----------

    @Test
    public void shouldAllowAddingAndRemovingUserFromMultipleRoles() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertFalse( "Should not have role architect", userHasRole( "readSubject", ARCHITECT ) );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + ARCHITECT + "')" );
        assertTrue( "Should have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertTrue( "Should have role architect", userHasRole( "readSubject", ARCHITECT ) );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + ARCHITECT + "')" );
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertFalse( "Should not have role architect", userHasRole( "readSubject", ARCHITECT ) );
    }

    //---------- list users -----------

    @Test
    public void shouldListUsers() throws Exception
    {
        executeQuery( adminSubject, "CALL dbms.listUsers() YIELD username",
                r -> assertKeyIs( r, "username", initialUsers ) );
    }

    @Test
    public void shouldReturnUsersWithRoles() throws Exception
    {
        Map<String, Object> expected = map(
                "adminSubject", listOf( ADMIN ),
                "readSubject", listOf( READER ),
                "schemaSubject", listOf( ARCHITECT ),
                "writeSubject", listOf( READER, PUBLISHER ),
                "pwdSubject", listOf( ),
                "noneSubject", listOf( ),
                "neo4j", listOf( ADMIN )
        );
        userManager.addUserToRole( "writeSubject", READER );
        executeQuery( adminSubject, "CALL dbms.listUsers()",
                r -> assertKeyIsMap( r, "username", "roles", expected ) );
    }

    @Test
    public void shouldShowCurrentUser() throws Exception
    {
        userManager.addUserToRole( "writeSubject", READER );
        executeQuery( adminSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "adminSubject", listOf( ADMIN ) ) ) );
        executeQuery( readSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "readSubject", listOf( READER ) ) ) );
        executeQuery( schemaSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "schemaSubject", listOf( ARCHITECT ) ) ) );
        executeQuery( writeSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles",
                        map( "writeSubject", listOf( READER, PUBLISHER ) ) ) );
        executeQuery( noneSubject, "CALL dbms.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "noneSubject", listOf() ) ) );
    }

    @Test
    public void shouldNotAllowNonAdminListUsers() throws Exception
    {
        testFailListUsers( pwdSubject, 5, CHANGE_PWD_ERR_MSG );
        testFailListUsers( readSubject, 5, PERMISSION_DENIED );
        testFailListUsers( writeSubject, 5, PERMISSION_DENIED );
        testFailListUsers( schemaSubject, 5, PERMISSION_DENIED );
    }

    //---------- list roles -----------

    @Test
    public void shouldListRoles() throws Exception
    {
        executeQuery( adminSubject, "CALL dbms.listRoles() YIELD role",
                r -> assertKeyIs( r, "role", initialRoles ) );
    }

    @Test
    public void shouldReturnRolesWithUsers() throws Exception
    {
        Map<String,Object> expected = map(
                ADMIN, listOf( "adminSubject", "neo4j" ),
                READER, listOf( "readSubject" ),
                ARCHITECT, listOf( "schemaSubject" ),
                PUBLISHER, listOf( "writeSubject" ),
                "empty", listOf()
        );
        executeQuery( adminSubject, "CALL dbms.listRoles()",
                r -> assertKeyIsMap( r, "role", "users", expected ) );
    }

    @Test
    public void shouldNotAllowNonAdminListRoles() throws Exception
    {
        testFailListRoles( pwdSubject, CHANGE_PWD_ERR_MSG );
        testFailListRoles( readSubject, PERMISSION_DENIED );
        testFailListRoles( writeSubject, PERMISSION_DENIED );
        testFailListRoles( schemaSubject, PERMISSION_DENIED );
    }

    //---------- list roles for user -----------

    @Test
    public void shouldListRolesForUser() throws Exception
    {
        executeQuery( adminSubject, "CALL dbms.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        executeQuery( adminSubject, "CALL dbms.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }

    @Test
    public void shouldListNoRolesForUserWithNoRoles() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.listRolesForUser('Henrik') YIELD value as roles RETURN roles" );
    }

    @Test
    public void shouldNotListRolesForNonExistingUser() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.listRolesForUser('Petra') YIELD value as roles RETURN roles",
                "User Petra does not exist" );
        assertCallFail( adminSubject, "CALL dbms.listRolesForUser('') YIELD value as roles RETURN roles",
                "User  does not exist" );
    }

    /*
    TODO: uncommend and fix
    @Test
    public void shouldListOwnRolesRoles() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        testResult( readSubject, "CALL dbms.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }
     */

    @Test
    public void shouldNotAllowNonAdminListUserRoles() throws Exception
    {
        testFailListUserRoles( pwdSubject, "adminSubject", CHANGE_PWD_ERR_MSG );
        testFailListUserRoles( readSubject, "adminSubject", PERMISSION_DENIED );
        testFailListUserRoles( writeSubject, "adminSubject", PERMISSION_DENIED );
        testFailListUserRoles( schemaSubject, "adminSubject", PERMISSION_DENIED );
    }

    //---------- list users for role -----------

    @Test
    public void shouldListUsersForRole() throws Exception
    {
        executeQuery( adminSubject, "CALL dbms.listUsersForRole('admin') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "adminSubject", "neo4j" ) );
    }

    @Test
    public void shouldListNoUsersForRoleWithNoUsers() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.listUsersForRole('empty') YIELD value as users RETURN users" );
    }

    @Test
    public void shouldNotListUsersForNonExistingRole() throws Exception
    {
        assertCallFail( adminSubject, "CALL dbms.listUsersForRole('poodle') YIELD value as users RETURN users",
                "Role poodle does not exist" );
        assertCallFail( adminSubject, "CALL dbms.listUsersForRole('') YIELD value as users RETURN users",
                "Role  does not exist" );
    }

    @Test
    public void shouldNotListUsersForRoleIfNotAdmin() throws Exception
    {
        testFailListRoleUsers( pwdSubject, ADMIN, CHANGE_PWD_ERR_MSG );
        testFailListRoleUsers( readSubject, ADMIN, PERMISSION_DENIED );
        testFailListRoleUsers( writeSubject, ADMIN, PERMISSION_DENIED );
        testFailListRoleUsers( schemaSubject, ADMIN, PERMISSION_DENIED );
    }

    //---------- permissions -----------

    /*
    TODO: uncomment and fix un-authentication handling
    @Test
    public void shouldSetCorrectUnAuthenticatedPermissions() throws Exception
    {
        pwdSubject.logout();
        testUnAuthenticated( pwdSubject, "MATCH (n) RETURN n" );
        testUnAuthenticated( pwdSubject, "CREATE (:Node)" );
        testUnAuthenticated( pwdSubject, "CREATE INDEX ON :Node(number)" );
        testUnAuthenticated( pwdSubject, "CALL dbms.changePassword( '321' )" );
        testUnAuthenticated( pwdSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
    }
    */

    @Test
    public void shouldSetCorrectPasswordChangeRequiredPermissions() throws Throwable
    {
        testFailRead( pwdSubject, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( pwdSubject, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( pwdSubject, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertCallEmpty( pwdSubject, "CALL dbms.changePassword( '321' )" );

        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ARCHITECT + "')" );
        S henrik = neo.login( "Henrik", "bar" );
        assertEquals( PASSWORD_CHANGE_REQUIRED, neo.authenticationResult( henrik ) );
        testFailRead( henrik, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( henrik, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( henrik, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertCallEmpty( henrik, "CALL dbms.changePassword( '321' )" );

        assertCallEmpty( adminSubject, "CALL dbms.createUser('Olivia', 'bar', true)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Olivia', '" + ADMIN + "')" );
        S olivia = neo.login( "Olivia", "bar" );
        assertEquals( PASSWORD_CHANGE_REQUIRED, neo.authenticationResult( olivia ) );
        testFailRead( olivia, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( olivia, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( olivia, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertCallFail( olivia, "CALL dbms.createUser('OliviasFriend', 'bar', false)", CHANGE_PWD_ERR_MSG );
        assertCallEmpty( olivia, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectNoRolePermissions() throws Exception
    {
        testFailRead( noneSubject, 3 );
        testFailWrite( noneSubject );
        testFailSchema( noneSubject );
        testFailCreateUser( noneSubject, PERMISSION_DENIED );
        assertCallEmpty( noneSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectReaderPermissions() throws Exception
    {
        testSuccessfulRead( readSubject, 3 );
        testFailWrite( readSubject );
        testFailSchema( readSubject );
        testFailCreateUser( readSubject, PERMISSION_DENIED );
        assertCallEmpty( readSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectPublisherPermissions() throws Exception
    {
        testSuccessfulRead( writeSubject, 3 );
        testSuccessfulWrite( writeSubject );
        testFailSchema( writeSubject );
        testFailCreateUser( writeSubject, PERMISSION_DENIED );
        assertCallEmpty( writeSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectSchemaPermissions() throws Exception
    {
        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertCallEmpty( schemaSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectAdminPermissions() throws Exception
    {
        testSuccessfulRead( adminSubject, 3 );
        testSuccessfulWrite( adminSubject );
        testSuccessfulSchema( adminSubject );
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Olivia', 'bar', true)" );
        assertCallEmpty( adminSubject, "CALL dbms.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectMultiRolePermissions() throws Exception
    {
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('schemaSubject', '" + READER + "')" );

        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertCallEmpty( schemaSubject, "CALL dbms.changePassword( '321' )" );
    }
}
