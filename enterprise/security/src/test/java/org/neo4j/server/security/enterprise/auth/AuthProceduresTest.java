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
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

public class AuthProceduresTest extends AuthProcedureTestBase
{

    //---------- Change own password -----------

    // Enterprise version of test in BuiltInProceduresIT.callChangePasswordWithAccessModeInDbmsMode.
    // Uses community edition procedure in BuiltInProcedures
    @Test
    public void shouldChangeOwnPassword() throws Exception
    {
        testCallEmpty( readSubject, "CALL dbms.changePassword( '321' )" );
        AuthSubject subject = manager.login( authToken( "readSubject", "321" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
    }

    //---------- Change user password -----------

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

    //---------- User creation -----------

    @Test
    public void shouldCreateUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('craig', '1234', true)" );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
    }

    @Test
    public void shouldNotCreateUserWithEmptyPassword() throws Exception
    {
        testCallFail( db, adminSubject, "CALL dbms.createUser('craig', '', true)", QueryExecutionException.class,
                "Password cannot be empty." );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {

        testCallEmpty( adminSubject, "CALL dbms.createUser('craig', '1234', true)" );
        assertNotNull( "User craig should exist", manager.getUser( "craig" ) );
        testCallFail( adminSubject, "CALL dbms.createUser('craig', '1234', true)", QueryExecutionException.class,
                "The specified user already exists" );
    }

    @Test
    public void shouldNotAllowNonAdminCreateUser() throws Exception
    {
        testFailCreateUser( noneSubject );
        testFailCreateUser( readSubject );
        testFailCreateUser( writeSubject );
        testFailCreateUser( schemaSubject );
    }

    //----------User and role management-----------
    @Test
    public void shouldAllowAddingAndRemovingUserFromRole() throws Exception
    {
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertTrue( "Should have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
    }

    @Test
    public void shouldAllowAddingUserToRoleMultipleTimes() throws Exception
    {
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertTrue( "Should have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
    }

    @Test
    public void shouldAllowRemovingUserFromRoleMultipleTimes() throws Exception
    {
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('readSubject', '" + PUBLISHER + "')" );
        assertTrue( "Should have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('readSubject', '" + PUBLISHER + "')" );
        assertFalse( "Should not have role publisher",
                ShiroAuthSubject.castOrFail( readSubject ).getSubject().hasRole( PUBLISHER ) );
    }

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

    @Test
    public void shouldNotAllowNonAdminAddingUserToRole() throws Exception
    {
        testFailAddUserToRoleAction( noneSubject );
        testFailAddUserToRoleAction( readSubject );
        testFailAddUserToRoleAction( writeSubject );
        testFailAddUserToRoleAction( schemaSubject );
    }

    @Test
    public void shouldNotAllowNonAdminRemovingUserFromRole() throws Exception
    {
        testFailRemoveUserFromRoleAction( noneSubject );
        testFailRemoveUserFromRoleAction( readSubject );
        testFailRemoveUserFromRoleAction( writeSubject );
        testFailRemoveUserFromRoleAction( schemaSubject );
    }

    //----------User deletion -----------

    @Test
    public void shouldDeleteUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Craig', '1234', true)" );
        assertNotNull( "User Craig should exist", manager.getUser( "Craig" ) );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Craig')" );
        assertNull( "User Craig should not exist", manager.getUser( "Craig" ) );
    }

    @Test
    public void shouldNotAllowNonAdminDeleteUser() throws Exception
    {
        testFailDeleteUser( noneSubject );
        testFailDeleteUser( readSubject );
        testFailDeleteUser( writeSubject );
        testFailDeleteUser( schemaSubject );
    }

    @Test
    public void shouldNotAllowDeletingNonExistingUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Craig', '1234', true)" );
        assertNotNull( "User Craig should exist", manager.getUser( "Craig" ) );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Craig')" );
        assertNull( "User Craig should not exist", manager.getUser( "Craig" ) );
        testCallFail( adminSubject, "CALL dbms.deleteUser('Craig')", QueryExecutionException.class,
                "The user 'Craig' does not exist" );
    }

    //----------User suspension scenarios-----------

    @Test
    public void shouldSuspendUser() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('readSubject')" );
        assertTrue( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldActivateUser() throws Exception
    {
        manager.suspendUser( "readSubject" );
        testCallEmpty( adminSubject, "CALL dbms.activateUser('readSubject')" );
        assertFalse( manager.getUser( "readSubject" ).hasFlag( FileUserRealm.IS_SUSPENDED ) );
    }

    @Test
    public void shouldFailOnNonAdminSuspend() throws Exception
    {
        testCallFail( schemaSubject, "CALL dbms.suspendUser('readSubject')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    @Test
    public void shouldFailOnNonAdminActivate() throws Exception
    {
        manager.suspendUser( "readSubject" );
        testCallFail( schemaSubject, "CALL dbms.activateUser('readSubject')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }

    //----------List users/roles-----------

    @Test
    public void shouldReturnUsers() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listUsers() YIELD username",
                r -> resultKeyIs( r, "username", "adminSubject", "readSubject", "schemaSubject",
                        "readWriteSubject", "noneSubject", "neo4j" ) );
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
        testFailListUsers( noneSubject, 5 );
        testFailListUsers( readSubject, 5 );
        testFailListUsers( writeSubject, 5 );
        testFailListUsers( schemaSubject, 5 );
    }

    @Test
    public void shouldReturnRoles() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRoles() YIELD role AS roles RETURN roles",
                r -> resultKeyIs( r, "roles", ADMIN, ARCHITECT, PUBLISHER, READER, "empty" ) );
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
        testFailListRoles( noneSubject );
        testFailListRoles( readSubject );
        testFailListRoles( writeSubject );
        testFailListRoles( schemaSubject );
    }

    @Test
    public void shouldListRolesForUser() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> resultKeyIs( r, "roles", ADMIN ) );
    }

    @Test
    public void shouldNotAllowNonAdminListUserRoles() throws Exception
    {
        testFailListUserRoles( noneSubject, "adminSubject" );
        testFailListUserRoles( readSubject, "adminSubject" );
        testFailListUserRoles( writeSubject, "adminSubject" );
        testFailListUserRoles( schemaSubject, "adminSubject" );
    }

    @Test
    public void shouldFailToListRolesForUnknownUser() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.listRolesForUser('Henrik') YIELD value as roles RETURN roles",
                QueryExecutionException.class, "User Henrik does not exist." );
    }

    @Test
    public void shouldListNoRolesForUserWithNoRoles() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.listRolesForUser('Henrik') YIELD value as roles RETURN roles" );
    }

    @Test
    public void shouldListUsersForRole() throws Exception
    {
        testResult( adminSubject, "CALL dbms.listUsersForRole('admin') YIELD value as users RETURN users",
                r -> resultKeyIs( r, "users", adminSubject.name(), "neo4j" ) );
    }

    @Test
    public void shouldNotAllowNonAdminListRoleUsers() throws Exception
    {
        testFailListRoleUsers( noneSubject, ADMIN );
        testFailListRoleUsers( readSubject, ADMIN );
        testFailListRoleUsers( writeSubject, ADMIN );
        testFailListRoleUsers( schemaSubject, ADMIN );
    }

    @Test
    public void shouldFailToListUsersForUnknownRole() throws Exception
    {
        testCallFail( adminSubject, "CALL dbms.listUsersForRole('Foo') YIELD value as users RETURN users",
                QueryExecutionException.class, "Role Foo does not exist." );
    }

    @Test
    public void shouldListNoUsersForRoleWithNoUsers() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.listUsersForRole('empty') YIELD value as users RETURN users" );
    }
}
