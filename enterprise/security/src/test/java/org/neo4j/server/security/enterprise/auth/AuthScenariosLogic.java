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

import org.neo4j.kernel.api.security.AuthenticationResult;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.security.enterprise.auth.AuthProcedures.*;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

/*
    This class is so far missing scenarios related to killing transactions and sessions. These are
        Delete user scenario 4
        Role management scenario 5 and 6
        Suspend user scenario 3

    Further, the scenario on calling procedures has been omitted for the time being

    -- johan teleman
 */
public abstract class AuthScenariosLogic<S> extends AuthTestBase<S>
{
    //---------- User creation -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Reader
    Henrik logs in with incorrect password → fail
    Henrik logs in with correct password → gets prompted to change
    Henrik starts read transaction → permission denied
    Henrik changes password to foo → ok
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    public void userCreation1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        S subject = neo.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
        subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, neo.authenticationResult( subject ) );
        testFailRead( subject, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        // TODO: REST doesn't allow changing your own password via procedure if you're in PASSWORD_CHANGE_REQUIRED mode
        if ( IS_EMBEDDED ) assertCallEmpty( subject, "CALL dbms.changePassword( 'foo' )" );
        else assertCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'Henrik', 'foo' )" );
        // remove above if-else ASAP
        subject = neo.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password (gets prompted to change - change to foo)
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role Reader
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    public void userCreation2() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, neo.authenticationResult( subject ) );
        // TODO: REST doesn't allow changing your own password via procedure if you're in PASSWORD_CHANGE_REQUIRED mode
        if ( IS_EMBEDDED ) assertCallEmpty( subject, "CALL dbms.changePassword( 'foo' )" );
        else assertCallEmpty( adminSubject, "CALL dbms.changeUserPassword( 'Henrik', 'foo' )" );
        // remove above if-else ASAP
        subject = neo.login( "Henrik", "foo" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailRead( subject, 3 );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role Publisher
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation3() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailRead( subject, 3 );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testFailSchema( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Henrik starts write transaction → permission denied
    Henrik starts schema transaction → permission denied
    Henrik creates user Craig → permission denied
    Admin adds user Henrik to role Architect
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → ok
    Henrik creates user Craig → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation4() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailRead( subject, 3 );
        testFailWrite( subject );
        testFailSchema( subject );
        testFailCreateUser( subject, PERMISSION_DENIED );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ARCHITECT + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testSuccessfulSchema( subject );
        testFailCreateUser( subject, PERMISSION_DENIED );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik creates user Craig → permission denied
    Henrik logs off
     */
    @Test
    public void userCreation5() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        testFailCreateUser( subject, PERMISSION_DENIED );
    }

    //---------- User deletion -----------

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Henrik logs in with correct password → fail
    */
    @Test
    public void userDeletion1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Admin adds user Henrik to role Publisher → fail
    */
    @Test
    public void userDeletion2() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        assertCallFail( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')",
                "User Henrik does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Admin deletes user Henrik
    Admin removes user Henrik from role Publisher → fail
    */
    @Test
    public void userDeletion3() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        assertCallFail( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')",
                "User Henrik does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    User Henrik logs in with correct password → ok
    Admin deletes user Henrik
    Henrik starts transaction with read query → fail
    */
    @Test
    public void userDeletion4() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        assertCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        assertCallFail( subject, "MATCH (n:Node) RETURN n",
                IS_EMBEDDED ? "Read operations are not allowed" : "Invalid username or password");
    }

    //---------- Role management -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik starts transaction with write query → ok
    Admin removes user Henrik from role Publisher
    Henrik starts transaction with read query → permission denied
    Admin adds Henrik to role Reader
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    public void roleManagement1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulWrite( subject );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailRead( subject, 4 );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts transaction with write query → permission denied
    Admin adds user Henrik to role Publisher → ok
    Admin adds user Henrik to role Publisher → ok
    Henrik starts transaction with write query → ok
    */
    @Test
    public void roleManagement2() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailWrite( subject );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testSuccessfulWrite( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Admin adds user Henrik to role Reader
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from role Publisher
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    public void roleManagement3() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Admin adds user Henrik to role Reader
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from all roles
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → permission denied
     */
    @Test
    public void roleManagement4() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + READER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailWrite( subject );
        testFailRead( subject, 4 );
    }

    //---------- User suspension -----------

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password → ok
    Henrik logs off
    Admin suspends user Henrik
    User Henrik logs in with correct password → fail
     */
    @Test
    public void userSuspension1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        neo.logout( subject );
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Reader
    Henrik logs in with correct password → ok
    Henrik starts and completes transaction with read query → ok
    Admin suspends user Henrik
    Henrik’s session is terminated
    Henrik logs in with correct password → fail
     */
    @Test
    public void userSuspension2() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );

        // TODO: uncomment and fix
        testUnAuthenticated( subject );

        subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
    }

    //---------- User activation -----------

    /*
    Admin creates user Henrik with password bar
    Admin suspends user Henrik
    Henrik logs in with correct password → fail
    Admin reinstates user Henrik
    Henrik logs in with correct password → ok
     */
    @Test
    public void userActivation1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
        assertCallEmpty( adminSubject, "CALL dbms.activateUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
    }

    //---------- list users / roles -----------

    /*
    Admin lists all users → ok
    Admin creates user Henrik with password bar
    Admin lists all users → ok
    Henrik logs in with correct password → ok
    Henrik lists all users → permission denied
    Admin adds user Henrik to role Admin
    Henrik lists all users → ok
    */
    @Test
    public void userListing() throws Throwable
    {
        testSuccessfulListUsers( adminSubject, initialUsers );
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testSuccessfulListUsers( adminSubject, with( initialUsers, "Henrik" ) );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailListUsers( subject, 6, PERMISSION_DENIED );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
        testSuccessfulListUsers( subject, with( initialUsers, "Henrik" ) );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password → ok
    Henrik lists all roles → permission denied
    Admin lists all roles → ok
    Admin adds user Henrik to role Admin
    Henrik lists all roles → ok
    */
    @Test
    public void rolesListing() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailListRoles( subject, PERMISSION_DENIED);
        testSuccessfulListRoles( adminSubject, initialRoles );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
        testSuccessfulListRoles( subject, initialRoles );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Craig to role Publisher
    Henrik logs in with correct password → ok
    Henrik lists all roles for user Craig → permission denied
    Admin lists all roles for user Craig → ok
    Admin adds user Henrik to role Publisher
    Craig logs in with correct password → ok
    Craig lists all roles for user Craig → ok
    */
    @Test
    public void listingUserRoles() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );

        testFailListUserRoles( subject, "Craig", PERMISSION_DENIED );
        executeQuery( adminSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", PUBLISHER ) );

        S craigSubject = neo.login( "Craig", "foo" );
        executeQuery( craigSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", PUBLISHER ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Henrik to role Publisher
    Admin adds user Craig to role Publisher
    Henrik logs in with correct password → ok
    Henrik lists all users for role Publisher → permission denied
    Admin lists all users for role Publisher → ok
    */
    @Test
    public void listingRoleUsers() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        S subject = neo.login( "Henrik", "bar" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testFailListRoleUsers( subject, PUBLISHER, PERMISSION_DENIED );
        executeQuery( adminSubject,
                "CALL dbms.listUsersForRole('" + PUBLISHER + "') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "Henrik", "Craig", "writeSubject" ) );
    }

    //---------- change password -----------

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role Reader
    Henrik logs in with correct password → ok
    Henrik starts transaction with read query → ok
    Henrik changes password to 123
    Henrik starts transaction with read query → ok
    Henrik logs out
    Henrik logs in with password abc → fail
    Henrik logs in with password 123 → ok
    Henrik starts transaction with read query → ok
    Henrik logs out
     */
    @Test
    public void changeUserPassword1() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        S subject = neo.login( "Henrik", "abc" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
        assertCallEmpty( subject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        //TODO: uncomment the next line and make the test pass
        //testSuccessfulRead( subject, 3 );
        neo.logout( subject );
        subject = neo.login( "Henrik", "abc" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
        subject = neo.login( "Henrik", "123" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role Reader
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Admin changes user Henrik’s password to 123
    Henrik logs out
    Henrik logs in with password abc → fail
    Henrik logs in with password 123 → ok
    Henrik starts transaction with read query → ok
    Henrik logs out
     */
    @Test
    public void changeUserPassword2() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        S subject = neo.login( "Henrik", "abc" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
        assertCallEmpty( adminSubject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        neo.logout( subject );
        subject = neo.login( "Henrik", "abc" );
        assertEquals( AuthenticationResult.FAILURE, neo.authenticationResult( subject ) );
        subject = neo.login( "Henrik", "123" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin creates user Craig
    Admin adds user Henrik to role Reader
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Henrik changes Craig’s password to 123 → fail
     */
    @Test
    public void changeUserPassword3() throws Throwable
    {
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'abc', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        S subject = neo.login( "Henrik", "abc" );
        assertEquals( AuthenticationResult.SUCCESS, neo.authenticationResult( subject ) );
        testSuccessfulRead( subject, 3 );
        assertCallFail( subject, "CALL dbms.changeUserPassword('Craig', '123')", PERMISSION_DENIED );
    }
}
