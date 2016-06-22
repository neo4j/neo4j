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

import org.apache.shiro.authc.AuthenticationException;
import org.junit.Test;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
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
public class AuthScenariosIT extends AuthProcedureTestBase
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
    public void userCreation1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testFailRead( subject, 3 );
        testCallEmpty( subject, "CALL dbms.changePassword( 'foo' )" );
        subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
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
    public void userCreation2() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testCallEmpty( subject, "CALL dbms.changePassword( 'foo' )" );
        subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3 );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
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
    public void userCreation3() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3 );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
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
    public void userCreation4() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailRead( subject, 3 );
        testFailWrite( subject );
        testFailSchema( subject );
        testFailCreateUser( subject );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ARCHITECT + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testSuccessfulSchema( subject );
        testFailCreateUser( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik creates user Craig → permission denied
    Henrik logs off
     */
    @Test
    public void userCreation5() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        testFailCreateUser( subject );
    }

    //---------- User deletion -----------

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Henrik logs in with correct password → fail
    */
    @Test
    public void userDeletion1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
    }

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Admin adds user Henrik to role Publisher → fail
    */
    @Test
    public void userDeletion2() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testCallFail( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')",
                QueryExecutionException.class, "User Henrik does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Admin deletes user Henrik
    Admin removes user Henrik from role Publisher → fail
    */
    @Test
    public void userDeletion3() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testCallFail( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')",
                QueryExecutionException.class, "User Henrik does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    User Henrik logs in with correct password → ok
    Admin deletes user Henrik
    Henrik starts transaction with read query → fail
    */
    @Test
    public void userDeletion4() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testCallEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testFailRead( subject, 3 );
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
    public void roleManagement1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulWrite( subject );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailRead( subject, 4 );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
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
    public void roleManagement2() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailWrite( subject );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
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
    public void roleManagement3() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
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
    public void roleManagement4() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + READER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
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
    public void userSuspension1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        subject.logout();
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
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
    public void userSuspension2() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulRead( subject, 3 );
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        testFailRead( subject, 3 );

        // TODO: Check that user session is terminated instead of checking failed read
        subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
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
    public void userActivation1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        testCallEmpty( adminSubject, "CALL dbms.activateUser('Henrik')" );
        subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
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
    public void userListing() throws Exception
    {
        testSuccessfulListUsers( adminSubject, initialUsers );
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testSuccessfulListUsers( adminSubject, with( initialUsers, "Henrik" ) );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListUsers( subject, 6 );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
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
    public void rolesListing() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListRoles( subject );
        testSuccessfulListRoles( adminSubject, initialRoles );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
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
    Henrik lists all roles for user Henrik → ok
    */
    @Test
    public void listingUserRoles() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );

        testFailListUserRoles( subject, "Craig" );
        testResult( adminSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", PUBLISHER ) );

        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        //TODO: uncomment the next line and make the test pass
        //testResult( subject, "CALL dbms.listRolesForUser('Henrik') YIELD value as roles RETURN roles",
        //        r -> assertKeyIs( r, "roles", PUBLISHER ) );
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
    public void listingRoleUsers() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListRoleUsers( subject, PUBLISHER );
        testResult( adminSubject,
                "CALL dbms.listUsersForRole('" + PUBLISHER + "') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "Henrik", "Craig", writeSubject.name() ) );
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
    public void changeUserPassword1() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "abc" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulRead( subject, 3 );
        testCallEmpty( subject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        //TODO: uncomment the next line and make the test pass
        //testSuccessfulRead( subject, 3 );
        subject.logout();
        subject = manager.login( authToken( "Henrik", "abc" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        subject = manager.login( authToken( "Henrik", "123" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
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
    public void changeUserPassword2() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "abc" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulRead( subject, 3 );
        testCallEmpty( adminSubject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        subject.logout();
        subject = manager.login( authToken( "Henrik", "abc" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        subject = manager.login( authToken( "Henrik", "123" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
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
    public void changeUserPassword3() throws Exception
    {
        testCallEmpty( adminSubject, "CALL dbms.createUser('Craig', 'abc', false)" );
        testCallEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        testCallEmpty( adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "abc" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulRead( subject, 3 );
        testCallFail( subject, "CALL dbms.changeUserPassword('Craig', '123')",
                QueryExecutionException.class, AuthProcedures.PERMISSION_DENIED );
    }
}
