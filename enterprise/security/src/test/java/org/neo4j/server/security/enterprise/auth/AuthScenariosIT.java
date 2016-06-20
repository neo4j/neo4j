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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testFailReadAction( subject, 3 );
        testCallEmpty( db, subject, "CALL dbms.changePassword( 'foo' )" );
        subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailWriteAction( subject );
        testSuccessfulReadAction( subject, 3 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED, subject.getAuthenticationResult() );
        testCallEmpty( db, subject, "CALL dbms.changePassword( 'foo' )" );
        subject = manager.login( authToken( "Henrik", "foo" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailReadAction( subject, 3 );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testFailWriteAction( subject );
        testSuccessfulReadAction( subject, 3 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailReadAction( subject, 3 );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testSuccessfulWriteAction( subject );
        testSuccessfulReadAction( subject, 4 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailReadAction( subject, 3 );
        testFailWriteAction( subject );
        testFailSchema( subject );
        testFailCreateUser( subject );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ARCHITECT + "')" );
        testSuccessfulWriteAction( subject );
        testSuccessfulReadAction( subject, 4 );
        testSuccessfulSchemaAction( subject );
        testFailCreateUser( subject );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulWriteAction( subject );
        testCallEmpty( db, adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailReadAction( subject, 4 );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testFailWriteAction( subject );
        testSuccessfulReadAction( subject, 4 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailWriteAction( subject );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testSuccessfulWriteAction( subject );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        testSuccessfulWriteAction( subject );
        testSuccessfulReadAction( subject, 4 );
        testCallEmpty( db, adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')" );
        testFailWriteAction( subject );
        testSuccessfulReadAction( subject, 4 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.deleteUser('Henrik')" );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testCallFail( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')",
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        testCallEmpty( db, adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testCallFail( db, adminSubject, "CALL dbms.removeUserFromRole('Henrik', '" + PUBLISHER + "')",
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testCallEmpty( db, adminSubject, "CALL dbms.deleteUser('Henrik')" );
        try
        {
            testSuccessfulReadAction( subject, 3 );
            fail( "Expected exception to be thrown" );
        }
        catch ( AuthenticationException e )
        {
            assertTrue( "Exception should contain 'User Henrik does not exist'",
                    e.getMessage().contains( "User Henrik does not exist" ) );
        }
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        subject.logout();
        testCallEmpty( db, adminSubject, "CALL dbms.suspendUser('Henrik')" );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + READER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testSuccessfulReadAction( subject, 3 );
        testCallEmpty( db, adminSubject, "CALL dbms.suspendUser('Henrik')" );
        testFailReadAction( subject, 3 );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.suspendUser('Henrik')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.FAILURE, subject.getAuthenticationResult() );
        testCallEmpty( db, adminSubject, "CALL dbms.activateUser('Henrik')" );
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
        testResult( db, adminSubject, "CALL dbms.listUsers() YIELD username AS users RETURN users",
                r -> resultContainsInAnyOrder( r, "users", "adminSubject", "readSubject", "schemaSubject",
                        "readWriteSubject", "noneSubject", "neo4j" ) );
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testResult( db, adminSubject, "CALL dbms.listUsers() YIELD username AS users RETURN users",
                r -> resultContainsInAnyOrder( r, "users", "adminSubject", "readSubject", "schemaSubject",
                        "readWriteSubject", "noneSubject", "Henrik", "neo4j" ) );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListUsers( subject, 6 );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
        testResult( db, subject, "CALL dbms.listUsers() YIELD username AS users RETURN users",
                r -> resultContainsInAnyOrder( r, "users", "adminSubject", "readSubject", "schemaSubject",
                        "readWriteSubject", "noneSubject", "Henrik", "neo4j" ) );
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
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListRoles( subject );
        testSuccessfulListRolesAction( adminSubject );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Henrik', '" + ADMIN + "')" );
        testSuccessfulListRolesAction( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig with password foo
    Admin adds user Craig to role Publisher
    Henrik logs in with correct password → ok
    Henrik lists all roles for user Craig → permission denied
    Admin lists all roles for user Craig → ok
    */
    @Test
    public void listingUserRoles() throws Exception
    {
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        testCallEmpty( db, adminSubject, "CALL dbms.addUserToRole('Craig', '" + PUBLISHER + "')" );
        AuthSubject subject = manager.login( authToken( "Henrik", "bar" ) );
        assertEquals( AuthenticationResult.SUCCESS, subject.getAuthenticationResult() );
        testFailListUserRoles( subject, "Craig" );
        testResult( db, adminSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> resultContainsInAnyOrder( r, "roles", PUBLISHER ) );
    }
}
