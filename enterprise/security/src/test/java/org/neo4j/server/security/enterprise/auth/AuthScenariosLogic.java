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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.neo4j.server.security.enterprise.auth.AuthProcedures.*;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READ_WRITE_SCHEMA;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READ_WRITE;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READ;

public abstract class AuthScenariosLogic<S> extends AuthTestBase<S>
{

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    //---------- User creation -----------

    @Test
    public void readOperationsShouldNotBeAllowedWhenPasswordChangeRequired() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        testFailRead( subject, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
    }

    @Test
    public void passwordChangeShouldEnableRolePermissions() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        assertPasswordChangeWhenPasswordChangeRequired( subject, "foo" );
        subject = neo.login( "Henrik", "foo" );
        neo.assertAuthenticated( subject );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    @Test
    public void loginShouldFailWithIncorrectPassword() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "foo" );
        neo.assertInitFailed( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password (gets prompted to change - change to foo)
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role 'Read'
    Henrik starts write transaction → permission denied
    Henrik starts read transaction → ok
    Henrik logs off
    */
    @Test
    public void userCreation2() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', true)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        assertPasswordChangeWhenPasswordChangeRequired( subject, "foo" );
        subject = neo.login( "Henrik", "foo" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts read transaction → permission denied
    Admin adds user Henrik to role 'ReadWrite'
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation3() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
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
    Admin adds user Henrik to role 'ReadWriteSchema'
    Henrik starts write transaction → ok
    Henrik starts read transaction → ok
    Henrik starts schema transaction → ok
    Henrik creates user Craig → permission denied
    Henrik logs off
    */
    @Test
    public void userCreation4() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        testFailWrite( subject );
        testFailSchema( subject );
        testFailCreateUser( subject, PERMISSION_DENIED );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE_SCHEMA + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        testSuccessfulSchema( subject );
        testFailCreateUser( subject, PERMISSION_DENIED );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Henrik creates user Craig → permission denied
    Henrik logs off
     */
    @Test
    public void userCreation5() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Admin adds user Henrik to role 'ReadWrite' → fail
    */
    @Test
    public void userDeletion2() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        assertFail( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')",
                "User 'Henrik' does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Admin deletes user Henrik
    Admin removes user Henrik from role 'ReadWrite' → fail
    */
    @Test
    public void userDeletion3() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        assertEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        assertFail( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')",
                "User 'Henrik' does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    User Henrik logs in with correct password → ok
    Admin deletes user Henrik
    Henrik starts transaction with read query → fail
    Henrik tries to login again → fail
    */
    @Test
    public void userDeletion4() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );
        assertEmpty( adminSubject, "CALL dbms.deleteUser('Henrik')" );
        testSessionKilled( henrik );
        henrik = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( henrik );
    }

    //---------- Role management -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Henrik starts transaction with write query → ok
    Admin removes user Henrik from role 'ReadWrite'
    Henrik starts transaction with read query → permission denied
    Admin adds Henrik to role 'Read'
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    public void roleManagement1() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulWrite( subject );
        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );
        testFailRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Henrik logs in with correct password
    Henrik starts transaction with write query → permission denied
    Admin adds user Henrik to role 'ReadWrite' → ok
    Admin adds user Henrik to role 'ReadWrite' → ok
    Henrik starts transaction with write query → ok
    */
    @Test
    public void roleManagement2() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailWrite( subject );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        testSuccessfulWrite( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Admin adds user Henrik to role 'Read'
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from role 'ReadWrite'
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → ok
    */
    @Test
    public void roleManagement3() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );
        testFailWrite( subject );
        testSuccessfulRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Admin adds user Henrik to role 'Read'
    Henrik starts transaction with write query → ok
    Henrik starts transaction with read query → ok
    Admin removes user Henrik from all roles
    Henrik starts transaction with write query → permission denied
    Henrik starts transaction with read query → permission denied
     */
    @Test
    public void roleManagement4() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ + "')" );
        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );
        testFailWrite( subject );
        testFailRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Henrik starts transaction with long running writing query Q
    Admin removes user Henrik from role 'ReadWrite' (while Q still running)
    Q finishes and transaction is committed → ok
    Henrik starts new transaction with write query → permission denied
     */
    @Test
    public void roleManagement5() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        ThreadedTransactionCreate<S> write = new ThreadedTransactionCreate<>( neo );
        write.execute( threading, henrik );
        write.barrier.await();

        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );

        write.closeAndAssertException( AuthorizationViolationException.class,
                "Write operations are not allowed for 'Henrik'." );
        testFailWrite( henrik );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password
    Henrik starts transaction with long running writing query Q with periodic commit
    Q commits a couple of times → ok
    Admin adds user Henrik to role 'Read'
    Admin removes Henrik from role 'ReadWrite' (while Q still running)
    Q commits again → permission denied, kill rest of query??? ok???
    Henrik starts transaction with write query → permission denied
     */
    @Test
    public void roleManagement6() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        ThreadedTransactionPeriodicCommit<S> perCommit = new ThreadedTransactionPeriodicCommit<>( neo );
        perCommit.execute( threading, henrik );
        perCommit.barrier.await();

        // Would prefer something more robust here, but could not get it to work
        // This test might turn flaky if committing the initial lines takes longer than 100 ms
        // Ideally we would be polling for the correct number of nodes, given some timeout
        Thread.sleep( 100 );

        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );

        perCommit.closeAndAssertError( "Write operations are not allowed for 'Henrik'." );

        testFailWrite( henrik );

        assertSuccess( henrik, "MATCH (n) RETURN n.name as name",
                r -> assertKeyIs( r, "name", "node0", "node1", "node2", "line1", "line2" ) );
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        neo.logout( subject );
        assertEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'Read'
    Henrik logs in with correct password → ok
    Henrik starts and completes transaction with read query → ok
    Admin suspends user Henrik
    Henrik’s session is terminated
    Henrik logs in with correct password → fail
     */
    @Test
    public void userSuspension2() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );

        testSessionKilled( subject );

        subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.suspendUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
        assertEmpty( adminSubject, "CALL dbms.activateUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        testSuccessfulListUsers( adminSubject, with( initialUsers, "Henrik" ) );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListUsers( subject, 6, PERMISSION_DENIED );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + ADMIN + "')" );
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoles( subject, PERMISSION_DENIED);
        testSuccessfulListRoles( adminSubject, initialRoles );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + ADMIN + "')" );
        testSuccessfulListRoles( subject, initialRoles );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Craig to role 'ReadWrite'
    Henrik logs in with correct password → ok
    Henrik lists all roles for user Craig → permission denied
    Admin lists all roles for user Craig → ok
    Admin adds user Henrik to role 'ReadWrite'
    Craig logs in with correct password → ok
    Craig lists all roles for user Craig → ok
    */
    @Test
    public void listingUserRoles() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Craig', '" + READ_WRITE + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );

        testFailListUserRoles( subject, "Craig", PERMISSION_DENIED );
        assertSuccess( adminSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READ_WRITE ) );

        S craigSubject = neo.login( "Craig", "foo" );
        assertSuccess( craigSubject, "CALL dbms.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READ_WRITE ) );
    }

    /*
    Admin creates user Henrik with password bar
    Admin creates user Craig
    Admin adds user Henrik to role 'ReadWrite'
    Admin adds user Craig to role 'ReadWrite'
    Henrik logs in with correct password → ok
    Henrik lists all users for role 'ReadWrite' → permission denied
    Admin lists all users for role 'ReadWrite' → ok
    */
    @Test
    public void listingRoleUsers() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.createUser('Craig', 'foo', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Craig', '" + READ_WRITE + "')" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoleUsers( subject, READ_WRITE, PERMISSION_DENIED );
        assertSuccess( adminSubject,
                "CALL dbms.listUsersForRole('" + READ_WRITE + "') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "Henrik", "Craig", "writeSubject" ) );
    }

    //---------- calling procedures -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role 'ReadWrite'
    Henrik logs in with correct password → ok
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin adds user Henrik to role 'Read'
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin removes Henrik from role 'ReadWrite'
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → permission denied
     */
    @Test
    public void callProcedures1() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ_WRITE + "')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                 r -> assertKeyIs( r, "count", "4" ) );

        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                r -> assertKeyIs( r, "count", "5" ) );

        assertEmpty( adminSubject, "CALL dbms.removeRoleFromUser('Henrik', '" + READ_WRITE + "')" );

        assertFail( henrik, "CALL test.createNode()", "Write operations are not allowed for 'Henrik'." );
    }

    //---------- change password -----------

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role 'Read'
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( subject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        neo.updateAuthToken( subject, "Henrik", "123" ); // Because RESTSubject caches an auth token that is sent with every request
        testSuccessfulRead( subject, 3 );
        neo.logout( subject );
        subject = neo.login( "Henrik", "abc" );
        neo.assertInitFailed( subject );
        subject = neo.login( "Henrik", "123" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin adds user Henrik to role 'Read'
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
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.changeUserPassword('Henrik', '123')" );
        neo.logout( subject );
        subject = neo.login( "Henrik", "abc" );
        neo.assertInitFailed( subject );
        subject = neo.login( "Henrik", "123" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
    }

    /*
    Admin creates user Henrik with password abc
    Admin creates user Craig
    Admin adds user Henrik to role 'Read'
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Henrik changes Craig’s password to 123 → fail
     */
    @Test
    public void changeUserPassword3() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.createUser('Craig', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.addRoleToUser('Henrik', '" + READ + "')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertFail( subject, "CALL dbms.changeUserPassword('Craig', '123')", PERMISSION_DENIED );
    }
}
