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

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.server.security.enterprise.auth.AuthProcedures.PERMISSION_DENIED;
import static org.neo4j.server.security.enterprise.auth.InternalFlatFileRealm.IS_SUSPENDED;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

public abstract class AuthProceduresTestLogic<S> extends AuthTestBase<S>
{
    static final String PWD_CHANGE = PASSWORD_CHANGE_REQUIRED.name().toLowerCase();

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    //---------- Change own password -----------

    // Enterprise version of test in BuiltInProceduresIT.callChangePasswordWithAccessModeInDbmsMode.
    // Uses community edition procedure in BuiltInProcedures
    @Test
    public void shouldChangeOwnPassword() throws Throwable
    {
        assertEmpty( readSubject, "CALL dbms.security.changePassword( '321' )" );
        neo.updateAuthToken( readSubject, "readSubject", "321" ); // Because RESTSubject caches an auth token that is sent with every request
        neo.assertAuthenticated( readSubject );
        testSuccessfulRead( readSubject, 3 );
    }

    @Test
    public void shouldChangeOwnPasswordEvenIfHasNoAuthorization() throws Throwable
    {
        neo.assertAuthenticated( noneSubject );
        assertEmpty( noneSubject, "CALL dbms.security.changePassword( '321' )" );
        neo.updateAuthToken( noneSubject, "noneSubject", "321" ); // Because RESTSubject caches an auth token that is sent with every request
        neo.assertAuthenticated( noneSubject );
    }

    @Test
    public void shouldNotChangeOwnPasswordIfNewPasswordInvalid() throws Exception
    {
        assertFail( readSubject, "CALL dbms.security.changePassword( '' )", "A password cannot be empty." );
        assertFail( readSubject, "CALL dbms.security.changePassword( '123' )",
                "Old password and new password cannot be the same." );
    }

    //---------- list running transactions -----------

    @Test
    public void shouldListSelfTransaction()
    {
        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
            r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );
    }

    @Test
    public void shouldNotListTransactionsIfNotAdmin()
    {
        assertFail( noneSubject, "CALL dbms.security.listTransactions()", PERMISSION_DENIED );
        assertFail( readSubject, "CALL dbms.security.listTransactions()", PERMISSION_DENIED );
        assertFail( writeSubject, "CALL dbms.security.listTransactions()", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.listTransactions()", PERMISSION_DENIED );
    }

    @Test
    public void shouldListTransactions() throws Throwable
    {
        ThreadedTransactionCreate<S> write1 = new ThreadedTransactionCreate<>( neo );
        ThreadedTransactionCreate<S> write2 = new ThreadedTransactionCreate<>( neo );
        write1.execute( threading, writeSubject );
        write2.execute( threading, writeSubject );
        write1.barrier.await();
        write2.barrier.await();

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions",
                        map( "adminSubject", "1", "writeSubject", "2" ) ) );

        write1.closeAndAssertSuccess();
        write2.closeAndAssertSuccess();
    }

    //---------- terminate transactions for user -----------

    @Test
    public void shouldTerminateTransactionForUser() throws Throwable
    {
        ThreadedTransactionCreate<S> write = new ThreadedTransactionCreate<>( neo );
        write.execute( threading, writeSubject );
        write.barrier.await();

        assertSuccess( adminSubject, "CALL dbms.security.terminateTransactionsForUser( 'writeSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "writeSubject", "1" ) ) );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        write.closeAndAssertTransactionTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    @Test
    public void shouldTerminateOnlyGivenUsersTransaction() throws Throwable
    {
        ThreadedTransactionCreate<S> schema = new ThreadedTransactionCreate<>( neo );
        ThreadedTransactionCreate<S> write = new ThreadedTransactionCreate<>( neo );

        schema.execute( threading, schemaSubject );
        write.execute( threading, writeSubject );

        schema.barrier.await();
        write.barrier.await();

        assertSuccess( adminSubject, "CALL dbms.security.terminateTransactionsForUser( 'schemaSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "schemaSubject", "1" ) ) );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r ->  assertKeyIsMap( r, "username", "activeTransactions",
                        map( "adminSubject", "1", "writeSubject", "1" ) ) );

        schema.closeAndAssertTransactionTermination();
        write.closeAndAssertSuccess();

        assertSuccess( adminSubject, "MATCH (n:Test) RETURN n.name AS name",
                r -> assertKeyIs( r, "name", "writeSubject-node" ) );
    }

    @Test
    public void shouldTerminateAllTransactionsForGivenUser() throws Throwable
    {
        ThreadedTransactionCreate<S> schema1 = new ThreadedTransactionCreate<>( neo );
        ThreadedTransactionCreate<S> schema2 = new ThreadedTransactionCreate<>( neo );

        schema1.execute( threading, schemaSubject );
        schema2.execute( threading, schemaSubject );

        schema1.barrier.await();
        schema2.barrier.await();

        assertSuccess( adminSubject, "CALL dbms.security.terminateTransactionsForUser( 'schemaSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "schemaSubject", "2" ) ) );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r ->  assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        schema1.closeAndAssertTransactionTermination();
        schema2.closeAndAssertTransactionTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    @Test
    public void shouldNotTerminateTerminationTransaction() throws InterruptedException, ExecutionException
    {
        assertSuccess( adminSubject, "CALL dbms.security.terminateTransactionsForUser( 'adminSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "adminSubject", "0" ) ) );
        assertSuccess( readSubject, "CALL dbms.security.terminateTransactionsForUser( 'readSubject' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( "readSubject", "0" ) ) );
    }

    @Test
    public void shouldTerminateSelfTransactionsExceptTerminationTransactionIfAdmin() throws Throwable
    {
        shouldTerminateSelfTransactionsExceptTerminationTransaction( adminSubject );
    }

    @Test
    public void shouldTerminateSelfTransactionsExceptTerminationTransactionIfNotAdmin() throws Throwable
    {
        shouldTerminateSelfTransactionsExceptTerminationTransaction( writeSubject );
    }

    private void shouldTerminateSelfTransactionsExceptTerminationTransaction( S subject ) throws Throwable
    {
        ThreadedTransactionCreate<S> create = new ThreadedTransactionCreate<>( neo );
        create.execute( threading, subject );
        create.barrier.await();

        String subjectName = neo.nameOf( subject );
        assertSuccess( subject, "CALL dbms.security.terminateTransactionsForUser( '" + subjectName + "' )",
                r -> assertKeyIsMap( r, "username", "transactionsTerminated", map( subjectName, "1" ) ) );

        create.closeAndAssertTransactionTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    @Test
    public void shouldNotTerminateTransactionsIfNonExistentUser() throws InterruptedException, ExecutionException
    {
        assertFail( adminSubject, "CALL dbms.security.terminateTransactionsForUser( 'Petra' )", "User 'Petra' does not exist" );
        assertFail( adminSubject, "CALL dbms.security.terminateTransactionsForUser( '' )", "User '' does not exist" );
    }

    @Test
    public void shouldNotTerminateTransactionsIfNotAdmin() throws Throwable
    {
        ThreadedTransactionCreate<S> write = new ThreadedTransactionCreate<>( neo );
        write.execute( threading, writeSubject );
        write.barrier.await();

        assertFail( noneSubject, "CALL dbms.security.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );
        assertFail( pwdSubject, "CALL dbms.security.terminateTransactionsForUser( 'writeSubject' )", CHANGE_PWD_ERR_MSG );
        assertFail( readSubject, "CALL dbms.security.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.terminateTransactionsForUser( 'writeSubject' )", PERMISSION_DENIED );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r -> assertKeyIs( r, "username", "adminSubject", "writeSubject" ) );

        write.closeAndAssertSuccess();

        assertSuccess( adminSubject, "MATCH (n:Test) RETURN n.name AS name",
                r -> assertKeyIs( r, "name", "writeSubject-node" ) );
    }

    //---------- change user password -----------

    // Should change password for admin subject and valid user
    @Test
    public void shouldChangeUserPassword() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321' )" );
        // TODO: uncomment and fix
        // testUnAuthenticated( readSubject );

        neo.assertInitFailed( neo.login( "readSubject", "123" ) );
        neo.assertAuthenticated( neo.login( "readSubject", "321" ) );
    }

    // Should fail vaguely to change password for non-admin subject, regardless of user and password
    @Test
    public void shouldNotChangeUserPasswordIfNotAdmin() throws Exception
    {
        assertFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321' )", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'jake', '321' )", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '' )", PERMISSION_DENIED );
    }

    // Should change own password for non-admin or admin subject
    @Test
    public void shouldChangeUserPasswordIfSameUser() throws Throwable
    {
        assertEmpty( readSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '321' )" );
        neo.updateAuthToken( readSubject, "readSubject", "321" ); // Because RESTSubject caches an auth token that is sent with every request
        neo.assertAuthenticated( readSubject );
        testSuccessfulRead( readSubject, 3 );

        assertEmpty( adminSubject, "CALL dbms.security.changeUserPassword( 'adminSubject', 'cba' )" );
        neo.updateAuthToken( adminSubject, "adminSubject", "cba" ); // Because RESTSubject caches an auth token that is sent with every request
        neo.assertAuthenticated( adminSubject );
        testSuccessfulRead( adminSubject, 3 );
    }

    // Should fail nicely to change own password for non-admin or admin subject if password invalid
    @Test
    public void shouldFailToChangeUserPasswordIfSameUserButInvalidPassword() throws Exception
    {
        assertFail( readSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same." );

        assertFail( adminSubject, "CALL dbms.security.changeUserPassword( 'adminSubject', 'abc' )",
                "Old password and new password cannot be the same." );
    }

    // Should fail nicely to change password for admin subject and non-existing user
    @Test
    public void shouldNotChangeUserPasswordIfNonExistentUser() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.changeUserPassword( 'jake', '321' )",
                "User 'jake' does not exist." );
    }

    // Should fail nicely to change password for admin subject and empty password
    @Test
    public void shouldNotChangeUserPasswordIfEmptyPassword() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '' )",
                "A password cannot be empty." );
    }

    // Should fail to change password for admin subject and same password
    @Test
    public void shouldNotChangeUserPasswordIfSamePassword() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.changeUserPassword( 'readSubject', '123' )",
                "Old password and new password cannot be the same." );
    }

    @Test
    public void shouldTerminateTransactionsOnChangeUserPassword() throws Throwable
    {
        shouldTerminateTransactionsForUser( writeSubject, "dbms.security.changeUserPassword( '%s', 'newPassword' )" );
    }

    @Test
    public void shouldTerminateConnectionsOnChangeUserPassword() throws Exception
    {
        TransportConnection conn = startBoltSession( "writeSubject", "abc" );
        assertSuccess( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                        "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount",
                r -> assertKeyIsMap( r, "username", "connectionCount", map( "writeSubject", IS_BOLT ? "2" : "1" ) ) );

        assertEmpty( adminSubject, "CALL dbms.security.changeUserPassword( 'writeSubject', 'newPassword' )" );

        assertEmpty( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                        "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount");
        conn.disconnect();
    }

    //---------- create user -----------

    @Test
    public void shouldCreateUser() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('craig', '1234', true)" );
        userManager.getUser( "craig" );
    }

    @Test
    public void shouldNotCreateUserIfInvalidUsername() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.createUser('', '1234', true)",
                "The provided user name is empty." );
        assertFail( adminSubject, "CALL dbms.security.createUser('&%ss!', '1234', true)",
                "User name '&%ss!' contains illegal characters." );
        assertFail( adminSubject, "CALL dbms.security.createUser('&%ss!', '', true)",
                "User name '&%ss!' contains illegal characters." );
    }

    @Test
    public void shouldNotCreateUserIfInvalidPassword() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.createUser('craig', '', true)", "A password cannot be empty." );
    }

    @Test
    public void shouldNotCreateExistingUser() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.createUser('readSubject', '1234', true)",
                "The specified user 'readSubject' already exists" );
        assertFail( adminSubject, "CALL dbms.security.createUser('readSubject', '', true)",
                "A password cannot be empty." );
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
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('noneSubject')" );
        try
        {
            userManager.getUser( "noneSubject" );
            fail( "User noneSubject should not exist" );
        }
        catch ( InvalidArgumentsException e )
        {
            assertTrue( "User noneSubject should not exist",
                    e.getMessage().contains( "User 'noneSubject' does not exist." ) );
        }

        userManager.addRoleToUser( PUBLISHER, "readSubject" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('readSubject')" );
        try
        {
            userManager.getUser( "readSubject" );
            fail( "User readSubject should not exist" );
        }
        catch ( InvalidArgumentsException e )
        {
            assertTrue( "User readSubject should not exist",
                    e.getMessage().contains( "User 'readSubject' does not exist." ) );
        }
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
    public void shouldNotAllowDeletingNonExistentUser() throws Exception
    {
        testFailDeleteUser( adminSubject, "Craig", "User 'Craig' does not exist." );
        testFailDeleteUser( adminSubject, "", "User '' does not exist." );
    }

    @Test
    public void shouldNotAllowDeletingYourself() throws Exception
    {
        testFailDeleteUser( adminSubject, "adminSubject", "Deleting yourself (user 'adminSubject') is not allowed." );
    }

    @Test
    public void shouldTerminateTransactionsOnUserDeletion() throws Throwable
    {
        shouldTerminateTransactionsForUser( writeSubject, "dbms.security.deleteUser( '%s' )" );
    }

    @Test
    public void shouldTerminateConnectionsOnUserDeletion() throws Exception
    {
        TransportConnection conn = startBoltSession( "writeSubject", "abc" );
        assertSuccess( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                        "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount",
                r -> assertKeyIsMap( r, "username", "connectionCount", map( "writeSubject", IS_BOLT ? "2" : "1" ) ) );

        assertEmpty( adminSubject, "CALL dbms.security.deleteUser( 'writeSubject' )" );

        assertEmpty( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount");
        conn.disconnect();
    }

    //---------- suspend user -----------

    @Test
    public void shouldSuspendUser() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    public void shouldSuspendSuspendedUser() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('readSubject')" );
        assertTrue( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    public void shouldFailToSuspendNonExistentUser() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.suspendUser('Craig')", "User 'Craig' does not exist." );
    }

    @Test
    public void shouldFailToSuspendIfNotAdmin() throws Exception
    {
        assertFail( schemaSubject, "CALL dbms.security.suspendUser('readSubject')", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.suspendUser('Craig')", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.suspendUser('')", PERMISSION_DENIED );
    }

    @Test
    public void shouldFailToSuspendYourself() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.suspendUser('adminSubject')",
                "Suspending yourself (user 'adminSubject') is not allowed." );
    }

    @Test
    public void shouldTerminateTransactionsOnUserSuspension() throws Throwable
    {
        shouldTerminateTransactionsForUser( writeSubject, "dbms.security.suspendUser( '%s' )" );
    }

    @Test
    public void shouldTerminateConnectionsOnUserSuspension() throws Exception
    {
        TransportConnection conn = startBoltSession( "writeSubject", "abc" );
        assertSuccess( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                        "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount",
                r -> assertKeyIsMap( r, "username", "connectionCount", map( "writeSubject", IS_BOLT ? "2" : "1" ) ) );

        assertEmpty( adminSubject, "CALL dbms.security.suspendUser( 'writeSubject' )" );

        assertEmpty( adminSubject, "CALL dbms.security.listConnections() YIELD username, connectionCount " +
                "WITH username, connectionCount WHERE username = 'writeSubject' RETURN username, connectionCount");
        conn.disconnect();
    }

    //---------- activate user -----------

    @Test
    public void shouldActivateUser() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertEmpty( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    public void shouldActivateActiveUser() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertEmpty( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        assertEmpty( adminSubject, "CALL dbms.security.activateUser('readSubject')" );
        assertFalse( userManager.getUser( "readSubject" ).hasFlag( IS_SUSPENDED ) );
    }

    @Test
    public void shouldFailToActivateNonExistentUser() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.activateUser('Craig')", "User 'Craig' does not exist." );
    }

    @Test
    public void shouldFailToActivateIfNotAdmin() throws Exception
    {
        userManager.suspendUser( "readSubject" );
        assertFail( schemaSubject, "CALL dbms.security.activateUser('readSubject')", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.activateUser('Craig')", PERMISSION_DENIED );
        assertFail( schemaSubject, "CALL dbms.security.activateUser('')", PERMISSION_DENIED );
    }

    @Test
    public void shouldFailToActivateYourself() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.activateUser('adminSubject')",
                "Activating yourself (user 'adminSubject') is not allowed." );
    }

    //---------- add user to role -----------

    @Test
    public void shouldAddRoleToUser() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'readSubject' )" );
        assertTrue( "Should have role publisher", userHasRole( "readSubject", PUBLISHER ) );
    }

    @Test
    public void shouldAddRetainUserInRole() throws Exception
    {
        assertTrue( "Should have role reader", userHasRole( "readSubject", READER ) );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'readSubject')" );
        assertTrue( "Should have still have role reader", userHasRole( "readSubject", READER ) );
    }

    @Test
    public void shouldFailToAddNonExistentUserToRole() throws Exception
    {
        testFailAddRoleToUser( adminSubject, PUBLISHER, "Olivia", "User 'Olivia' does not exist." );
        testFailAddRoleToUser( adminSubject, "thisRoleDoesNotExist", "Olivia", "User 'Olivia' does not exist." );
        testFailAddRoleToUser( adminSubject, "", "Olivia", "The provided role name is empty." );
    }

    @Test
    public void shouldFailToAddUserToNonExistentRole() throws Exception
    {
        testFailAddRoleToUser( adminSubject, "thisRoleDoesNotExist", "readSubject",
                "Role 'thisRoleDoesNotExist' does not exist." );
        testFailAddRoleToUser( adminSubject, "", "readSubject", "The provided role name is empty." );
    }

    @Test
    public void shouldFailToAddRoleToUserIfNotAdmin() throws Exception
    {
        testFailAddRoleToUser( pwdSubject, PUBLISHER, "readSubject", CHANGE_PWD_ERR_MSG );
        testFailAddRoleToUser( readSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailAddRoleToUser( writeSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );

        testFailAddRoleToUser( schemaSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailAddRoleToUser( schemaSubject, PUBLISHER, "Olivia", PERMISSION_DENIED );
        testFailAddRoleToUser( schemaSubject, "thisRoleDoesNotExist", "Olivia", PERMISSION_DENIED );
    }

    //---------- remove user from role -----------

    @Test
    public void shouldRemoveRoleFromUser() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + READER + "', 'readSubject')" );
        assertFalse( "Should not have role reader", userHasRole( "readSubject", READER ) );
    }

    @Test
    public void shouldKeepUserOutOfRole() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'readSubject')" );
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
    }

    @Test
    public void shouldFailToRemoveNonExistentUserFromRole() throws Exception
    {
        testFailRemoveRoleFromUser( adminSubject, PUBLISHER, "Olivia", "User 'Olivia' does not exist." );
        testFailRemoveRoleFromUser( adminSubject, "thisRoleDoesNotExist", "Olivia", "User 'Olivia' does not exist." );
        testFailRemoveRoleFromUser( adminSubject, "", "Olivia", "The provided role name is empty." );
        testFailRemoveRoleFromUser( adminSubject, "", "", "The provided role name is empty." );
        testFailRemoveRoleFromUser( adminSubject, PUBLISHER, "", "The provided user name is empty." );
    }

    @Test
    public void shouldFailToRemoveUserFromNonExistentRole() throws Exception
    {
        testFailRemoveRoleFromUser( adminSubject, "thisRoleDoesNotExist", "readSubject",
                "Role 'thisRoleDoesNotExist' does not exist." );
        testFailRemoveRoleFromUser( adminSubject, "", "readSubject", "The provided role name is empty." );
    }

    @Test
    public void shouldFailToRemoveRoleFromUserIfNotAdmin() throws Exception
    {
        testFailRemoveRoleFromUser( pwdSubject, PUBLISHER, "readSubject",CHANGE_PWD_ERR_MSG );
        testFailRemoveRoleFromUser( readSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( writeSubject, PUBLISHER, "readSubject", PERMISSION_DENIED );

        testFailRemoveRoleFromUser( schemaSubject, READER, "readSubject", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( schemaSubject, READER, "Olivia", PERMISSION_DENIED );
        testFailRemoveRoleFromUser( schemaSubject, "thisRoleDoesNotExist", "Olivia", PERMISSION_DENIED );
    }

    @Test
    public void shouldFailToRemoveYourselfFromAdminRole() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.removeRoleFromUser('" + ADMIN + "', 'adminSubject')",
                "Removing yourself (user 'adminSubject') from the admin role is not allowed." );
    }

    //---------- manage multiple roles -----------

    @Test
    public void shouldAllowAddingAndRemovingUserFromMultipleRoles() throws Exception
    {
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertFalse( "Should not have role architect", userHasRole( "readSubject", ARCHITECT ) );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'readSubject')" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'readSubject')" );
        assertTrue( "Should have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertTrue( "Should have role architect", userHasRole( "readSubject", ARCHITECT ) );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'readSubject')" );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + ARCHITECT + "', 'readSubject')" );
        assertFalse( "Should not have role publisher", userHasRole( "readSubject", PUBLISHER ) );
        assertFalse( "Should not have role architect", userHasRole( "readSubject", ARCHITECT ) );
    }

    //---------- list users -----------

    @Test
    public void shouldListUsers() throws Exception
    {
        assertSuccess( adminSubject, "CALL dbms.security.listUsers() YIELD username",
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
        userManager.addRoleToUser( READER, "writeSubject" );
        assertSuccess( adminSubject, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "roles", expected ) );
    }

    @Test
    public void shouldReturnUsersWithFlags() throws Exception
    {
        Map<String,Object> expected = map(
                "adminSubject", listOf(),
                "readSubject", listOf(),
                "schemaSubject", listOf(),
                "writeSubject", listOf( IS_SUSPENDED ),
                "pwdSubject", listOf( PWD_CHANGE, IS_SUSPENDED ),
                "noneSubject", listOf(),
                "neo4j", listOf( PWD_CHANGE.toLowerCase() )
        );
        userManager.suspendUser( "writeSubject" );
        userManager.suspendUser( "pwdSubject" );
        assertSuccess( adminSubject, "CALL dbms.security.listUsers()",
                r -> assertKeyIsMap( r, "username", "flags", expected ) );
    }

    @Test
    public void shouldShowCurrentUser() throws Exception
    {
        userManager.addRoleToUser( READER, "writeSubject" );
        assertSuccess( adminSubject, "CALL dbms.security.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "adminSubject", listOf( ADMIN ) ) ) );
        assertSuccess( readSubject, "CALL dbms.security.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "readSubject", listOf( READER ) ) ) );
        assertSuccess( schemaSubject, "CALL dbms.security.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles", map( "schemaSubject", listOf( ARCHITECT ) ) ) );
        assertSuccess( writeSubject, "CALL dbms.security.showCurrentUser()",
                r -> assertKeyIsMap( r, "username", "roles",
                        map( "writeSubject", listOf( READER, PUBLISHER ) ) ) );
        assertSuccess( noneSubject, "CALL dbms.security.showCurrentUser()",
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
        assertSuccess( adminSubject, "CALL dbms.security.listRoles() YIELD role",
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
        assertSuccess( adminSubject, "CALL dbms.security.listRoles()",
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
        assertSuccess( adminSubject, "CALL dbms.security.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        assertSuccess( adminSubject, "CALL dbms.security.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }

    @Test
    public void shouldListNoRolesForUserWithNoRoles() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.listRolesForUser('Henrik') YIELD value as roles RETURN roles" );
    }

    @Test
    public void shouldNotListRolesForNonExistentUser() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.listRolesForUser('Petra') YIELD value as roles RETURN roles",
                "User 'Petra' does not exist." );
        assertFail( adminSubject, "CALL dbms.security.listRolesForUser('') YIELD value as roles RETURN roles",
                "User '' does not exist." );
    }

    @Test
    public void shouldListOwnRolesRoles() throws Exception
    {
        assertSuccess( adminSubject, "CALL dbms.security.listRolesForUser('adminSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", ADMIN ) );
        assertSuccess( readSubject, "CALL dbms.security.listRolesForUser('readSubject') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", READER ) );
    }

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
        assertSuccess( adminSubject, "CALL dbms.security.listUsersForRole('admin') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "adminSubject", "neo4j" ) );
    }

    @Test
    public void shouldListNoUsersForRoleWithNoUsers() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.listUsersForRole('empty') YIELD value as users RETURN users" );
    }

    @Test
    public void shouldNotListUsersForNonExistentRole() throws Exception
    {
        assertFail( adminSubject, "CALL dbms.security.listUsersForRole('poodle') YIELD value as users RETURN users",
                "Role 'poodle' does not exist." );
        assertFail( adminSubject, "CALL dbms.security.listUsersForRole('') YIELD value as users RETURN users",
                "Role '' does not exist." );
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

    @Test
    public void shouldSetCorrectUnAuthenticatedPermissions() throws Throwable
    {
        S unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "MATCH (n) RETURN n", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CREATE (:Node)", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CREATE INDEX ON :Node(number)", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CALL dbms.security.changePassword( '321' )", "" );

        unknownUser = neo.login( "Batman", "Matban" );
        assertFail( unknownUser, "CALL dbms.security.createUser('Henrik', 'bar', true)", "" );
    }

    @Test
    public void shouldSetCorrectPasswordChangeRequiredPermissions() throws Throwable
    {
        testFailRead( pwdSubject, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( pwdSubject, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( pwdSubject, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertPasswordChangeWhenPasswordChangeRequired( pwdSubject, "321" );

        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( henrik );
        testFailRead( henrik, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( henrik, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( henrik, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertPasswordChangeWhenPasswordChangeRequired( henrik, "321" );

        assertEmpty( adminSubject, "CALL dbms.security.createUser('Olivia', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Olivia')" );
        S olivia = neo.login( "Olivia", "bar" );
        neo.assertPasswordChangeRequired( olivia );
        testFailRead( olivia, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
        testFailWrite( olivia, pwdReqErrMsg( WRITE_OPS_NOT_ALLOWED ) );
        testFailSchema( olivia, pwdReqErrMsg( SCHEMA_OPS_NOT_ALLOWED ) );
        assertFail( olivia, "CALL dbms.security.createUser('OliviasFriend', 'bar', false)", CHANGE_PWD_ERR_MSG );
        assertPasswordChangeWhenPasswordChangeRequired( olivia, "321" );
    }

    @Test
    public void shouldSetCorrectNoRolePermissions() throws Exception
    {
        testFailRead( noneSubject, 3 );
        testFailWrite( noneSubject );
        testFailSchema( noneSubject );
        testFailCreateUser( noneSubject, PERMISSION_DENIED );
        assertEmpty( noneSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectReaderPermissions() throws Exception
    {
        testSuccessfulRead( readSubject, 3 );
        testFailWrite( readSubject );
        testFailSchema( readSubject );
        testFailCreateUser( readSubject, PERMISSION_DENIED );
        assertEmpty( readSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectPublisherPermissions() throws Exception
    {
        testSuccessfulRead( writeSubject, 3 );
        testSuccessfulWrite( writeSubject );
        testFailSchema( writeSubject );
        testFailCreateUser( writeSubject, PERMISSION_DENIED );
        assertEmpty( writeSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectSchemaPermissions() throws Exception
    {
        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertEmpty( schemaSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectAdminPermissions() throws Exception
    {
        testSuccessfulRead( adminSubject, 3 );
        testSuccessfulWrite( adminSubject );
        testSuccessfulSchema( adminSubject );
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Olivia', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    @Test
    public void shouldSetCorrectMultiRolePermissions() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'schemaSubject')" );

        testSuccessfulRead( schemaSubject, 3 );
        testSuccessfulWrite( schemaSubject );
        testSuccessfulSchema( schemaSubject );
        testFailCreateUser( schemaSubject, PERMISSION_DENIED );
        assertEmpty( schemaSubject, "CALL dbms.security.changePassword( '321' )" );
    }

    // --------------------- helpers -----------------------

    private void shouldTerminateTransactionsForUser( S subject, String procedure ) throws Throwable
    {
        ThreadedTransactionCreate<S> userThread = new ThreadedTransactionCreate<>( neo );
        userThread.execute( threading, subject );
        userThread.barrier.await();

        assertEmpty( adminSubject, "CALL " + String.format(procedure, neo.nameOf( subject ) ) );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        userThread.closeAndAssertTransactionTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    private TransportConnection startBoltSession( String username, String password ) throws Exception
    {
        TransportConnection connection = new SocketConnection();
        HostnamePort address = new HostnamePort( "localhost:7687" );
        Map<String,Object> authToken = map( "principal", username, "credentials", password, "scheme", "basic" );

        connection.connect( address ).send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( init( "TestClient/1.1", authToken ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, eventuallyReceives( msgSuccess() ) );
        return connection;
    }

}
