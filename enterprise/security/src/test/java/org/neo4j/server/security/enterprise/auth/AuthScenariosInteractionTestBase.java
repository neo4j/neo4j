/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.commons.io.Charsets;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

public abstract class AuthScenariosInteractionTestBase<S> extends ProcedureInteractionTestBase<S>
{

    //---------- User creation -----------

    @Test
    public void readOperationsShouldNotBeAllowedWhenPasswordChangeRequired() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        testFailRead( subject, 3, pwdReqErrMsg( READ_OPS_NOT_ALLOWED ) );
    }

    @Test
    public void passwordChangeShouldEnableRolePermissions() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "foo" );
        neo.assertInitFailed( subject );
    }

    /*
     * Logging scenario smoke test
     */
    @Test
    public void shouldLogSecurityEvents() throws Exception
    {
        S mats = neo.login( "mats", "neo4j" );
        // for REST, login doesn't happen until the subject does something
        neo.executeQuery( mats, "UNWIND [] AS i RETURN 1", Collections.emptyMap(), r -> {} );
        assertEmpty( adminSubject, "CALL dbms.security.createUser('mats', 'neo4j', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.createRole('role1')" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteRole('role1')" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('reader', 'mats')" );
        mats = neo.login( "mats", "neo4j" );
        assertEmpty( mats, "MATCH (n) WHERE id(n) < 0 RETURN 1" );
        assertFail( mats, "CALL dbms.security.changeUserPassword('neo4j', 'hackerPassword')", PERMISSION_DENIED );
        assertFail( mats, "CALL dbms.security.changeUserPassword('mats', '')", "A password cannot be empty." );
        assertEmpty( mats, "CALL dbms.security.changeUserPassword('mats', 'hackerPassword')" );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('reader', 'mats')" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('mats')" );

        // flush log
        neo.getLocalGraph().shutdown();

        // assert on log content
        SecurityLog log = new SecurityLog();
        log.load();

        log.assertHasLine( "mats", "logged in" );
        log.assertHasLine( "adminSubject", "created user `mats`" );
        log.assertHasLine( "adminSubject", "created role `role1`" );
        log.assertHasLine( "adminSubject", "deleted role `role1`" );
        log.assertHasLine( "mats", "logged in" );
        log.assertHasLine( "adminSubject", "added role `reader` to user `mats`" );
        log.assertHasLine( "mats", "tried to change password for user `neo4j`: " + PERMISSION_DENIED);
        log.assertHasLine( "mats", "tried to change password: A password cannot be empty.");
        log.assertHasLine( "mats", "changed password");
        log.assertHasLine( "adminSubject", "removed role `reader` from user `mats`");
        log.assertHasLine( "adminSubject", "deleted user `mats`");
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', true)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertPasswordChangeRequired( subject );
        assertPasswordChangeWhenPasswordChangeRequired( subject, "foo" );
        subject = neo.login( "Henrik", "foo" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailRead( subject, 3 );
        testFailWrite( subject );
        testFailSchema( subject );
        testFailCreateUser( subject, PERMISSION_DENIED );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ARCHITECT + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
    }

    /*
    Admin creates user Henrik with password bar
    Admin deletes user Henrik
    Admin adds user Henrik to role Publisher → fail
    */
    @Test
    public void userDeletion2()
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        assertFail( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')",
                "User 'Henrik' does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Admin deletes user Henrik
    Admin removes user Henrik from role Publisher → fail
    */
    @Test
    public void userDeletion3()
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        assertFail( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')",
                "User 'Henrik' does not exist" );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    User Henrik logs in with correct password → ok
    Admin deletes user Henrik
    Henrik starts transaction with read query → fail
    Henrik tries to login again → fail
    */
    @Test
    public void userDeletion4() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );
        assertEmpty( adminSubject, "CALL dbms.security.deleteUser('Henrik')" );
        neo.assertSessionKilled( henrik );
        henrik = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( henrik );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulWrite( subject );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
        testFailRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailWrite( subject );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        testSuccessfulWrite( subject );
        testSuccessfulRead( subject, 4 );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + READER + "', 'Henrik')" );
        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );
        testFailWrite( subject );
        testFailRead( subject, 4 );
    }

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password
    Henrik starts transaction with long running writing query Q
    Admin removes user Henrik from role Publisher (while Q still running)
    Q finishes and transaction is committed → ok
    Henrik starts new transaction with write query → permission denied
     */
    @Test
    public void roleManagement5() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> write = new ThreadedTransaction<>( neo, latch );
        write.executeCreateNode( threading, henrik );
        latch.startAndWaitForAllToStart();

        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );

        latch.finishAndWaitForAllToFinish();

        write.closeAndAssertSuccess();
        testFailWrite( henrik );
    }

    /*
     * Procedure 'test.allowedReadProcedure' with READ mode and 'allowed = role1' is loaded.
     * Procedure 'test.allowedWriteProcedure' with WRITE mode and 'allowed = role1' is loaded.
     * Procedure 'test.allowedSchemaProcedure' with SCHEMA mode and 'allowed = role1' is loaded.
     * Admin creates a new user 'mats'.
     * 'mats' logs in.
     * 'mats' executes the procedures, access denied.
     * Admin creates 'role1'.
     * 'mats' executes the procedures, access denied.
     * Admin adds role 'role1' to 'mats'.
     * 'mats' executes the procedures successfully.
     * Admin removes the role 'role1'.
     * 'mats' executes the procedures, access denied.
     * Admin creates the role 'role1' again (new).
     * 'mats' executes the procedures, access denied.
     * Admin adds role 'architect' to 'mats'.
     * 'mats' executes the procedures successfully.
     * Admin adds 'role1' to 'mats'.
     * 'mats' executes the procedures successfully.
     */
    @Test
    public void customRoleWithProcedureAccess() throws Exception
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('mats', 'neo4j', false)" );
        S mats = neo.login( "mats", "neo4j" );
        testFailTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.createRole('role1')" );
        testFailTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('role1', 'mats')" );
        testSuccessfulTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.deleteRole('role1')" );
        testFailTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.createRole('role1')" );
        testFailTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('architect', 'mats')" );
        testSuccessfulTestProcs( mats );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('role1', 'mats')" );
        testSuccessfulTestProcs( mats );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        neo.logout( subject );
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );
        subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );

        neo.assertSessionKilled( subject );

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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.suspendUser('Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertInitFailed( subject );
        assertEmpty( adminSubject, "CALL dbms.security.activateUser('Henrik', false)" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        testSuccessfulListUsers( adminSubject, with( initialUsers, "Henrik" ) );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListUsers( subject, 6, PERMISSION_DENIED );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoles( subject, PERMISSION_DENIED);
        testSuccessfulListRoles( adminSubject, initialRoles );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + ADMIN + "', 'Henrik')" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Craig', 'foo', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Craig')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );

        testFailListUserRoles( subject, "Craig", PERMISSION_DENIED );
        assertSuccess( adminSubject, "CALL dbms.security.listRolesForUser('Craig') YIELD value as roles RETURN roles",
                r -> assertKeyIs( r, "roles", PUBLISHER ) );

        S craigSubject = neo.login( "Craig", "foo" );
        assertSuccess( craigSubject, "CALL dbms.security.listRolesForUser('Craig') YIELD value as roles RETURN roles",
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Craig', 'foo', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Craig')" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( subject );
        testFailListRoleUsers( subject, PUBLISHER, PERMISSION_DENIED );
        assertSuccess( adminSubject,
                "CALL dbms.security.listUsersForRole('" + PUBLISHER + "') YIELD value as users RETURN users",
                r -> assertKeyIs( r, "users", "Henrik", "Craig", "writeSubject" ) );
    }

    //---------- calling procedures -----------

    /*
    Admin creates user Henrik with password bar
    Admin adds user Henrik to role Publisher
    Henrik logs in with correct password → ok
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin adds user Henrik to role Reader
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → ok
    Admin removes Henrik from role Publisher
    Henrik calls procedure marked as read-only → ok
    Henrik calls procedure marked as read-write → permission denied
     */
    @Test
    public void callProcedures1() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'bar', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + PUBLISHER + "', 'Henrik')" );
        S henrik = neo.login( "Henrik", "bar" );
        neo.assertAuthenticated( henrik );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                 r -> assertKeyIs( r, "count", "4" ) );

        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );

        assertEmpty( henrik, "CALL test.createNode()" );
        assertSuccess( henrik, "CALL test.numNodes() YIELD count as count RETURN count",
                r -> assertKeyIs( r, "count", "5" ) );

        assertEmpty( adminSubject, "CALL dbms.security.removeRoleFromUser('" + PUBLISHER + "', 'Henrik')" );

        assertFail( henrik, "CALL test.createNode()",
                "Write operations are not allowed for user 'Henrik' with roles [reader]." );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( subject, "CALL dbms.security.changeUserPassword('Henrik', '123', false)" );
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
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertEmpty( adminSubject, "CALL dbms.security.changeUserPassword('Henrik', '123', false)" );
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
    Admin adds user Henrik to role Reader
    Henrik logs in with password abc → ok
    Henrik starts transaction with read query → ok
    Henrik changes Craig’s password to 123 → fail
     */
    @Test
    public void changeUserPassword3() throws Throwable
    {
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Craig', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.createUser('Henrik', 'abc', false)" );
        assertEmpty( adminSubject, "CALL dbms.security.addRoleToUser('" + READER + "', 'Henrik')" );
        S subject = neo.login( "Henrik", "abc" );
        neo.assertAuthenticated( subject );
        testSuccessfulRead( subject, 3 );
        assertFail( subject, "CALL dbms.security.changeUserPassword('Craig', '123')", PERMISSION_DENIED );
    }

    // OTHER TESTS

    @Test
    public void shouldNotTryToCreateTokensWhenReading()
    {
        assertEmpty( adminSubject, "CREATE (:MyNode)" );

        assertSuccess( readSubject, "MATCH (n:MyNode) WHERE n.nonExistent = 'foo' RETURN toString(count(*)) AS c",
                r -> assertKeyIs( r, "c", "0" ) );
        assertFail( readSubject, "MATCH (n:MyNode) SET n.nonExistent = 'foo' RETURN toString(count(*)) AS c",
                TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertFail( readSubject, "MATCH (n:MyNode) SET n:Foo RETURN toString(count(*)) AS c",
                TOKEN_CREATE_OPS_NOT_ALLOWED );
        assertSuccess( schemaSubject, "MATCH (n:MyNode) SET n.nonExistent = 'foo' RETURN toString(count(*)) AS c",
                r -> assertKeyIs( r, "c", "1" ) );
        assertSuccess( readSubject, "MATCH (n:MyNode) WHERE n.nonExistent = 'foo' RETURN toString(count(*)) AS c",
                r -> assertKeyIs( r, "c", "1" ) );
    }

    private class SecurityLog
    {
        List<String> lines;

        void load() throws IOException
        {
            File securityLog = new File( AuthScenariosInteractionTestBase.this.securityLog.getAbsolutePath() );
            try ( FileSystemAbstraction fileSystem = neo.fileSystem();
                  BufferedReader bufferedReader = new BufferedReader(
                            fileSystem.openAsReader( securityLog, Charsets.UTF_8 ) ) )
            {
                lines = bufferedReader.lines().collect( java.util.stream.Collectors.toList() );
            }
        }

        void assertHasLine( String subject, String msg )
        {
            Objects.requireNonNull( lines );
            assertThat( lines, hasItem( containsString( "[" + subject + "]: " + msg ) ) );
        }
    }
}
