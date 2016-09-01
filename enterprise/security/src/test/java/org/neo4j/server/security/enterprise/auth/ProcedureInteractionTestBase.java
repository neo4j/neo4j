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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.procedure.Procedure.Mode.READ;
import static org.neo4j.procedure.Procedure.Mode.WRITE;
import static org.neo4j.server.security.auth.AuthProceduresIT.assertKeyIsMap;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ADMIN;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.PredefinedRolesBuilder.READER;

abstract class ProcedureInteractionTestBase<S>
{
    protected boolean PWD_CHANGE_CHECK_FIRST = false;
    protected String CHANGE_PWD_ERR_MSG = AuthProcedures.PERMISSION_DENIED;
    private String BOLT_PWD_ERR_MSG =
            "The credentials you provided were valid, but must be changed before you can use this instance.";
    String READ_OPS_NOT_ALLOWED = "Read operations are not allowed";
    String WRITE_OPS_NOT_ALLOWED = "Write operations are not allowed";
    String SCHEMA_OPS_NOT_ALLOWED = "Schema operations are not allowed";

    protected boolean IS_EMBEDDED = true;
    boolean IS_BOLT = false;

    String pwdReqErrMsg( String errMsg )
    {
        return PWD_CHANGE_CHECK_FIRST ? CHANGE_PWD_ERR_MSG : IS_EMBEDDED ? errMsg : BOLT_PWD_ERR_MSG;
    }

    private final String EMPTY_ROLE = "empty";

    S adminSubject;
    S schemaSubject;
    S writeSubject;
    S readSubject;
    S pwdSubject;
    S noneSubject;

    String[] initialUsers = { "adminSubject", "readSubject", "schemaSubject",
        "writeSubject", "pwdSubject", "noneSubject", "neo4j" };
    String[] initialRoles = { ADMIN, ARCHITECT, PUBLISHER, READER, EMPTY_ROLE };

    protected abstract ThreadingRule threading();

    EnterpriseUserManager userManager;

    protected NeoInteractionLevel<S> neo;

    @Before
    public void setUp() throws Throwable
    {
        Neo4jWithSocket.cleanupTemporaryTestFiles();
        neo = setUpNeoServer();
        neo.getGraph().getDependencyResolver().resolveDependency( Procedures.class )
                .register( ClassWithProcedures.class );
        userManager = neo.getManager();

        userManager.newUser( "noneSubject", "abc", false );
        userManager.newUser( "pwdSubject", "abc", true );
        userManager.newUser( "adminSubject", "abc", false );
        userManager.newUser( "schemaSubject", "abc", false );
        userManager.newUser( "writeSubject", "abc", false );
        userManager.newUser( "readSubject", "123", false );
        // Currently admin role is created by default
        userManager.addRoleToUser( ADMIN, "adminSubject" );
        userManager.addRoleToUser( ARCHITECT, "schemaSubject" );
        userManager.addRoleToUser( PUBLISHER, "writeSubject" );
        userManager.addRoleToUser( READER, "readSubject" );
        userManager.newRole( EMPTY_ROLE );
        noneSubject = neo.login( "noneSubject", "abc" );
        pwdSubject = neo.login( "pwdSubject", "abc" );
        readSubject = neo.login( "readSubject", "123" );
        writeSubject = neo.login( "writeSubject", "abc" );
        schemaSubject = neo.login( "schemaSubject", "abc" );
        adminSubject = neo.login( "adminSubject", "abc" );
        executeQuery( writeSubject, "UNWIND range(0,2) AS number CREATE (:Node {number:number, name:'node'+number})" );
    }

    protected abstract NeoInteractionLevel<S> setUpNeoServer() throws Throwable;

    @After
    public void tearDown() throws Throwable
    {
        neo.tearDown();
        Neo4jWithSocket.cleanupTemporaryTestFiles();
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream(strs), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    List<String> listOf( String... values )
    {
        return Stream.of( values ).collect( toList() );
    }

    //------------- Helper functions---------------

    void testSuccessfulRead( S subject, int count )
    {
        assertSuccess( subject, "MATCH (n) RETURN count(n) as count", r -> {
            List<Object> result = r.stream().map( s -> s.get( "count" ) ).collect( toList() );
            assertThat( result.size(), equalTo( 1 ) );
            assertThat( String.valueOf( result.get( 0 ) ), equalTo( String.valueOf( count ) ) );
        } );
    }

    void testFailRead( S subject, int count ) { testFailRead( subject, count, READ_OPS_NOT_ALLOWED ); }
    void testFailRead( S subject, int count, String errMsg )
    {
        assertFail( subject, "MATCH (n) RETURN count(n)", errMsg );
    }

    void testSuccessfulWrite( S subject )
    {
        assertEmpty( subject, "CREATE (:Node)" );
    }

    void testFailWrite( S subject ) { testFailWrite( subject, WRITE_OPS_NOT_ALLOWED ); }
    void testFailWrite( S subject, String errMsg )
    {
        assertFail( subject, "CREATE (:Node)", errMsg );
    }

    void testSuccessfulSchema( S subject )
    {
        assertEmpty( subject, "CREATE INDEX ON :Node(number)" );
    }

    void testFailSchema( S subject ) { testFailSchema( subject, SCHEMA_OPS_NOT_ALLOWED ); }
    void testFailSchema( S subject, String errMsg )
    {
        assertFail( subject, "CREATE INDEX ON :Node(number)", errMsg );
    }

    void testFailCreateUser( S subject, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.createUser('Craig', 'foo', false)", errMsg );
        assertFail( subject, "CALL dbms.security.createUser('Craig', '', false)", errMsg );
        assertFail( subject, "CALL dbms.security.createUser('', 'foo', false)", errMsg );
    }

    void testFailCreateRole( S subject, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
        assertFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
        assertFail( subject, "CALL dbms.security.createRole('RealAdmins')", errMsg );
    }

    void testFailAddRoleToUser( S subject, String role, String username, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.addRoleToUser('" + role + "', '" + username + "')", errMsg );
    }

    void testFailRemoveRoleFromUser( S subject, String role, String username, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.removeRoleFromUser('" + role + "', '" + username + "')", errMsg );
    }

    void testFailDeleteUser( S subject, String username, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.deleteUser('" + username + "')", errMsg );
    }

    void testFailDeleteRole( S subject, String roleName, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.deleteRole('" + roleName + "')", errMsg );
    }

    void testSuccessfulListUsers( S subject, String[] users )
    {
        assertSuccess( subject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIsArray( r, "username", users ) );
    }

    void testFailListUsers( S subject, int count, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.listUsers() YIELD username", errMsg );
    }

    void testSuccessfulListRoles( S subject, String[] roles )
    {
        assertSuccess( subject, "CALL dbms.security.listRoles() YIELD role",
                r -> assertKeyIsArray( r, "role", roles ) );
    }

    void testFailListRoles( S subject, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.listRoles() YIELD role", errMsg );
    }

    void testFailListUserRoles( S subject, String username, String errMsg )
    {
        assertFail( subject,
                "CALL dbms.security.listRolesForUser('" + username + "') YIELD value AS roles RETURN count(roles)",
                errMsg );
    }

    void testFailListRoleUsers( S subject, String roleName, String errMsg )
    {
        assertFail( subject,
                "CALL dbms.security.listUsersForRole('" + roleName + "') YIELD value AS users RETURN count(users)",
                errMsg );
    }

    void testFailTestProcs( S subject )
    {
        assertFail( subject, "CALL test.allowedProcedure1()", READ_OPS_NOT_ALLOWED );
        assertFail( subject, "CALL test.allowedProcedure2()", WRITE_OPS_NOT_ALLOWED );
        assertFail( subject, "CALL test.allowedProcedure3()", SCHEMA_OPS_NOT_ALLOWED );
    }

    void testSuccessfulTestProcs( S subject )
    {
        assertSuccess( subject, "CALL test.allowedProcedure1()",
                r -> assertKeyIs( r, "value", "foo" ) );
        assertSuccess( subject, "CALL test.allowedProcedure2()",
                r -> assertKeyIs( r, "value", "a" ) );
        assertSuccess( subject, "CALL test.allowedProcedure3()",
                r -> assertKeyIs( r, "value", "OK" ) );
    }

    void testSessionKilled( S subject )
    {
        if ( IS_BOLT )
        {
            // After the connection has been terminated, attempts to receive
            // data will result in an IOException with the message below...
            String unixErrorMessage = "Failed to read 2 bytes, missing 2 bytes. Buffer: 00 00";
            // ...but only on Unix. If we're on Windows, we get...
            String windowsErrorMessage = "Software caused connection abort: recv failed";
            // TODO: This whole method is probably ripe for a bit of refactoring.
            assertFail( subject, "MATCH (n:Node) RETURN count(n)", unixErrorMessage, windowsErrorMessage );
        }
        else if ( IS_EMBEDDED )
        {
            assertFail( subject, "MATCH (n:Node) RETURN count(n)", "Read operations are not allowed" );
        }
        else
        {
            assertFail( subject, "MATCH (n:Node) RETURN count(n)", "Invalid username or password" );
        }
    }

    void assertPasswordChangeWhenPasswordChangeRequired( S subject, String newPassword )
    {
        StringBuilder builder = new StringBuilder(128);
        S subjectToUse;

        // remove if-else ASAP
        if ( IS_EMBEDDED ) {
            subjectToUse = subject;
            builder.append("CALL dbms.security.changePassword('");
            builder.append( newPassword );
            builder.append("')" );
        } else {
            subjectToUse = adminSubject;
            builder.append("CALL dbms.security.changeUserPassword('");
            builder.append( neo.nameOf( subject ) );
            builder.append( "', '" );
            builder.append( newPassword );
            builder.append( "', false)" );
        }

        assertEmpty( subjectToUse, builder.toString() );
    }

    void assertFail( S subject, String call, String partOfErrorMsg )
    {
        String err = assertCallEmpty( subject, call );
        assertThat( err, not( equalTo( "" ) ) );
        assertThat( err, containsString( partOfErrorMsg ) );
    }

    private void assertFail( S subject, String call, String partOfErrorMsg1, String partOfErrorMsg2 )
    {
        String err = assertCallEmpty( subject, call );
        assertThat( err, not( equalTo( "" ) ) );
        assertThat( err, either( containsString( partOfErrorMsg1 ) ).or( containsString( partOfErrorMsg2 ) ) );
    }

    void assertEmpty( S subject, String call )
    {
        String err = assertCallEmpty( subject, call );
        assertThat( err, equalTo( "" ) );
    }

    void assertSuccess( S subject, String call, Consumer<ResourceIterator<Map<String, Object>>> resultConsumer )
    {
        String err = neo.executeQuery( subject, call, null, resultConsumer );
        assertThat( err, equalTo( "" ) );
    }

    private String assertCallEmpty( S subject, String call )
    {
        return neo.executeQuery( subject, call, null,
                ( res ) -> assertFalse( "Expected no results", res.hasNext()
            ) );
    }

    private void executeQuery( S subject, String call )
    {
        neo.executeQuery( subject, call, null, r -> {} );
    }

    boolean userHasRole( String user, String role ) throws InvalidArgumentsException
    {
        return userManager.getRoleNamesForUser( user ).contains( role );
    }

    List<Object> getObjectsAsList( ResourceIterator<Map<String, Object>> r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( toList() );
    }

    void assertKeyIs( ResourceIterator<Map<String, Object>> r, String key, String... items )
    {
        assertKeyIsArray( r, key, items );
    }

    private void assertKeyIsArray( ResourceIterator<Map<String,Object>> r, String key, String[] items )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertEquals( Arrays.asList( items ).size(), results.size() );
        Assert.assertThat( results, containsInAnyOrder( items ) );
    }

    // --------------------- helpers -----------------------

    void shouldTerminateTransactionsForUser( S subject, String procedure ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransactionCreate<S> userThread = new ThreadedTransactionCreate<>( neo, latch );
        userThread.execute( threading(), subject );
        latch.startAndWaitForAllToStart();

        assertEmpty( adminSubject, "CALL " + format(procedure, neo.nameOf( subject ) ) );

        assertSuccess( adminSubject, "CALL dbms.security.listTransactions()",
                r -> assertKeyIsMap( r, "username", "activeTransactions", map( "adminSubject", "1" ) ) );

        latch.finishAndWaitForAllToFinish();

        userThread.closeAndAssertTransactionTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    @SuppressWarnings( "unchecked" )
    TransportConnection startBoltSession( String username, String password ) throws Exception
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

    private static class CountResult
    {
        public final String count;

        CountResult( Long count )
        {
            this.count = ""+count;
        }
    }

    static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        private static final AtomicReference<LatchedRunnables> testLatch = new AtomicReference<>();

        @Procedure( name = "test.numNodes" )
        public Stream<CountResult> numNodes()
        {
            Long nNodes = db.getAllNodes().stream().count();
            return Stream.of( new CountResult( nNodes ) );
        }

        @Procedure( name = "test.allowedProcedure1", allowed = {"role1"}, mode = Procedure.Mode.READ )
        public Stream<AuthProcedures.StringResult> allowedProcedure1()
        {
            db.execute( "MATCH (:Foo) RETURN 'foo' AS foo" );
            return Stream.of( new AuthProcedures.StringResult( "foo" ) );
        }

        @Procedure( name = "test.allowedProcedure2", allowed = {"otherRole", "role1"}, mode = Procedure.Mode.WRITE )
        public Stream<AuthProcedures.StringResult> allowedProcedure2()
        {
            db.execute( "CREATE (:VeryUniqueLabel {prop: 'a'})" );
            return db.execute( "MATCH (n:VeryUniqueLabel) RETURN n.prop AS a LIMIT 1" ).stream()
                    .map( r -> new AuthProcedures.StringResult( (String) r.get( "a" ) ) );
        }

        @Procedure( name = "test.allowedProcedure3", allowed = {"role1"}, mode = Procedure.Mode.SCHEMA )
        public Stream<AuthProcedures.StringResult> allowedProcedure3()
        {
            db.execute( "CREATE INDEX ON :VeryUniqueLabel(prop)" );
            return Stream.of( new AuthProcedures.StringResult( "OK" ) );
        }

        @Procedure( name = "test.createNode", mode = WRITE )
        public void createNode()
        {
            db.createNode();
        }

        @Procedure( name = "test.waitForLatch", mode = READ )
        public void waitForLatch()
        {
            try
            {
                testLatch.get().runBefore.run();
            }
            finally
            {
                testLatch.get().doubleLatch.startAndWaitForAllToStart();
            }
            try
            {
                testLatch.get().runAfter.run();
            }
            finally
            {
                testLatch.get().doubleLatch.finishAndWaitForAllToFinish();
            }
        }

        static class LatchedRunnables implements AutoCloseable
        {
            DoubleLatch doubleLatch;
            Runnable runBefore;
            Runnable runAfter;

            LatchedRunnables( DoubleLatch doubleLatch, Runnable runBefore, Runnable runAfter )
            {
                this.doubleLatch = doubleLatch;
                this.runBefore = runBefore;
                this.runAfter = runAfter;
            }

            @Override
            public void close() throws Exception
            {
                ClassWithProcedures.testLatch.set( null );
            }
        }

        static void setTestLatch( LatchedRunnables testLatch )
        {
            ClassWithProcedures.testLatch.set( testLatch );
        }
    }
}
