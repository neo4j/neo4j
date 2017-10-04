/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.helpers.BaseToObjectValueWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionGuardException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.bolt.ManagedBoltStateMachine;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.builtinprocs.EnterpriseBuiltInDbmsProcedures;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.procedure.UserFunction;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOut;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

public abstract class ProcedureInteractionTestBase<S>
{
    static final String PROCEDURE_TIMEOUT_ERROR = "Procedure got: Transaction guard check failed";
    protected boolean PWD_CHANGE_CHECK_FIRST;
    protected String CHANGE_PWD_ERR_MSG = AuthorizationViolationException.PERMISSION_DENIED;
    private static final String BOLT_PWD_ERR_MSG =
            "The credentials you provided were valid, but must be changed before you can use this instance.";
    String READ_OPS_NOT_ALLOWED = "Read operations are not allowed";
    String WRITE_OPS_NOT_ALLOWED = "Write operations are not allowed";
    String TOKEN_CREATE_OPS_NOT_ALLOWED = "Token create operations are not allowed";
    String SCHEMA_OPS_NOT_ALLOWED = "Schema operations are not allowed";

    protected boolean IS_EMBEDDED = true;
    boolean IS_BOLT;

    String pwdReqErrMsg( String errMsg )
    {
        return PWD_CHANGE_CHECK_FIRST ? CHANGE_PWD_ERR_MSG : IS_EMBEDDED ? errMsg : BOLT_PWD_ERR_MSG;
    }

    private final String EMPTY_ROLE = "empty";

    S adminSubject;
    S schemaSubject;
    S writeSubject;
    S editorSubject;
    S readSubject;
    S pwdSubject;
    S noneSubject;

    String[] initialUsers = {"adminSubject", "readSubject", "schemaSubject",
            "writeSubject", "editorSubject", "pwdSubject", "noneSubject", "neo4j"};
    String[] initialRoles = {ADMIN, ARCHITECT, PUBLISHER, EDITOR, READER, EMPTY_ROLE};

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    private ThreadingRule threading()
    {
        return threading;
    }

    EnterpriseUserManager userManager;

    protected NeoInteractionLevel<S> neo;
    File securityLog;

    Map<String,String> defaultConfiguration() throws IOException
    {
        Path homeDir = Files.createTempDirectory( "logs" );
        securityLog = new File( homeDir.toFile(), "security.log" );
        return stringMap( GraphDatabaseSettings.logs_directory.name(), homeDir.toAbsolutePath().toString(),
                SecuritySettings.procedure_roles.name(),
                "test.allowed*Procedure:role1;test.nestedAllowedFunction:role1;" +
                "test.allowedFunc*:role1;test.*estedAllowedProcedure:role1" );
    }

    @Before
    public void setUp() throws Throwable
    {
        configuredSetup( defaultConfiguration() );
    }

    void configuredSetup( Map<String,String> config ) throws Throwable
    {
        neo = setUpNeoServer( config );
        Procedures procedures = neo.getLocalGraph().getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( ClassWithProcedures.class );
        procedures.registerFunction( ClassWithFunctions.class );
        userManager = neo.getLocalUserManager();
        userManager.newUser( "noneSubject", "abc", false );
        userManager.newUser( "pwdSubject", "abc", true );
        userManager.newUser( "adminSubject", "abc", false );
        userManager.newUser( "schemaSubject", "abc", false );
        userManager.newUser( "writeSubject", "abc", false );
        userManager.newUser( "editorSubject", "abc", false );
        userManager.newUser( "readSubject", "123", false );
        // Currently admin role is created by default
        userManager.addRoleToUser( ADMIN, "adminSubject" );
        userManager.addRoleToUser( ARCHITECT, "schemaSubject" );
        userManager.addRoleToUser( PUBLISHER, "writeSubject" );
        userManager.addRoleToUser( EDITOR, "editorSubject" );
        userManager.addRoleToUser( READER, "readSubject" );
        userManager.newRole( EMPTY_ROLE );
        noneSubject = neo.login( "noneSubject", "abc" );
        pwdSubject = neo.login( "pwdSubject", "abc" );
        readSubject = neo.login( "readSubject", "123" );
        editorSubject = neo.login( "editorSubject", "abc" );
        writeSubject = neo.login( "writeSubject", "abc" );
        schemaSubject = neo.login( "schemaSubject", "abc" );
        adminSubject = neo.login( "adminSubject", "abc" );
        assertEmpty( schemaSubject, "CREATE (n) SET n:A:Test:NEWNODE:VeryUniqueLabel:Node " +
                                    "SET n.id = '2', n.square = '4', n.name = 'me', n.prop = 'a', n.number = '1' " +
                                    "DELETE n" );
        assertEmpty( writeSubject, "UNWIND range(0,2) AS number CREATE (:Node {number:number, name:'node'+number})" );
    }

    protected abstract NeoInteractionLevel<S> setUpNeoServer( Map<String,String> config ) throws Throwable;

    @After
    public void tearDown() throws Throwable
    {
        if ( neo != null )
        {
            neo.tearDown();
        }
    }

    protected String[] with( String[] strs, String... moreStr )
    {
        return Stream.concat( Arrays.stream( strs ), Arrays.stream( moreStr ) ).toArray( String[]::new );
    }

    List<String> listOf( String... values )
    {
        return Stream.of( values ).collect( toList() );
    }

    //------------- Helper functions---------------

    void testSuccessfulRead( S subject, Object count )
    {
        assertSuccess( subject, "MATCH (n) RETURN count(n) as count", r ->
        {
            List<Object> result = r.stream().map( s -> s.get( "count" ) ).collect( toList() );
            assertThat( result.size(), equalTo( 1 ) );
            assertThat( result.get( 0 ), equalTo( valueOf( count ) ) );
        } );
    }

    void testFailRead( S subject, int count )
    {
        testFailRead( subject, count, READ_OPS_NOT_ALLOWED );
    }

    void testFailRead( S subject, int count, String errMsg )
    {
        assertFail( subject, "MATCH (n) RETURN count(n)", errMsg );
    }

    void testSuccessfulWrite( S subject )
    {
        assertEmpty( subject, "CREATE (:Node)" );
    }

    void testFailWrite( S subject )
    {
        testFailWrite( subject, WRITE_OPS_NOT_ALLOWED );
    }

    void testFailWrite( S subject, String errMsg )
    {
        assertFail( subject, "CREATE (:Node)", errMsg );
    }

    void testSuccessfulTokenWrite( S subject )
    {
        assertEmpty( subject, "CALL db.createLabel('NewNodeName')" );
    }

    void testFailTokenWrite( S subject )
    {
        testFailTokenWrite( subject, TOKEN_CREATE_OPS_NOT_ALLOWED );
    }

    void testFailTokenWrite( S subject, String errMsg )
    {
        assertFail( subject, "CALL db.createLabel('NewNodeName')", errMsg );
    }

    void testSuccessfulSchema( S subject )
    {
        assertEmpty( subject, "CREATE INDEX ON :Node(number)" );
    }

    void testFailSchema( S subject )
    {
        testFailSchema( subject, SCHEMA_OPS_NOT_ALLOWED );
    }

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

    void testSuccessfulListUsers( S subject, Object[] users )
    {
        assertSuccess( subject, "CALL dbms.security.listUsers() YIELD username",
                r -> assertKeyIsArray( r, "username", users ) );
    }

    void testFailListUsers( S subject, int count, String errMsg )
    {
        assertFail( subject, "CALL dbms.security.listUsers() YIELD username", errMsg );
    }

    void testSuccessfulListRoles( S subject, Object[] roles )
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
        assertFail( subject, "CALL test.allowedReadProcedure()", READ_OPS_NOT_ALLOWED );
        assertFail( subject, "CALL test.allowedWriteProcedure()", WRITE_OPS_NOT_ALLOWED );
        assertFail( subject, "CALL test.allowedSchemaProcedure()", SCHEMA_OPS_NOT_ALLOWED );
    }

    void testSuccessfulTestProcs( S subject )
    {
        assertSuccess( subject, "CALL test.allowedReadProcedure()",
                r -> assertKeyIs( r, "value", "foo" ) );
        assertSuccess( subject, "CALL test.allowedWriteProcedure()",
                r -> assertKeyIs( r, "value", "a", "a" ) );
        assertSuccess( subject, "CALL test.allowedSchemaProcedure()",
                r -> assertKeyIs( r, "value", "OK" ) );
    }

    void assertPasswordChangeWhenPasswordChangeRequired( S subject, String newPassword )
    {
        StringBuilder builder = new StringBuilder( 128 );
        S subjectToUse;

        // remove if-else ASAP
        if ( IS_EMBEDDED )
        {
            subjectToUse = subject;
            builder.append( "CALL dbms.security.changePassword('" );
            builder.append( newPassword );
            builder.append( "')" );
        }
        else
        {
            subjectToUse = adminSubject;
            builder.append( "CALL dbms.security.changeUserPassword('" );
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
        if ( StringUtils.isEmpty( partOfErrorMsg ) )
        {
            assertThat( err, not( equalTo( "" ) ) );
        }
        else
        {
            assertThat( err, containsString( partOfErrorMsg ) );
        }
    }

    void assertEmpty( S subject, String call )
    {
        String err = assertCallEmpty( subject, call );
        assertThat( err, equalTo( "" ) );
    }

    void assertSuccess( S subject, String call, Consumer<ResourceIterator<Map<String,Object>>> resultConsumer )
    {
        String err = neo.executeQuery( subject, call, null, resultConsumer );
        assertThat( err, equalTo( "" ) );
    }

    List<Map<String,Object>> collectSuccessResult( S subject, String call )
    {
        List<Map<String,Object>> result = new LinkedList<>();
        assertSuccess( subject, call, r -> r.stream().forEach( result::add ) );
        return result;
    }

    private String assertCallEmpty( S subject, String call )
    {
        return neo.executeQuery( subject, call, null,
                result ->
                {
                    List<Map<String,Object>> collect = result.stream().collect( toList() );
                    assertTrue( "Expected no results but got: " + collect, collect.isEmpty() );
                } );
    }

    private void executeQuery( S subject, String call )
    {
        neo.executeQuery( subject, call, null, r ->
        {
        } );
    }

    boolean userHasRole( String user, String role ) throws InvalidArgumentsException
    {
        return userManager.getRoleNamesForUser( user ).contains( role );
    }

    List<Object> getObjectsAsList( ResourceIterator<Map<String,Object>> r, String key )
    {
        return r.stream().map( s -> s.get( key ) ).collect( toList() );
    }

    void assertKeyIs( ResourceIterator<Map<String,Object>> r, String key, Object... items )
    {
        assertKeyIsArray( r, key, items );
    }

    private void assertKeyIsArray( ResourceIterator<Map<String,Object>> r, String key, Object[] items )
    {
        List<Object> results = getObjectsAsList( r, key );
        assertEquals( Arrays.asList( items ).size(), results.size() );
        Assert.assertThat( results, containsInAnyOrder( Arrays.stream( items ).map( this::valueOf ).toArray()
        ) );
    }

    static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            Object expected )
    {
        if ( expected instanceof MapValue )
        {
            assertKeyIsMap( r, keyKey, valueKey, (MapValue) expected );
        }
        else
        {
            assertKeyIsMap( r, keyKey, valueKey, (Map<String,Object>) expected );
        }
    }

    @SuppressWarnings( "unchecked" )
    static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            Map<String,Object> expected )
    {
        List<Map<String,Object>> result = r.stream().collect( toList() );

        assertEquals( "Results for should have size " + expected.size() + " but was " + result.size(),
                expected.size(), result.size() );

        for ( Map<String,Object> row : result )
        {
            String key = (String) row.get( keyKey );
            assertThat( expected, hasKey( key ) );
            assertThat( row, hasKey( valueKey ) );

            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof List )
            {
                List<String> value = (List<String>) objectValue;
                List<String> expectedValues = (List<String>) expected.get( key );
                assertEquals( "sizes", value.size(), expectedValues.size() );
                assertThat( value, containsInAnyOrder( expectedValues.toArray() ) );
            }
            else
            {
                String value = objectValue.toString();
                String expectedValue = expected.get( key ).toString();
                assertThat( value, equalTo( expectedValue ) );
            }
        }
    }

    static void assertKeyIsMap( ResourceIterator<Map<String,Object>> r, String keyKey, String valueKey,
            MapValue expected )
    {
        List<Map<String,Object>> result = r.stream().collect( toList() );

        assertEquals( "Results for should have size " + expected.size() + " but was " + result.size(),
                expected.size(), result.size() );

        for ( Map<String,Object> row : result )
        {
            TextValue key = (TextValue) row.get( keyKey );
            assertTrue( expected.containsKey( key.stringValue() ) );
            assertThat( row, hasKey( valueKey ) );

            Object objectValue = row.get( valueKey );
            if ( objectValue instanceof ListValue )
            {
                ListValue value = (ListValue) objectValue;
                ListValue expectedValues = (ListValue) expected.get( key.stringValue() );
                assertEquals( "sizes", value.size(), expectedValues.size() );
                assertThat( Arrays.asList( value.asArray() ), containsInAnyOrder( expectedValues.asArray() ) );
            }
            else
            {
                String value = ((TextValue) objectValue).stringValue();
                String expectedValue = ((TextValue) expected.get( key.stringValue() )).stringValue();
                assertThat( value, equalTo( expectedValue ) );
            }
        }
    }

    // --------------------- helpers -----------------------
    void shouldTerminateTransactionsForUser( S subject, String procedure ) throws Throwable
    {
        DoubleLatch latch = new DoubleLatch( 2 );
        ThreadedTransaction<S> userThread = new ThreadedTransaction<>( neo, latch );
        userThread.executeCreateNode( threading(), subject );
        latch.startAndWaitForAllToStart();

        assertEmpty( adminSubject, "CALL " + format( procedure, neo.nameOf( subject ) ) );

        Map<String,Long> transactionsByUser = countTransactionsByUsername();

        assertThat( transactionsByUser.get( neo.nameOf( subject ) ), equalTo( null ) );

        latch.finishAndWaitForAllToFinish();

        userThread.closeAndAssertExplicitTermination();

        assertEmpty( adminSubject, "MATCH (n:Test) RETURN n.name AS name" );
    }

    private Map<String,Long> countTransactionsByUsername()
    {
        return EnterpriseBuiltInDbmsProcedures.countTransactionByUsername(
                EnterpriseBuiltInDbmsProcedures.getActiveTransactions(
                        neo.getLocalGraph().getDependencyResolver()
                ).stream()
                        .filter( tx -> !tx.terminationReason().isPresent() )
                        .map( tx -> tx.securityContext().subject().username() )
        ).collect( Collectors.toMap( r -> r.username, r -> r.activeTransactions ) );
    }

    protected Object toRawValue( Object value )
    {
        if ( value instanceof AnyValue )
        {
            BaseToObjectValueWriter<RuntimeException> writer = writer();
            ((AnyValue) value).writeTo( writer );
            return writer.value();
        }
        else
        {
            return value;
        }
    }

    Map<String,Long> countBoltConnectionsByUsername()
    {
        BoltConnectionTracker boltConnectionTracker = EnterpriseBuiltInDbmsProcedures.getBoltConnectionTracker(
                neo.getLocalGraph().getDependencyResolver() );
        return EnterpriseBuiltInDbmsProcedures.countConnectionsByUsername(
                boltConnectionTracker
                        .getActiveConnections()
                        .stream()
                        .filter( session -> !session.willTerminate() )
                        .map( ManagedBoltStateMachine::owner )
        ).collect( Collectors.toMap( r -> r.username, r -> r.connectionCount ) );
    }

    @SuppressWarnings( "unchecked" )
    TransportConnection startBoltSession( String username, String password ) throws Exception
    {
        TransportConnection connection = new SocketConnection();
        HostnamePort address = neo.lookupConnector( DEFAULT_CONNECTOR_KEY );
        Map<String,Object> authToken = map( "principal", username, "credentials", password, "scheme", "basic" );

        connection.connect( address ).send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk( init( "TestClient/1.1", authToken ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, eventuallyReceives( msgSuccess() ) );
        return connection;
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class CountResult
    {
        public final String count;

        CountResult( Long count )
        {
            this.count = "" + count;
        }
    }

    @SuppressWarnings( {"unused", "WeakerAccess"} )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        private static final AtomicReference<LatchedRunnables> testLatch = new AtomicReference<>();

        static DoubleLatch doubleLatch;

        public static volatile DoubleLatch volatileLatch;

        public static List<Exception> exceptionsInProcedure = Collections.synchronizedList( new ArrayList<>() );

        @Context
        public TerminationGuard guard;

        @Procedure( name = "test.loop" )
        public void loop()
        {
            DoubleLatch latch = volatileLatch;

            if ( latch != null )
            {
                latch.startAndWaitForAllToStart();
            }
            try
            {
                //noinspection InfiniteLoopStatement
                while ( true )
                {
                    try
                    {
                        Thread.sleep( 250 );
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.interrupted();
                    }
                    guard.check();
                }
            }
            catch ( TransactionTerminatedException | TransactionGuardException e )
            {
                if ( e.status().equals( TransactionTimedOut ) )
                {
                    throw new TransactionGuardException( TransactionTimedOut, PROCEDURE_TIMEOUT_ERROR, e );
                }
                else
                {
                    throw e;
                }
            }
            finally
            {
                if ( latch != null )
                {
                    latch.finish();
                }
            }
        }

        @Procedure( name = "test.neverEnding" )
        public void neverEndingWithLock()
        {
            doubleLatch.start();
            doubleLatch.finishAndWaitForAllToFinish();
        }

        @Procedure( name = "test.numNodes" )
        public Stream<CountResult> numNodes()
        {
            Long nNodes = db.getAllNodes().stream().count();
            return Stream.of( new CountResult( nNodes ) );
        }

        @Procedure( name = "test.staticReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> staticReadProcedure()
        {
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.staticWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> staticWriteProcedure()
        {
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.staticSchemaProcedure", mode = Mode.SCHEMA )
        public Stream<AuthProceduresBase.StringResult> staticSchemaProcedure()
        {
            return Stream.of( new AuthProceduresBase.StringResult( "static" ) );
        }

        @Procedure( name = "test.allowedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure1()
        {
            Result result = db.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "foo" ).toString() ) );
        }

        @Procedure( name = "test.otherAllowedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> otherAllowedProcedure()
        {
            Result result = db.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "foo" ).toString() ) );
        }

        @Procedure( name = "test.allowedWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure2()
        {
            db.execute( "UNWIND [1, 2] AS i CREATE (:VeryUniqueLabel {prop: 'a'})" );
            Result result = db.execute( "MATCH (n:VeryUniqueLabel) RETURN n.prop AS a LIMIT 2" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "a" ).toString() ) );
        }

        @Procedure( name = "test.allowedSchemaProcedure", mode = Mode.SCHEMA )
        public Stream<AuthProceduresBase.StringResult> allowedProcedure3()
        {
            db.execute( "CREATE INDEX ON :VeryUniqueLabel(prop)" );
            return Stream.of( new AuthProceduresBase.StringResult( "OK" ) );
        }

        @Procedure( name = "test.nestedAllowedProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> nestedAllowedProcedure(
                @Name( "nestedProcedure" ) String nestedProcedure
        )
        {
            Result result = db.execute( "CALL " + nestedProcedure );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.doubleNestedAllowedProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> doubleNestedAllowedProcedure()
        {
            Result result = db.execute( "CALL test.nestedAllowedProcedure('test.allowedReadProcedure') YIELD value" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.failingNestedAllowedWriteProcedure", mode = Mode.WRITE )
        public Stream<AuthProceduresBase.StringResult> failingNestedAllowedWriteProcedure()
        {
            Result result = db.execute( "CALL test.nestedReadProcedure('test.allowedWriteProcedure') YIELD value" );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
        }

        @Procedure( name = "test.nestedReadProcedure", mode = Mode.READ )
        public Stream<AuthProceduresBase.StringResult> nestedReadProcedure(
                @Name( "nestedProcedure" ) String nestedProcedure
        )
        {
            Result result = db.execute( "CALL " + nestedProcedure );
            return result.stream().map( r -> new AuthProceduresBase.StringResult( r.get( "value" ).toString() ) );
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

        @Procedure( name = "test.threadTransaction", mode = WRITE )
        public void newThreadTransaction()
        {
            startWriteThread();
        }

        @Procedure( name = "test.threadReadDoingWriteTransaction" )
        public void threadReadDoingWriteTransaction()
        {
            startWriteThread();
        }

        private void startWriteThread()
        {
            new Thread( () ->
            {
                doubleLatch.start();
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode( Label.label( "VeryUniqueLabel" ) );
                    tx.success();
                }
                catch ( Exception e )
                {
                    exceptionsInProcedure.add( e );
                }
                finally
                {
                    doubleLatch.finish();
                }
            } ).start();
        }

        protected static class LatchedRunnables implements AutoCloseable
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

    @SuppressWarnings( "unused" )
    public static class ClassWithFunctions
    {
        @Context
        public GraphDatabaseService db;

        @UserFunction( name = "test.nonAllowedFunc" )
        public String nonAllowedFunc()
        {
            return "success";
        }

        @UserFunction( name = "test.allowedFunc" )
        public String allowedFunc()
        {
            return "success for role1";
        }

        @UserFunction( name = "test.allowedFunction1" )
        public String allowedFunction1()
        {
            Result result = db.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.next().get( "foo" ).toString();
        }

        @UserFunction( name = "test.allowedFunction2" )
        public String allowedFunction2()
        {
            Result result = db.execute( "MATCH (:Foo) WITH count(*) AS c RETURN 'foo' AS foo" );
            return result.next().get( "foo" ).toString();
        }

        @UserFunction( name = "test.nestedAllowedFunction" )
        public String nestedAllowedFunction(
                @Name( "nestedFunction" ) String nestedFunction
        )
        {
            Result result = db.execute( "RETURN " + nestedFunction + " AS value" );
            return result.next().get( "value" ).toString();
        }
    }

    protected abstract Object valueOf( Object obj );

    private BaseToObjectValueWriter<RuntimeException> writer()
    {
        return new BaseToObjectValueWriter<RuntimeException>()
        {
            @Override
            protected Node newNodeProxyById( long id )
            {
                return null;
            }

            @Override
            protected Relationship newRelationshipProxyById( long id )
            {
                return null;
            }

            @Override
            protected Point newGeographicPoint( double longitude, double latitude, String name, int code, String href )
            {
                return null;
            }

            @Override
            protected Point newCartesianPoint( double x, double y, String name, int code, String href )
            {
                return null;
            }
        };
    }
}
