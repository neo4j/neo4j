/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime.integration;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.messaging.BoltResponseMessage;
import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.cypher.result.QueryResult.Record;
import org.neo4j.helpers.ValueUtils;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.verifyKillsConnection;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "unchecked" )
public class BoltConnectionIT
{
    private static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    private static final String USER_AGENT = "BoltConnectionIT/0.0";
    private static final BoltChannel boltChannel = mock( BoltChannel.class );
    @Rule
    public SessionRule env = new SessionRule();

    @Test
    public void shouldCloseConnectionAckFailureBeforeInit() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.ackFailure( recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldCloseConnectionResetBeforeInit() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.reset( recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldCloseConnectionOnRunBeforeInit() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.run( "RETURN 1", map(), recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldCloseConnectionOnDiscardAllBeforeInit() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.discardAll( recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldCloseConnectionOnPullAllBeforeInit() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );

        // when
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.pullAll( recorder ) );

        // then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldExecuteStatement() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "CREATE (n {k:'k'}) RETURN n.k", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );

        // When
        recorder.reset();
        machine.pullAll( recorder );

        // Then
        recorder.nextResponse().assertRecord( 0, stringValue( "k" ) );
        //assertThat( pulling.next(), streamContaining( StreamMatchers.eqRecord( equalTo( "k" ) ) ) );
    }

    @Test
    public void shouldSucceedOn__run__pullAll__run() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.pullAll( nullResponseHandler() );

        // When I run a new statement
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldSucceedOn__run__discardAll__run() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When I run a new statement
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "RETURN 1", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldSucceedOn__run_BEGIN__pullAll__run_COMMIT__pullALL__run_COMMIT() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "BEGIN", EMPTY_PARAMS, recorder );
        machine.pullAll( recorder );
        machine.run( "COMMIT", EMPTY_PARAMS, recorder );
        machine.pullAll( recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );

        // When I run a new statement
        recorder.reset();
        machine.run( "BEGIN", EMPTY_PARAMS, recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldFailOn__run__run() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran one statement
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );

        // When I run a new statement, before consuming the stream
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.run( "RETURN 1", EMPTY_PARAMS, recorder ) );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__pullAll__pullAll() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.pullAll( nullResponseHandler() );

        // Then further attempts to PULL should be treated as protocol violations
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.pullAll( recorder ) );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__pullAll__discardAll() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.pullAll( nullResponseHandler() );

        // When I attempt to pull more items from the stream
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.discardAll( recorder ) );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__discardAll__discardAll() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When I attempt to pull more items from the stream
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.discardAll( recorder ) );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__discardAll__pullAll() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And Given that I've ran and pulled one stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When I attempt to pull more items from the stream
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        verifyKillsConnection( () -> machine.pullAll( recorder ) );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Request.Invalid ) );
    }

    @Test
    public void shouldHandleImplicitCommitFailure() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        machine.run( "CREATE (n:Victim)-[:REL]->()", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When I perform an action that will fail on commit
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "MATCH (n:Victim) DELETE n", EMPTY_PARAMS, recorder );
        // Then the statement running should have succeeded
        assertThat( recorder.nextResponse(), succeeded() );

        recorder.reset();
        machine.discardAll( recorder );

        // But the stop should have failed, since it implicitly triggers commit and thus triggers a failure
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Schema.ConstraintValidationFailed ) );
        //assertThat( discarding.next(), failedWith( Status.Schema.ConstraintValidationFailed ) );
    }

    @Test
    public void shouldAllowUserControlledRollbackOnExplicitTxFailure() throws Throwable
    {
        // Given whenever en explicit transaction has a failure,
        // it is more natural for drivers to see the failure, acknowledge it
        // and send a `ROLLBACK`, because that means that all failures in the
        // transaction, be they client-local or inside neo, can be handled the
        // same way by a driver.
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        machine.run( "BEGIN", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );
        machine.run( "CREATE (n:Victim)-[:REL]->()", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );

        // When I perform an action that will fail
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( "this is not valid syntax", EMPTY_PARAMS, recorder );

        // Then I should see a failure
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );

        // And when I acknowledge that failure, and roll back the transaction
        recorder.reset();
        machine.ackFailure( recorder );
        machine.run( "ROLLBACK", EMPTY_PARAMS, recorder );

        // Then both operations should succeed
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    public void shouldHandleFailureDuringResultPublishing() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        final CountDownLatch pullAllCallbackCalled = new CountDownLatch( 1 );
        final AtomicReference<Neo4jError> error = new AtomicReference<>();

        // When something fails while publishing the result stream
        machine.run( "RETURN 1", EMPTY_PARAMS, nullResponseHandler() );
        machine.pullAll( new BoltResponseHandler()
        {
            @Override
            public void onStart()
            {
            }

            @Override
            public void onRecords( BoltResult result, boolean pull ) throws Exception
            {
                throw new RuntimeException( "Ooopsies!" );
            }

            @Override
            public void onMetadata( String key, AnyValue value )
            {
            }

            @Override
            public void markFailed( Neo4jError err )
            {
                error.set( err );
                pullAllCallbackCalled.countDown();
            }

            @Override
            public void markIgnored()
            {
            }

            @Override
            public void onFinish()
            {
            }
        } );

        // Then
        assertTrue( pullAllCallbackCalled.await( 30, TimeUnit.SECONDS ) );
        final Neo4jError err = error.get();
        assertThat( err.status(), equalTo( Status.General.UnknownError ) );
        assertThat( err.message(), CoreMatchers.containsString( "Ooopsies!" ) );
    }

    @Test
    public void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread() throws Throwable
    {
        // Given
        BoltStateMachine firstMachine = env.newMachine( boltChannel );
        firstMachine.init( USER_AGENT, emptyMap(), null );
        BoltStateMachine secondMachine = env.newMachine( boltChannel );
        secondMachine.init( USER_AGENT, emptyMap(), null );

        // And given I've started a transaction in one session
        runAndPull( firstMachine, "BEGIN" );

        // When I issue a statement in a separate session
        Object[] stream = runAndPull( secondMachine, "CREATE (a:Person) RETURN id(a)" );
        long id = ((LongValue) ((Record) stream[0]).fields()[0]).value();

        // And when I roll back that first session transaction
        runAndPull( firstMachine, "ROLLBACK" );

        // Then the two should not have interfered with each other
        stream = runAndPull( secondMachine, "MATCH (a:Person) WHERE id(a) = " + id + " RETURN COUNT(*)" );
        assertThat( ((Record) stream[0]).fields()[0], equalTo( longValue( 1L ) ) );
    }

    @Test
    public void shouldSupportUsingPeriodicCommitInSession() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        MapValue params = map( "csvFileUrl", createLocalIrisData( machine ) );
        long txIdBeforeQuery = env.lastClosedTxId();
        long batch = 40;

        // When
        Object[] result = runAndPull(
                machine,
                "USING PERIODIC COMMIT " + batch + "\n" +
                        "LOAD CSV WITH HEADERS FROM {csvFileUrl} AS l\n" +
                        "MATCH (c:Class {name: l.class_name})\n" +
                        "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, " +
                        "petal_length: l.petal_length, petal_width: l.petal_width})\n" +
                        "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
                        "RETURN count(*) AS c",
                params
        );

        // Then
        assertThat( result.length, equalTo( 1 ) );
        Record record = (Record) result[0];

        AnyValue[] fields = record.fields();
        assertThat( fields.length, equalTo( 1) );
        assertThat( fields[0], equalTo( longValue( 150L )) );

        /*
         * 7 tokens have been created for
         * 'Sample' label
         * 'HAS_CLASS' relationship type
         * 'name', 'sepal_length', 'sepal_width', 'petal_length', and 'petal_width' property keys
         *
         * Note that the token id for the label 'Class' has been created in `createLocalIrisData(...)` so it shouldn't1
         * be counted again here
         */
        long tokensCommits = 7;
        long commits = (IRIS_DATA.split( "\n" ).length - 1 /* header */) / batch;
        long txId = env.lastClosedTxId();
        assertEquals( tokensCommits + commits + txIdBeforeQuery, txId );
    }

    @Test
    public void shouldNotSupportUsingPeriodicCommitInTransaction() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );
        MapValue params = map( "csvFileUrl", createLocalIrisData( machine ) );
        runAndPull( machine, "BEGIN" );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run(
                "USING PERIODIC COMMIT 40\n" +
                        "LOAD CSV WITH HEADERS FROM {csvFileUrl} AS l\n" +
                        "MATCH (c:Class {name: l.class_name})\n" +
                        "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l" +
                        ".petal_length, petal_width: l.petal_width})\n" +
                        "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
                        "RETURN count(*) AS c",
                params,
                recorder
        );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SemanticError ) );
        // "Executing queries that use periodic commit in an open transaction is not possible."
    }

    @Test
    public void shouldCloseTransactionOnCommit() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        runAndPull( machine, "BEGIN" );
        runAndPull( machine, "RETURN 1" );
        runAndPull( machine, "COMMIT" );

        assertFalse( machine.statementProcessor().hasTransaction() );
    }

    @Test
    public void shouldCloseTransactionEvenIfCommitFails() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        runAndPull( machine, "BEGIN" );
        runAndPull( machine, "X", map(), IGNORED );
        machine.ackFailure( nullResponseHandler() );
        runAndPull( machine, "COMMIT", map(), IGNORED );
        machine.ackFailure( nullResponseHandler() );

        assertFalse( machine.statementProcessor().hasTransaction() );
    }

    @Test
    public void shouldCloseTransactionOnRollback() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        runAndPull( machine, "BEGIN" );
        runAndPull( machine, "RETURN 1" );
        runAndPull( machine, "ROLLBACK" );

        assertFalse( machine.statementProcessor().hasTransaction() );
    }

    @Test
    public void shouldCloseTransactionOnRollbackAfterFailure() throws Exception
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        runAndPull( machine, "BEGIN" );
        runAndPull( machine, "X", map(), IGNORED );
        machine.ackFailure( nullResponseHandler() );
        runAndPull( machine, "ROLLBACK" );

        assertFalse( machine.statementProcessor().hasTransaction() );
    }

    @Test
    public void shouldAllowNewTransactionAfterFailure() throws Throwable
    {
        // Given
        BoltStateMachine machine = env.newMachine( boltChannel );
        machine.init( USER_AGENT, emptyMap(), null );

        // And given I've started a transaction that failed
        runAndPull( machine, "BEGIN" );
        machine.run( "invalid", EMPTY_PARAMS, nullResponseHandler() );
        machine.reset( nullResponseHandler() );

        // When
        runAndPull( machine, "BEGIN" );
        Object[] stream = runAndPull( machine, "RETURN 1" );

        // Then
        assertThat( ((Record) stream[0]).fields()[0], equalTo( longValue( 1L )) );
    }

    private String createLocalIrisData( BoltStateMachine machine ) throws Exception
    {
        for ( String className : IRIS_CLASS_NAMES )
        {
            MapValue params = map( "className", className );
            runAndPull( machine, "CREATE (c:Class {name: {className}}) RETURN c", params );
        }

        return env.putTmpFile( "iris", ".csv", IRIS_DATA ).toExternalForm();
    }

    private Object[] runAndPull( BoltStateMachine machine, String statement ) throws Exception
    {
        return runAndPull( machine, statement, EMPTY_PARAMS, SUCCESS );
    }

    private Record[] runAndPull( BoltStateMachine machine, String statement, MapValue params ) throws Exception
    {
        return runAndPull( machine, statement, params, SUCCESS );
    }

    private Record[] runAndPull( BoltStateMachine machine, String statement, MapValue params,
            BoltResponseMessage expectedResponse ) throws Exception
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.run( statement, params, nullResponseHandler() );
        machine.pullAll( recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertEquals( expectedResponse, response.message() );
        return response.records();
    }

    private MapValue map( Object... keyValues )
    {
        return ValueUtils.asMapValue( MapUtil.map( keyValues ) );
    }

    private static String[] IRIS_CLASS_NAMES =
            new String[] {
                    "Iris-setosa",
                    "Iris-versicolor",
                    "Iris-virginica"
            };

    private static String IRIS_DATA =
            "sepal_length,sepal_width,petal_length,petal_width,class_name\n" +
                    "5.1,3.5,1.4,0.2,Iris-setosa\n" +
                    "4.9,3.0,1.4,0.2,Iris-setosa\n" +
                    "4.7,3.2,1.3,0.2,Iris-setosa\n" +
                    "4.6,3.1,1.5,0.2,Iris-setosa\n" +
                    "5.0,3.6,1.4,0.2,Iris-setosa\n" +
                    "5.4,3.9,1.7,0.4,Iris-setosa\n" +
                    "4.6,3.4,1.4,0.3,Iris-setosa\n" +
                    "5.0,3.4,1.5,0.2,Iris-setosa\n" +
                    "4.4,2.9,1.4,0.2,Iris-setosa\n" +
                    "4.9,3.1,1.5,0.1,Iris-setosa\n" +
                    "5.4,3.7,1.5,0.2,Iris-setosa\n" +
                    "4.8,3.4,1.6,0.2,Iris-setosa\n" +
                    "4.8,3.0,1.4,0.1,Iris-setosa\n" +
                    "4.3,3.0,1.1,0.1,Iris-setosa\n" +
                    "5.8,4.0,1.2,0.2,Iris-setosa\n" +
                    "5.7,4.4,1.5,0.4,Iris-setosa\n" +
                    "5.4,3.9,1.3,0.4,Iris-setosa\n" +
                    "5.1,3.5,1.4,0.3,Iris-setosa\n" +
                    "5.7,3.8,1.7,0.3,Iris-setosa\n" +
                    "5.1,3.8,1.5,0.3,Iris-setosa\n" +
                    "5.4,3.4,1.7,0.2,Iris-setosa\n" +
                    "5.1,3.7,1.5,0.4,Iris-setosa\n" +
                    "4.6,3.6,1.0,0.2,Iris-setosa\n" +
                    "5.1,3.3,1.7,0.5,Iris-setosa\n" +
                    "4.8,3.4,1.9,0.2,Iris-setosa\n" +
                    "5.0,3.0,1.6,0.2,Iris-setosa\n" +
                    "5.0,3.4,1.6,0.4,Iris-setosa\n" +
                    "5.2,3.5,1.5,0.2,Iris-setosa\n" +
                    "5.2,3.4,1.4,0.2,Iris-setosa\n" +
                    "4.7,3.2,1.6,0.2,Iris-setosa\n" +
                    "4.8,3.1,1.6,0.2,Iris-setosa\n" +
                    "5.4,3.4,1.5,0.4,Iris-setosa\n" +
                    "5.2,4.1,1.5,0.1,Iris-setosa\n" +
                    "5.5,4.2,1.4,0.2,Iris-setosa\n" +
                    "4.9,3.1,1.5,0.2,Iris-setosa\n" +
                    "5.0,3.2,1.2,0.2,Iris-setosa\n" +
                    "5.5,3.5,1.3,0.2,Iris-setosa\n" +
                    "4.9,3.6,1.4,0.1,Iris-setosa\n" +
                    "4.4,3.0,1.3,0.2,Iris-setosa\n" +
                    "5.1,3.4,1.5,0.2,Iris-setosa\n" +
                    "5.0,3.5,1.3,0.3,Iris-setosa\n" +
                    "4.5,2.3,1.3,0.3,Iris-setosa\n" +
                    "4.4,3.2,1.3,0.2,Iris-setosa\n" +
                    "5.0,3.5,1.6,0.6,Iris-setosa\n" +
                    "5.1,3.8,1.9,0.4,Iris-setosa\n" +
                    "4.8,3.0,1.4,0.3,Iris-setosa\n" +
                    "5.1,3.8,1.6,0.2,Iris-setosa\n" +
                    "4.6,3.2,1.4,0.2,Iris-setosa\n" +
                    "5.3,3.7,1.5,0.2,Iris-setosa\n" +
                    "5.0,3.3,1.4,0.2,Iris-setosa\n" +
                    "7.0,3.2,4.7,1.4,Iris-versicolor\n" +
                    "6.4,3.2,4.5,1.5,Iris-versicolor\n" +
                    "6.9,3.1,4.9,1.5,Iris-versicolor\n" +
                    "5.5,2.3,4.0,1.3,Iris-versicolor\n" +
                    "6.5,2.8,4.6,1.5,Iris-versicolor\n" +
                    "5.7,2.8,4.5,1.3,Iris-versicolor\n" +
                    "6.3,3.3,4.7,1.6,Iris-versicolor\n" +
                    "4.9,2.4,3.3,1.0,Iris-versicolor\n" +
                    "6.6,2.9,4.6,1.3,Iris-versicolor\n" +
                    "5.2,2.7,3.9,1.4,Iris-versicolor\n" +
                    "5.0,2.0,3.5,1.0,Iris-versicolor\n" +
                    "5.9,3.0,4.2,1.5,Iris-versicolor\n" +
                    "6.0,2.2,4.0,1.0,Iris-versicolor\n" +
                    "6.1,2.9,4.7,1.4,Iris-versicolor\n" +
                    "5.6,2.9,3.6,1.3,Iris-versicolor\n" +
                    "6.7,3.1,4.4,1.4,Iris-versicolor\n" +
                    "5.6,3.0,4.5,1.5,Iris-versicolor\n" +
                    "5.8,2.7,4.1,1.0,Iris-versicolor\n" +
                    "6.2,2.2,4.5,1.5,Iris-versicolor\n" +
                    "5.6,2.5,3.9,1.1,Iris-versicolor\n" +
                    "5.9,3.2,4.8,1.8,Iris-versicolor\n" +
                    "6.1,2.8,4.0,1.3,Iris-versicolor\n" +
                    "6.3,2.5,4.9,1.5,Iris-versicolor\n" +
                    "6.1,2.8,4.7,1.2,Iris-versicolor\n" +
                    "6.4,2.9,4.3,1.3,Iris-versicolor\n" +
                    "6.6,3.0,4.4,1.4,Iris-versicolor\n" +
                    "6.8,2.8,4.8,1.4,Iris-versicolor\n" +
                    "6.7,3.0,5.0,1.7,Iris-versicolor\n" +
                    "6.0,2.9,4.5,1.5,Iris-versicolor\n" +
                    "5.7,2.6,3.5,1.0,Iris-versicolor\n" +
                    "5.5,2.4,3.8,1.1,Iris-versicolor\n" +
                    "5.5,2.4,3.7,1.0,Iris-versicolor\n" +
                    "5.8,2.7,3.9,1.2,Iris-versicolor\n" +
                    "6.0,2.7,5.1,1.6,Iris-versicolor\n" +
                    "5.4,3.0,4.5,1.5,Iris-versicolor\n" +
                    "6.0,3.4,4.5,1.6,Iris-versicolor\n" +
                    "6.7,3.1,4.7,1.5,Iris-versicolor\n" +
                    "6.3,2.3,4.4,1.3,Iris-versicolor\n" +
                    "5.6,3.0,4.1,1.3,Iris-versicolor\n" +
                    "5.5,2.5,4.0,1.3,Iris-versicolor\n" +
                    "5.5,2.6,4.4,1.2,Iris-versicolor\n" +
                    "6.1,3.0,4.6,1.4,Iris-versicolor\n" +
                    "5.8,2.6,4.0,1.2,Iris-versicolor\n" +
                    "5.0,2.3,3.3,1.0,Iris-versicolor\n" +
                    "5.6,2.7,4.2,1.3,Iris-versicolor\n" +
                    "5.7,3.0,4.2,1.2,Iris-versicolor\n" +
                    "5.7,2.9,4.2,1.3,Iris-versicolor\n" +
                    "6.2,2.9,4.3,1.3,Iris-versicolor\n" +
                    "5.1,2.5,3.0,1.1,Iris-versicolor\n" +
                    "5.7,2.8,4.1,1.3,Iris-versicolor\n" +
                    "6.3,3.3,6.0,2.5,Iris-virginica\n" +
                    "5.8,2.7,5.1,1.9,Iris-virginica\n" +
                    "7.1,3.0,5.9,2.1,Iris-virginica\n" +
                    "6.3,2.9,5.6,1.8,Iris-virginica\n" +
                    "6.5,3.0,5.8,2.2,Iris-virginica\n" +
                    "7.6,3.0,6.6,2.1,Iris-virginica\n" +
                    "4.9,2.5,4.5,1.7,Iris-virginica\n" +
                    "7.3,2.9,6.3,1.8,Iris-virginica\n" +
                    "6.7,2.5,5.8,1.8,Iris-virginica\n" +
                    "7.2,3.6,6.1,2.5,Iris-virginica\n" +
                    "6.5,3.2,5.1,2.0,Iris-virginica\n" +
                    "6.4,2.7,5.3,1.9,Iris-virginica\n" +
                    "6.8,3.0,5.5,2.1,Iris-virginica\n" +
                    "5.7,2.5,5.0,2.0,Iris-virginica\n" +
                    "5.8,2.8,5.1,2.4,Iris-virginica\n" +
                    "6.4,3.2,5.3,2.3,Iris-virginica\n" +
                    "6.5,3.0,5.5,1.8,Iris-virginica\n" +
                    "7.7,3.8,6.7,2.2,Iris-virginica\n" +
                    "7.7,2.6,6.9,2.3,Iris-virginica\n" +
                    "6.0,2.2,5.0,1.5,Iris-virginica\n" +
                    "6.9,3.2,5.7,2.3,Iris-virginica\n" +
                    "5.6,2.8,4.9,2.0,Iris-virginica\n" +
                    "7.7,2.8,6.7,2.0,Iris-virginica\n" +
                    "6.3,2.7,4.9,1.8,Iris-virginica\n" +
                    "6.7,3.3,5.7,2.1,Iris-virginica\n" +
                    "7.2,3.2,6.0,1.8,Iris-virginica\n" +
                    "6.2,2.8,4.8,1.8,Iris-virginica\n" +
                    "6.1,3.0,4.9,1.8,Iris-virginica\n" +
                    "6.4,2.8,5.6,2.1,Iris-virginica\n" +
                    "7.2,3.0,5.8,1.6,Iris-virginica\n" +
                    "7.4,2.8,6.1,1.9,Iris-virginica\n" +
                    "7.9,3.8,6.4,2.0,Iris-virginica\n" +
                    "6.4,2.8,5.6,2.2,Iris-virginica\n" +
                    "6.3,2.8,5.1,1.5,Iris-virginica\n" +
                    "6.1,2.6,5.6,1.4,Iris-virginica\n" +
                    "7.7,3.0,6.1,2.3,Iris-virginica\n" +
                    "6.3,3.4,5.6,2.4,Iris-virginica\n" +
                    "6.4,3.1,5.5,1.8,Iris-virginica\n" +
                    "6.0,3.0,4.8,1.8,Iris-virginica\n" +
                    "6.9,3.1,5.4,2.1,Iris-virginica\n" +
                    "6.7,3.1,5.6,2.4,Iris-virginica\n" +
                    "6.9,3.1,5.1,2.3,Iris-virginica\n" +
                    "5.8,2.7,5.1,1.9,Iris-virginica\n" +
                    "6.8,3.2,5.9,2.3,Iris-virginica\n" +
                    "6.7,3.3,5.7,2.5,Iris-virginica\n" +
                    "6.7,3.0,5.2,2.3,Iris-virginica\n" +
                    "6.3,2.5,5.0,1.9,Iris-virginica\n" +
                    "6.5,3.0,5.2,2.0,Iris-virginica\n" +
                    "6.2,3.4,5.4,2.3,Iris-virginica\n" +
                    "5.9,3.0,5.1,1.8,Iris-virginica\n" +
                    "\n";
}
