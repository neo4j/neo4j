/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v4.runtime;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.succeededWithRecord;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.begin;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.commit;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.discardAll;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.pullAll;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.reset;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.rollback;
import static org.neo4j.bolt.v4.messaging.BoltV4Messages.run;
import static org.neo4j.values.storable.Values.stringValue;

class BoltConnectionIT extends BoltStateMachineV4StateTestBase
{
    @Test
    void shouldExecuteStatement() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();

        // When
        var runRecorder = new BoltResponseRecorder();
        machine.process( run( "CREATE (n {k:'k'}) RETURN n.k" ), runRecorder );
        // Then
        assertThat( runRecorder.nextResponse(), succeeded() );

        // When
        var pullRecorder = new BoltResponseRecorder();
        machine.process( pullAll(), pullRecorder );
        // Then
        pullRecorder.nextResponse().assertRecord( 0, stringValue( "k" ) );
    }

    @Test
    void shouldHandleImplicitCommitFailure() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        machine.process( run( "CREATE (n:Victim)-[:REL]->()" ), nullResponseHandler() );
        machine.process( discardAll(), nullResponseHandler() );

        // When I perform an action that will fail on commit
        var recorder = new BoltResponseRecorder();
        machine.process( run( "MATCH (n:Victim) DELETE n" ), recorder );
        // Then the statement running should have succeeded
        assertThat( recorder.nextResponse(), succeeded() );

        // But the streaming should have failed, since it implicitly triggers commit and thus triggers a failure
        machine.process( discardAll(), recorder );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Schema.ConstraintValidationFailed ) );
    }

    @Test
    void shouldAllowUserControlledRollbackOnExplicitTxFailure() throws Throwable
    {
        // Given whenever en explicit transaction has a failure,
        // it is more natural for drivers to see the failure, acknowledge it
        // and send a `RESET`, because that means that all failures in the
        // transaction, be they client-local or inside neo, can be handled the
        // same way by a driver.
        var machine = newStateMachineAfterAuth();

        machine.process( begin(), nullResponseHandler() );
        machine.process( run( "CREATE (n:Victim)-[:REL]->()" ), nullResponseHandler() );
        machine.process( discardAll(), nullResponseHandler() );

        // When I perform an action that will fail
        var recorder = new BoltResponseRecorder();
        machine.process( run( "this is not valid syntax" ), recorder );
        // Then I should see a failure
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );
        // This result in an illegal state change, and closes all open statement by default.
        assertFalse( machine.hasOpenStatement() );

        // And when I reset that failure, and the tx should be rolled back
        recorder.reset();
        resetReceived( machine, recorder );

        // Then both operations should succeed
        assertThat( recorder.nextResponse(), succeeded() );
        assertFalse( machine.hasOpenStatement() );
    }

    @Test
    void shouldHandleFailureDuringResultPublishing() throws Throwable
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var pullAllCallbackCalled = new CountDownLatch( 1 );
        var error = new AtomicReference<Neo4jError>();

        // When something fails while publishing the result stream
        machine.process( run(), nullResponseHandler() );
        machine.process( pullAll(), new BoltResponseHandler()
        {
            @Override
            public boolean onPullRecords( BoltResult result, long size ) throws Throwable
            {
                throw new RuntimeException( "Ooopsies!" );
            }

            @Override
            public boolean onDiscardRecords( BoltResult result, long size ) throws Throwable
            {
                throw new RuntimeException( "Not this one!" );
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
        var err = error.get();
        assertThat( err.status(), equalTo( Status.General.UnknownError ) );
        assertThat( err.message(), CoreMatchers.containsString( "Ooopsies!" ) );
    }

    @Test
    void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread() throws Throwable
    {
        // Given
        var firstMachine = newStateMachineAfterAuth();
        var secondMachine = newStateMachineAfterAuth();

        // And given I've started a transaction in one session
        firstMachine.process( begin(), nullResponseHandler() );

        // When I issue a statement in a separate session
        secondMachine.process( run( "CREATE (a:Person) RETURN id(a)" ), nullResponseHandler() );

        var recorder = new BoltResponseRecorder();
        secondMachine.process( pullAll(), recorder );
        var response = recorder.nextResponse();
        var id = ((LongValue) response.singleValueRecord()).longValue();
        assertThat( response, succeeded() );

        // And when I roll back that first session transaction
        firstMachine.process( BoltV4Messages.rollback(), nullResponseHandler() );

        // Then the two should not have interfered with each other
        recorder.reset();
        secondMachine.process( run( "MATCH (a:Person) WHERE id(a) = " + id + " RETURN COUNT(*)" ), recorder );
        secondMachine.process( pullAll(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 1L ) );
    }

    @Test
    void shouldSupportUsingPeriodicCommitInSession() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var params = map( "csvFileUrl", createLocalIrisData( machine ) );
        var txIdBeforeQuery = env.lastClosedTxId();
        var batch = 40;

        // When
        var recorder = new BoltResponseRecorder();
        machine.process( run(
                "USING PERIODIC COMMIT " + batch + "\n" +
                        "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l\n" +
                        "MATCH (c:Class {name: l.class_name})\n" +
                        "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, " +
                        "petal_length: l.petal_length, petal_width: l.petal_width})\n" +
                        "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
                        "RETURN count(*) AS c",
                params ), recorder
        );
        machine.process( pullAll(), recorder );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeededWithRecord( 150L ) );

        /*
         * 7 tokens have been created for
         * 'Sample' label
         * 'HAS_CLASS' relationship type
         * 'name', 'sepal_length', 'sepal_width', 'petal_length', and 'petal_width' property keys
         *
         * Note that the token id for the label 'Class' has been created in `createLocalIrisData(...)` so it shouldn't1
         * be counted again here
         */
        var tokensCommits = 7;
        var commits = (IRIS_DATA.split( "\n" ).length - 1 /* header */) / batch;
        var txId = env.lastClosedTxId();
        assertEquals( tokensCommits + commits + txIdBeforeQuery, txId );
    }

    @Test
    void shouldNotSupportUsingPeriodicCommitInTransaction() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var params = map( "csvFileUrl", createLocalIrisData( machine ) );
        machine.process( begin(), nullResponseHandler() );

        // When
        var recorder = new BoltResponseRecorder();
        machine.process( run(
                "USING PERIODIC COMMIT 40\n" +
                        "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l\n" +
                        "MATCH (c:Class {name: l.class_name})\n" +
                        "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l" +
                        ".petal_length, petal_width: l.petal_width})\n" +
                        "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
                        "RETURN count(*) AS c",
                        params ),
                recorder
        );

        // Then
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SemanticError ) );
        // "Executing queries that use periodic commit in an open transaction is not possible."
    }

    @Test
    void shouldSupportUsingExplainPeriodicCommitInTransaction() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();
        var params = map( "csvFileUrl", createLocalIrisData( machine ) );
        machine.process( begin(), nullResponseHandler() );

        // When
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( run(
                "EXPLAIN USING PERIODIC COMMIT 40\n" +
                "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l\n" +
                "MATCH (c:Class {name: l.class_name})\n" +
                "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l" +
                ".petal_length, petal_width: l.petal_width})\n" +
                "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
                "RETURN count(*) AS c",
                params ), recorder
        );

        // Then
        assertThat( recorder.nextResponse(), succeeded() );
    }

    @Test
    void shouldCloseTransactionOnCommit() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process( begin(), nullResponseHandler() );
        runAndPull( machine );
        machine.process( commit(), nullResponseHandler() );

        assertFalse( hasTransaction( machine ) );
    }

    @Test
    void shouldCloseTransactionEvenIfCommitFails() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process( begin(), nullResponseHandler() );
        var recorder = new BoltResponseRecorder();
        machine.process( run( "X" ), recorder );
        machine.process( pullAll(), recorder );
        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );
        assertThat( recorder.nextResponse(), wasIgnored() );

        // The tx shall still be open.
        assertTrue( hasTransaction( machine ) );

        recorder.reset();
        machine.process( commit(), recorder );
        assertThat( recorder.nextResponse(), wasIgnored() );

        resetReceived( machine, recorder );
        assertThat( recorder.nextResponse(), succeeded() );

        assertFalse( hasTransaction( machine ) );
    }

    @Test
    void shouldCloseTransactionOnRollback() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process( begin(), nullResponseHandler() );
        runAndPull( machine );
        machine.process( rollback(), nullResponseHandler() );

        assertFalse( hasTransaction( machine ) );
    }

    @Test
    void shouldCloseTransactionOnRollbackAfterFailure() throws Exception
    {
        // Given
        var machine = newStateMachineAfterAuth();

        machine.process( begin(), nullResponseHandler() );
        var recorder = new BoltResponseRecorder();
        machine.process( run( "X" ), recorder );
        machine.process( pullAll(), recorder );

        assertThat( recorder.nextResponse(), failedWithStatus( Status.Statement.SyntaxError ) );
        assertThat( recorder.nextResponse(), wasIgnored() );

        // The tx shall still be open.
        assertTrue( hasTransaction( machine ) );

        recorder.reset();
        machine.process( rollback(), recorder );
        assertThat( recorder.nextResponse(), wasIgnored() );
        resetReceived( machine, recorder );
        assertThat( recorder.nextResponse(), succeeded() );

        assertFalse( hasTransaction( machine ) );
    }

    private void resetReceived( BoltStateMachineV4 machine, BoltResponseRecorder recorder ) throws BoltConnectionFatality
    {
        // Reset is two steps now: When parsing reset message we immediately interrupt, and then ignores all other messages until reset is reached.
        machine.interrupt();
        machine.process( reset(), recorder );
    }

    private static boolean hasTransaction( BoltStateMachine machine )
    {
        return ((AbstractBoltStateMachine) machine).statementProcessor().hasTransaction();
    }

    private String createLocalIrisData( BoltStateMachine machine ) throws Exception
    {
        for ( String className : IRIS_CLASS_NAMES )
        {
            MapValue params = map( "className", className );
            runAndPull( machine, "CREATE (c:Class {name: $className}) RETURN c", params );
        }

        return env.putTmpFile( "iris", ".csv", IRIS_DATA ).toExternalForm();
    }

    private void runAndPull( BoltStateMachine machine ) throws Exception
    {
        runAndPull( machine, "RETURN 1", EMPTY_PARAMS );
    }

    private void runAndPull( BoltStateMachine machine, String statement, MapValue params ) throws Exception
    {
        var recorder = new BoltResponseRecorder();
        machine.process( run( statement, params ), recorder );
        machine.process( pullAll(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( recorder.nextResponse(), succeeded() );
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
