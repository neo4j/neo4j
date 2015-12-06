/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.internal.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StreamMatchers;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.failedWith;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.streamContaining;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.success;

public class SessionIT
{
    private static final Map<String,Object> EMPTY_PARAMS = Collections.emptyMap();

    @Rule
    public TestSessions env = new TestSessions();
    private final RecordingCallback<StatementMetadata, ?> responses = new RecordingCallback<>();
    private final RecordingCallback<RecordStream, ?> pulling = new RecordingCallback<>();
    private final RecordingCallback<Void, ?> discarding = new RecordingCallback<>();

    @Test
    public void shouldExecuteStatement() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // When
        session.run( "CREATE (n {k:'k'}) RETURN n.k", Collections.<String,Object>emptyMap(), null, responses );

        // Then
        assertThat( responses.next(), success() );


        // When
        session.pullAll( null, pulling );

        // Then
        assertThat( pulling.next(), streamContaining( StreamMatchers.eqRecord( equalTo( "k" ) ) ) );
    }

    @Test
    public void shouldSucceedOn__run__pullAll__run() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.pullAll( null, Session.Callbacks.<RecordStream,Object>noop() );

        // When I run a new statement
        session.run( "RETURN 1", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldSucceedOn__run__discardAll__run() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata,Object>noop() );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // When I run a new statement
        session.run( "RETURN 1", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldSucceedOn__run_BEGIN__pullAll__run_COMMIT__pullALL__run_COMMIT() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "BEGIN", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.pullAll( null, Session.Callbacks.<RecordStream,Object>noop() );
        session.run( "COMMIT", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata,Object>noop() );
        session.pullAll( null, Session.Callbacks.<RecordStream,Object>noop() );

        // When I run a new statement
        session.run( "BEGIN", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldFailOn__run__run() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran one statement
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );

        // When I run a new statement, before consuming the stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), failedWith( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__pullAll__pullAll() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.pullAll( null, Session.Callbacks.<RecordStream,Object>noop() );

        // When I attempt to pull more items from the stream
        session.pullAll( null, pulling );

        // Then
        assertThat( pulling.next(), failedWith( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__pullAll__discardAll() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.pullAll( null, Session.Callbacks.<RecordStream,Object>noop() );

        // When I attempt to pull more items from the stream
        session.discardAll( null, discarding );

        // Then
        assertThat( discarding.next(), failedWith( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__discardAll__discardAll() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // When I attempt to pull more items from the stream
        session.discardAll( null, discarding );

        // Then
        assertThat( discarding.next(), failedWith( Status.Request.Invalid ) );
    }

    @Test
    public void shouldFailOn__discardAll__pullAll() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And Given that I've ran and pulled one stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // When I attempt to pull more items from the stream
        session.pullAll( null, pulling );

        // Then
        assertThat( pulling.next(), failedWith( Status.Request.Invalid ) );
    }

    @Test
    public void shouldHandleImplicitCommitFailure() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );
        session.run( "CREATE (n:Victim)-[:REL]->()", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // When I perform an action that will fail on commit
        session.run( "MATCH (n:Victim) DELETE n", EMPTY_PARAMS, null, responses );
        session.discardAll( null, discarding );

        // Then the statement running should have succeeded
        assertThat( responses.next(), success() );

        // But the stop should have failed, since it implicitly triggers commit and thus triggers a failure
        assertThat( discarding.next(), failedWith( Status.Schema.ConstraintViolation ) );
    }

    @Test
    public void shouldHandleFailureDuringResultPublishing() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        final CountDownLatch pullAllCallbackCalled = new CountDownLatch( 1 );
        final AtomicReference<Neo4jError> error = new AtomicReference<>();

        // When something fails while publishing the result stream
        session.run( "RETURN 1", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.pullAll( null, new Session.Callback.Adapter<RecordStream,Object>()
        {
            @Override
            public void result( RecordStream result, Object attachment ) throws Exception
            {
                throw new RuntimeException( "Ooopsies!" );
            }

            @Override
            public void failure( Neo4jError err, Object attachment )
            {
                error.set( err );
            }

            @Override
            public void completed( Object attachment )
            {
                pullAllCallbackCalled.countDown();
            }
        } );

        // Then
        assertTrue( pullAllCallbackCalled.await( 30, TimeUnit.SECONDS ) );
        final Neo4jError err = error.get();
        assertThat( err.status(), equalTo( (Status) Status.General.UnknownFailure ) );
        assertThat( err.message(), CoreMatchers.containsString( "Ooopsies!" ) );
    }

    @Test
    public void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread() throws Throwable
    {
        // Given
        Session firstSession = env.newSession();
        firstSession.init( "TestClient/1.0", null, null );
        Session secondSession = env.newSession();
        secondSession.init( "TestClient/1.0", null, null );

        // And given I've started a transaction in one session
        runAndPull( firstSession, "BEGIN" );

        // When I issue a statement in a separate session
        Object[] stream = runAndPull( secondSession, "CREATE (a:Person) RETURN id(a)" );
        long id = (long) ((Record) stream[0]).fields()[0];

        // And when I roll back that first session transaction
        runAndPull( firstSession, "ROLLBACK" );

        // Then the two should not have interfered with each other
        stream = runAndPull( secondSession, "MATCH (a:Person) WHERE id(a) = " + id + " RETURN COUNT(*)" );
        assertThat( ((Record) stream[0]).fields()[0], equalTo( (Object) 1L ) );
    }

    @Test
    public void shouldSupportUsingPeriodicCommitInSession() throws InterruptedException, IOException
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );
        Map<String, Object> params = new HashMap<>();
        params.put( "csvFileUrl", createLocalIrisData( session ) );

        // When
        Object[] result = runAndPull(
            session,
            "USING PERIODIC COMMIT 40\n" +
            "LOAD CSV WITH HEADERS FROM {csvFileUrl} AS l\n" +
            "MATCH (c:Class {name: l.class_name})\n" +
            "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l.petal_length, petal_width: l.petal_width})\n" +
            "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
            "RETURN count(*) AS c",
            params
        );

        // Then
        assertThat( result.length, equalTo( 1 ) );
        Record record = (Record) result[0];

        Object[] fields = record.fields();
        assertThat( fields.length, equalTo( 1) );
        assertThat( fields[0], equalTo( 150L ) );
    }

    @Test
    public void shouldNotSupportUsingPeriodicCommitInTransaction() throws InterruptedException, IOException
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );
        Map<String, Object> params = new HashMap<>();
        params.put( "csvFileUrl", createLocalIrisData( session ) );
        runAndPull( session, "BEGIN" );

        // When
        session.run(
            "USING PERIODIC COMMIT 40\n" +
            "LOAD CSV WITH HEADERS FROM {csvFileUrl} AS l\n" +
            "MATCH (c:Class {name: l.class_name})\n" +
            "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l" +
            ".petal_length, petal_width: l.petal_width})\n" +
            "CREATE (c)<-[:HAS_CLASS]-(s)\n" +
            "RETURN count(*) AS c",
            params,
            null,
            responses
        );

        // Then
        Neo4jError error = responses.next().error();
        assertThat( error.status(), equalTo( Status.Statement.InvalidSemantics ) );
        assertThat( error.message(), equalTo( "Executing queries that use periodic commit in an open transaction is not possible." ) );
    }

    @Test
    public void shouldAllowNewTransactionAfterFailure() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.init( "TestClient/1.0", null, null );

        // And given I've started a transaction that failed
        runAndPull( session, "BEGIN" );
        session.run( "invalid", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.acknowledgeFailure( null, Session.Callbacks.<Void,Object>noop() );

        // When
        runAndPull( session, "BEGIN" );
        Object[] stream = runAndPull( session, "RETURN 1" );

        // Then
        assertThat( ((Record) stream[0]).fields()[0], equalTo( (Object) 1L ) );

    }

    private String createLocalIrisData( Session session ) throws IOException, InterruptedException
    {
        for ( String className : IRIS_CLASS_NAMES )
        {
            Map<String, Object> params = new HashMap<>();
            params.put( "className", className );
            runAndPull( session, "CREATE (c:Class {name: {className}}) RETURN c", params );
        }

        return env.putTmpFile( "iris", ".csv", IRIS_DATA ).toExternalForm();
    }

    private Object[] runAndPull( Session session, String statement ) throws InterruptedException
    {
        return runAndPull( session, statement, EMPTY_PARAMS );
    }

    private Object[] runAndPull( Session session, String statement, Map<String, Object> params ) throws InterruptedException
    {
        RecordingCallback<RecordStream, ?> cb = new RecordingCallback<>();
        session.run( statement, params, null, Session.Callbacks.<StatementMetadata,Object>noop() );
        session.pullAll( null, cb );
        return ((RecordingCallback.Result) cb.next()).records();
    }

    public static String[] IRIS_CLASS_NAMES =
        new String[] {
                "Iris-setosa",
                "Iris-versicolor",
                "Iris-virginica"
        };

    public static String IRIS_DATA =
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
