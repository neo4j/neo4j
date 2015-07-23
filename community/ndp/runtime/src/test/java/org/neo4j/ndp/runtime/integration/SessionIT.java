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
package org.neo4j.ndp.runtime.integration;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.StatementMetadata;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.ndp.runtime.spi.Record;
import org.neo4j.ndp.runtime.spi.RecordStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.failedWith;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.streamContaining;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.success;
import static org.neo4j.ndp.runtime.spi.StreamMatchers.eqRecord;

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
        session.initialize( "TestClient/1.0", null, null );

        // When
        session.run( "CREATE (n {k:'k'}) RETURN n.k", Collections.<String,Object>emptyMap(), null, responses );

        // Then
        assertThat( responses.next(), success() );


        // When
        session.pullAll( null, pulling );

        // Then
        assertThat( pulling.next(), streamContaining( eqRecord( equalTo( "k" ) ) ) );
    }

    @Test
    public void shouldSucceedOn__run__pullAll__run() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );

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
        session.initialize( "TestClient/1.0", null, null );
        session.run( "CREATE (n:Victim)-[:REL]->()", EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata, Object>noop() );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // When I perform an action that will fail on commit
        session.run( "MATCH (n:Victim) DELETE n", EMPTY_PARAMS, null, responses );
        session.discardAll( null, discarding );

        // Then the statement running should have succeeded
        assertThat( responses.next(), success() );

        // But the stop should have failed, since it implicitly triggers commit and thus triggers a failure
        assertThat( discarding.next(), failedWith( Status.Transaction.ValidationFailed ) );
    }

    @Test
    public void shouldHandleFailureDuringResultPublishing() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.initialize( "TestClient/1.0", null, null );

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
        pullAllCallbackCalled.await( 30, TimeUnit.SECONDS );
        final Neo4jError err = error.get();
        assertThat( err.status(), equalTo( (Status)Status.General.UnknownFailure ) );
        assertThat( err.message(), containsString( "Ooopsies!" ) );
    }

    @Test
    public void shouldBeAbleToCleanlyRunMultipleSessionsInSingleThread() throws Throwable
    {
        // Given
        Session firstSession = env.newSession();
        firstSession.initialize( "TestClient/1.0", null, null );
        Session secondSession = env.newSession();
        secondSession.initialize( "TestClient/1.0", null, null );

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
    public void shouldAllowNewTransactionAfterFailure() throws Throwable
    {
        // Given
        Session session = env.newSession();
        session.initialize( "TestClient/1.0", null, null );

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

    private Object[] runAndPull( Session session, String statement ) throws InterruptedException
    {
        RecordingCallback<RecordStream, ?> cb = new RecordingCallback<>();
        session.run( statement, EMPTY_PARAMS, null, Session.Callbacks.<StatementMetadata,Object>noop() );
        session.pullAll( null, cb );
        return ((RecordingCallback.Result) cb.next()).records();
    }


}
