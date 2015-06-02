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
package org.neo4j.ndp.runtime.internal.session;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.StatementMetadata;
import org.neo4j.ndp.runtime.integration.RecordingCallback;
import org.neo4j.ndp.runtime.internal.StatementRunner;
import org.neo4j.stream.RecordStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.failedWith;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.ignored;
import static org.neo4j.ndp.runtime.integration.SessionMatchers.success;
import static org.neo4j.ndp.runtime.internal.session.SessionStateMachine.State.ERROR;

public class StateMachineErrorTest
{
    private static final Map<String, Object> EMPTY_PARAMS = Collections.emptyMap();

    private GraphDatabaseService db = mock( GraphDatabaseService.class );
    private ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
    private StatementRunner runner = mock( StatementRunner.class );
    private Transaction tx = mock( TopLevelTransaction.class );

    @Before
    public void setup()
    {
        when( db.beginTx() ).thenReturn( tx );
    }

    @Test
    public void testSyntaxError() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> responses = new RecordingCallback<>();

        doThrow( new SyntaxException( "src/test" ) ).when( runner ).run( any( SessionState.class ),
                any( String.class ), any( Map.class ) );

        SessionStateMachine machine = new SessionStateMachine( db, txBridge, runner, NullLogService.getInstance() );

        // When
        machine.run( "this is nonsense", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), failedWith( Status.Statement.InvalidSyntax ) );
        assertThat( machine.state(), equalTo( ERROR ) );
    }

    @Test
    public void testPublishingError() throws Throwable
    {
        // Given
        RecordingCallback<RecordStream, Object> failingCallback = new RecordingCallback<RecordStream, Object>()
        {
            @Override
            public void result( RecordStream result, Object attachment )
            {
                throw new RuntimeException( "Well, that didn't work out very well." );
            }
        };
        when( runner.run( any( SessionState.class ), any( String.class ), any( Map.class ) ) )
                .thenReturn( mock( RecordStream.class ) );

        SessionStateMachine machine = new SessionStateMachine( db, txBridge, runner, NullLogService.getInstance() );

        // and Given there is a result ready to be retrieved
        machine.run( "something", null, null, Session.Callbacks.<StatementMetadata, Object>noop() );

        // When
        machine.pullAll( null, failingCallback );

        // Then
        assertThat( failingCallback.next(), failedWith( Status.General.UnknownFailure ) );
        assertThat( machine.state(), equalTo( ERROR ) );
    }

    @Test
    public void testRollbackError() throws Throwable
    {
        // Given
        SessionStateMachine machine = new SessionStateMachine( db, txBridge, runner, NullLogService.getInstance() );

        // Given there is a running transaction
        machine.beginTransaction();

        // And given that transaction will fail to roll back
        doThrow( new TransactionFailureException( "This just isn't going well for us." ) ).when( tx ).close();

        // When
        machine.rollbackTransaction();

        // Then
        assertThat( machine.state(), equalTo( ERROR ) );
    }

    @Test
    public void testCantDoAnythingIfInErrorState() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> messages = new RecordingCallback<>();
        RecordingCallback<RecordStream, Object> pulling = new RecordingCallback<>();
        SessionStateMachine machine = new SessionStateMachine( db, txBridge, runner, NullLogService.getInstance() );

        // When I perform some action that causes an error state
        machine.commitTransaction(); // No tx to be committed!

        // Then it should be in an error state
        assertThat( machine.state(), equalTo( ERROR ) );

        // and no action other than acknowledging the error should be possible
        machine.beginTransaction();
        assertThat( machine.state(), equalTo( ERROR ) );

        machine.beginImplicitTransaction();
        assertThat( machine.state(), equalTo( ERROR ) );

        machine.commitTransaction();
        assertThat( machine.state(), equalTo( ERROR ) );

        machine.rollbackTransaction();
        assertThat( machine.state(), equalTo( ERROR ) );

        // this includes externally triggered actions
        machine.run( "src/test", EMPTY_PARAMS, null, messages );
        assertThat( machine.state(), equalTo( ERROR ) );
        assertThat( messages.next(), ignored() );

        machine.pullAll( null, pulling );
        assertThat( machine.state(), equalTo( ERROR ) );
        assertThat( pulling.next(), ignored() );

        // And nothing at all should have been done
        verifyNoMoreInteractions( db, runner );
    }

    @Test
    public void testAcknowledgingError() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> messages = new RecordingCallback<>();
        RecordingCallback<Void, Object> failures = new RecordingCallback<>();
        SessionStateMachine machine = new SessionStateMachine( db, txBridge, runner, NullLogService.getInstance() );

        // Given I've performed some action that causes an error state
        machine.commitTransaction(); // No tx to be committed!

        // When
        machine.acknowledgeFailure( null, failures );

        // Then
        assertThat( failures.next(), success() );

        // And when I know run some other operation
        machine.run( "src/test", EMPTY_PARAMS, null, messages );

        // Then
        assertThat( messages.next(), success() );

    }
}
