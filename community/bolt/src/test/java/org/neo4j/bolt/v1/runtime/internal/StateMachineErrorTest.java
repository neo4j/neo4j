/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.integration.RecordingCallback;
import org.neo4j.bolt.v1.runtime.integration.SessionMatchers;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.udc.UsageData;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.failedWith;
import static org.neo4j.helpers.collection.MapUtil.map;

public class StateMachineErrorTest
{
    private static final Map<String, Object> EMPTY_PARAMS = Collections.emptyMap();

    private GraphDatabaseFacade db = mock( GraphDatabaseFacade.class );
    private ThreadToStatementContextBridge txBridge = mock( ThreadToStatementContextBridge.class );
    private StatementRunner runner = mock( StatementRunner.class );
    private InternalTransaction tx = mock( InternalTransaction.class );
    private JobScheduler scheduler = mock(JobScheduler.class );

    @Before
    public void setup()
    {
        when( db.beginTransaction( any( KernelTransaction.Type.class ), any( AccessMode.class )) ).thenReturn( tx );
    }

    @Test
    public void testSyntaxError() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> responses = new RecordingCallback<>();

        doThrow( new SyntaxException( "src/test" ) ).when( runner ).run( any( SessionState.class ),
                any( String.class ), any( Map.class ) );

        SessionStateMachine machine = newIdleMachine();

        // When
        machine.run( "this is nonsense", EMPTY_PARAMS, null, responses );

        // Then
        assertThat( responses.next(), failedWith( Status.Statement.SyntaxError ) );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
    }

    private SessionStateMachine newIdleMachine()
    {
        SessionStateMachine machine = new SessionStateMachine( "<idle>", new UsageData( scheduler ), db, txBridge, runner, NullLogService
                .getInstance(), Authentication.NONE );
        machine.init( "FunClient", map(), null, Session.Callback.NO_OP );
        return machine;
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

        SessionStateMachine machine = newIdleMachine();

        // and Given there is a result ready to be retrieved
        machine.run( "something", null, null, Session.Callbacks.<StatementMetadata, Object>noop() );

        // When
        machine.pullAll( null, failingCallback );

        // Then
        assertThat( failingCallback.next(), failedWith( Status.General.UnknownError ) );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
    }

    @Test
    public void testRollbackError() throws Throwable
    {
        // Given
        SessionStateMachine machine = newIdleMachine();

        // Given there is a running transaction
        machine.beginTransaction();

        // And given that transaction will fail to roll back
        doThrow( new TransactionFailureException( "This just isn't going well for us." ) ).when( tx ).close();

        // When
        machine.rollbackTransaction();

        // Then
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
    }

    @Test
    public void testCantDoAnythingIfInErrorState() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> messages = new RecordingCallback<>();
        RecordingCallback<RecordStream, Object> pulling = new RecordingCallback<>();
        RecordingCallback<Boolean, Object> initializing = new RecordingCallback<>();

        SessionStateMachine machine = newIdleMachine();

        // When I perform some action that causes an error state
        machine.commitTransaction(); // No tx to be committed!

        // Then it should be in an error state
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );

        // and no action other than acknowledging the error should be possible
        machine.beginTransaction();
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );

        machine.beginImplicitTransaction();
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );

        machine.commitTransaction();
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );

        machine.rollbackTransaction();
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );

        // this includes externally triggered actions
        machine.run( "src/test", EMPTY_PARAMS, null, messages );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
        assertThat( messages.next(), SessionMatchers.ignored() );

        machine.pullAll( null, pulling );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
        assertThat( pulling.next(), SessionMatchers.ignored() );

        machine.init( "", Collections.emptyMap(), null, initializing );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.ERROR ) );
        assertThat( initializing.next(), SessionMatchers.ignored() );

        // And nothing at all should have been done
        Mockito.verifyNoMoreInteractions( db, runner );
    }

    @Test
    public void testUsingResetToAcknowledgeError() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, Object> messages = new RecordingCallback<>();
        RecordingCallback<Void, Object> failures = new RecordingCallback<>();

        SessionStateMachine machine = newIdleMachine();

        // Given I've performed some action that causes an error state
        machine.commitTransaction(); // No tx to be committed!

        // When
        machine.reset( null, failures );

        // Then
        assertThat( failures.next(), SessionMatchers.success() );

        // And when I know run some other operation
        machine.run( "src/test", EMPTY_PARAMS, null, messages );

        // Then
        assertThat( messages.next(), SessionMatchers.success() );

    }

    @Test
    public void actionsDisallowedBeforeInitialized() throws Throwable
    {
        // Given
        RecordingCallback messages = new RecordingCallback();
        SessionStateMachine machine = new SessionStateMachine( "<test>", new UsageData( scheduler ), db, txBridge, runner, NullLogService
                .getInstance(), Authentication.NONE );

        // When
        machine.run( "RETURN 1", null, null, messages );

        // Then
        assertThat( messages.next(), failedWith( Status.Request.Invalid ) );
        assertThat( machine.state(), equalTo( SessionStateMachine.State.STOPPED ) );
    }
}
