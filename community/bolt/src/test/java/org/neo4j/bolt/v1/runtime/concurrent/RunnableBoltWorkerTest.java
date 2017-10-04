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
package org.neo4j.bolt.v1.runtime.concurrent;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class RunnableBoltWorkerTest
{

    private AssertableLogProvider internalLog;
    private AssertableLogProvider userLog;
    private LogService logService;
    private BoltStateMachine machine;

    @Before
    public void setup()
    {
        internalLog = new AssertableLogProvider();
        userLog = new AssertableLogProvider();
        logService = mock( LogService.class );
        when( logService.getUserLogProvider() ).thenReturn( userLog );
        when( logService.getUserLog( RunnableBoltWorker.class ) )
                .thenReturn( userLog.getLog( RunnableBoltWorker.class ) );
        when( logService.getInternalLogProvider() ).thenReturn( internalLog );
        when( logService.getInternalLog( RunnableBoltWorker.class ) )
                .thenReturn( internalLog.getLog( RunnableBoltWorker.class ) );
        machine = mock( BoltStateMachine.class );
        when( machine.key() ).thenReturn( "test-session" );
    }

    @Test
    public void shouldExecuteWorkWhenRun() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s -> s.run( "Hello, world!", null, null ) );
        worker.enqueue( s -> worker.halt() );

        // When
        worker.run();

        // Then
        verify( machine ).run( "Hello, world!", null, null );
        verify( machine ).terminate();
        verify( machine ).close();
        verifyNoMoreInteractions( machine );
    }

    @Test
    public void errorThrownDuringExecutionShouldCauseSessionClose() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s ->
        {
            throw new RuntimeException( "It didn't work out." );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
    }

    @Test
    public void authExceptionShouldNotBeLoggedHere() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        worker.enqueue( s ->
        {
            throw new BoltConnectionAuthFatality( "fatality" );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
        internalLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
        userLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
    }

    @Test
    public void protocolBreachesShouldBeLoggedWithStackTraces() throws Throwable
    {
        // Given
        BoltProtocolBreachFatality error = new BoltProtocolBreachFatality( "protocol breach fatality" );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        worker.enqueue( s ->
        {
            throw error;
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
        internalLog.assertExactly( inLog( RunnableBoltWorker.class )
                .error( equalTo( "Bolt protocol breach in session 'test-session'" ), equalTo( error ) ) );
        userLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
    }

    @Test
    public void haltShouldTerminateButNotCloseTheStateMachine()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();

        verify( machine ).terminate();
        verify( machine, never() ).close();
    }

    @Test
    public void workerCanBeHaltedMultipleTimes()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();
        worker.halt();
        worker.halt();

        verify( machine, times( 3 ) ).terminate();
        verify( machine, never() ).close();

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void stateMachineIsClosedOnExit()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.enqueue( machine1 ->
        {
            machine1.run( "RETURN 1", null, null );
            worker.enqueue( machine2 ->
            {
                machine2.run( "RETURN 1", null, null );
                worker.enqueue( machine3 ->
                {
                    worker.halt();
                    worker.enqueue( machine4 -> fail( "Should not be executed" ) );
                } );
            } );
        } );

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void stateMachineNotClosedOnHalt()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();

        verify( machine, never() ).close();
    }

    @Test
    public void stateMachineInterrupted()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.interrupt();

        verify( machine ).interrupt();
    }

    @Test
    public void stateMachineCloseFailureIsLogged()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        RuntimeException closeError = new RuntimeException( "Oh!" );
        doThrow( closeError ).when( machine ).close();

        worker.enqueue( s -> worker.halt() );
        worker.run();

        internalLog.assertExactly( inLog( RunnableBoltWorker.class ).error(
                equalTo( "Unable to close Bolt session 'test-session'" ),
                equalTo( closeError ) ) );
    }

    @Test
    public void haltIsRespected()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.enqueue( machine1 ->
                worker.enqueue( machine2 ->
                        worker.enqueue( machine3 ->
                        {
                            worker.halt();
                            verify( machine ).terminate();
                            worker.enqueue( machine4 -> fail( "Should not be executed" ) );
                        } ) ) );

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void runDoesNothingAfterHalt()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        MutableBoolean jobWasExecuted = new MutableBoolean();
        worker.enqueue( machine1 ->
        {
            jobWasExecuted.setTrue();
            fail( "Should not be executed" );
        } );

        worker.halt();
        worker.run();

        assertFalse( jobWasExecuted.booleanValue() );
        verify( machine ).close();
    }
}
