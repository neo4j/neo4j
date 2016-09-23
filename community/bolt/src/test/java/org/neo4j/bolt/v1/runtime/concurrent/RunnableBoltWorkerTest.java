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
package org.neo4j.bolt.v1.runtime.concurrent;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Mockito.*;
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
        worker.enqueue( RunnableBoltWorker.SHUTDOWN );

        // When
        worker.run();

        // Then
        verify( machine ).run( "Hello, world!", null, null );
        verify( machine ).close();
        verifyNoMoreInteractions( machine );
    }

    @Test
    public void errorThrownDuringExecutionShouldCauseSessionClose() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s -> {
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
        worker.enqueue( s -> {
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
    public void protocolBreachesShouldBeLoggedWithoutStackTraces() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        worker.enqueue( s -> {
            throw new BoltProtocolBreachFatality( "protocol breach fatality" );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
        internalLog.assertExactly( inLog( RunnableBoltWorker.class ).error( "Bolt protocol breach in session " +
                "'test-session'" ) );
        userLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
    }
}
