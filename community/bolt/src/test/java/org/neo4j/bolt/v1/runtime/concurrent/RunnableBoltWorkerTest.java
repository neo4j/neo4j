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

import org.junit.Test;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.kernel.impl.logging.NullLogService;

import static org.mockito.Mockito.*;

public class RunnableBoltWorkerTest
{
    @Test
    public void shouldExecuteWorkWhenRun() throws Throwable
    {
        // Given
        BoltStateMachine machine = mock( BoltStateMachine.class );
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
        BoltStateMachine machine = mock( BoltStateMachine.class );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s -> {
            throw new RuntimeException( "It didn't work out." );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
    }
}
