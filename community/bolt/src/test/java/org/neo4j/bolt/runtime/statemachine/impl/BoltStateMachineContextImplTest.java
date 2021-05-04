/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.runtime.statemachine.impl;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;

class BoltStateMachineContextImplTest
{
    private static final String DB_NAME = "Molly";

    @Test
    void shouldHandleFailure() throws BoltConnectionFatality
    {
        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltStateMachineContextImpl context = newContext( machine, mock( BoltStateMachineSPI.class ) );

        RuntimeException cause = new RuntimeException();
        context.handleFailure( cause, true );

        verify( machine ).handleFailure( cause, true );
    }

    @Test
    void shouldResetMachine() throws BoltConnectionFatality
    {
        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltStateMachineContextImpl context = newContext( machine, mock( BoltStateMachineSPI.class ) );

        context.resetMachine();

        verify( machine ).reset();
    }

    @Test
    void releaseShouldResetTransactionState() throws Throwable
    {
        // Given a context that has a active tx state machine set.
        BoltStateMachine txStateMachine = mock( BoltStateMachine.class );
        BoltStateMachineContextImpl context = newContext( txStateMachine, mock( BoltStateMachineSPI.class ) );
        assertNull( context.connectionState().getCurrentTransactionId() );
        context.connectionState().setCurrentTransactionId( "123" );

        // When
        context.releaseStatementProcessor( "123");

        // Then
        assertNull( context.connectionState().getCurrentTransactionId() );
    }

    private static BoltStateMachineContextImpl newContext( BoltStateMachine machine, BoltStateMachineSPI boltSPI )
    {
        BoltChannel boltChannel = newTestBoltChannel( mock( Channel.class ) );
        return new BoltStateMachineContextImpl( machine, boltChannel, boltSPI, new MutableConnectionState(), Clock.systemUTC(),
                mock( DefaultDatabaseResolver.class ), mock( MemoryTracker.class, RETURNS_MOCKS ), mock( TransactionManager.class ) );
    }
}
