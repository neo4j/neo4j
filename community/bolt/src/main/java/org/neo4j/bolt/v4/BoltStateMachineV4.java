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
package org.neo4j.bolt.v4;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.runtime.ConnectedState;
import org.neo4j.bolt.v3.runtime.InterruptedState;
import org.neo4j.bolt.v4.runtime.AutoCommitState;
import org.neo4j.bolt.v4.runtime.FailedState;
import org.neo4j.bolt.v4.runtime.InTransactionState;
import org.neo4j.bolt.v4.runtime.ReadyState;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.virtual.MapValue;

public class BoltStateMachineV4 extends AbstractBoltStateMachine
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( BoltStateMachineV3.class );

    public BoltStateMachineV4( BoltStateMachineSPI boltSPI, BoltChannel boltChannel, Clock clock, DefaultDatabaseResolver defaultDatabaseResolver,
                               MapValue connectionHints, MemoryTracker memoryTracker, TransactionManager transactionManager )
    {
        super( boltSPI, boltChannel, clock, defaultDatabaseResolver, connectionHints, memoryTracker, transactionManager );
    }

    @Override
    protected States buildStates( MapValue connectionHints, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap(
                ConnectedState.SHALLOW_SIZE + ReadyState.SHALLOW_SIZE
                + AutoCommitState.SHALLOW_SIZE + InTransactionState.SHALLOW_SIZE
                + FailedState.SHALLOW_SIZE + InterruptedState.SHALLOW_SIZE );

        ConnectedState connected = new ConnectedState( connectionHints );
        ReadyState ready = new ReadyState(); // v4
        AutoCommitState autoCommitState = new AutoCommitState(); // v4
        InTransactionState inTransaction = new InTransactionState(); // v4
        FailedState failed = new FailedState(); // v4
        InterruptedState interrupted = new InterruptedState();

        connected.setReadyState( ready );

        ready.setTransactionReadyState( inTransaction );
        ready.setStreamingState( autoCommitState );
        ready.setFailedState( failed );
        ready.setInterruptedState( interrupted );

        autoCommitState.setReadyState( ready );
        autoCommitState.setFailedState( failed );
        autoCommitState.setInterruptedState( interrupted );

        inTransaction.setReadyState( ready );
        inTransaction.setFailedState( failed );
        inTransaction.setInterruptedState( interrupted );

        failed.setInterruptedState( interrupted );

        interrupted.setReadyState( ready );

        return new States( connected, failed );
    }
}
