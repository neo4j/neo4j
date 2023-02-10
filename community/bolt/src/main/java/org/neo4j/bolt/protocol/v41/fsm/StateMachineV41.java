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
package org.neo4j.bolt.protocol.v41.fsm;

import java.time.Clock;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.AbstractStateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.v40.fsm.AutoCommitState;
import org.neo4j.bolt.protocol.v40.fsm.FailedState;
import org.neo4j.bolt.protocol.v40.fsm.InTransactionState;
import org.neo4j.bolt.protocol.v40.fsm.InterruptedState;
import org.neo4j.bolt.protocol.v40.fsm.ReadyState;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

public class StateMachineV41 extends AbstractStateMachine {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(StateMachineV41.class);

    public StateMachineV41(StateMachineSPI spi, Connection connection, Clock clock) {
        super(spi, connection, clock);
    }

    @Override
    protected States buildStates(MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(ConnectedState.SHALLOW_SIZE
                + ReadyState.SHALLOW_SIZE
                + AutoCommitState.SHALLOW_SIZE
                + InTransactionState.SHALLOW_SIZE
                + FailedState.SHALLOW_SIZE
                + InterruptedState.SHALLOW_SIZE);

        var connected = new ConnectedState(); // v4.1
        var ready = new ReadyState(); // v4
        var autoCommitState = new AutoCommitState(); // v4
        var inTransaction = new InTransactionState(); // v4
        var failed = new FailedState(); // v4
        var interrupted = new InterruptedState();

        connected.setReadyState(ready);

        ready.setTransactionReadyState(inTransaction);
        ready.setStreamingState(autoCommitState);
        ready.setFailedState(failed);

        autoCommitState.setReadyState(ready);
        autoCommitState.setFailedState(failed);

        inTransaction.setReadyState(ready);
        inTransaction.setFailedState(failed);

        interrupted.setReadyState(ready);

        return new States(connected, failed, interrupted);
    }
}
