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
package org.neo4j.bolt.protocol.v44.fsm;

import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.v40.messaging.request.CommitMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RollbackMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.RunMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.memory.HeapEstimator;

/**
 * The FAILED state occurs when a recoverable error is encountered. This might be something like a Cypher SyntaxError or ConstraintViolation. To exit the FAILED
 * state, a RESET must be issued. All stream will be IGNORED until this is done.
 */
public class FailedState implements State {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(FailedState.class);

    private State interruptedState;

    @Override
    public State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();

        if (shouldIgnore(message)) {
            context.connectionState().markIgnored();
            return this;
        }
        if (message == Signal.INTERRUPT) {
            return interruptedState;
        }

        return null;
    }

    public void setInterruptedState(State interruptedState) {
        this.interruptedState = interruptedState;
    }

    protected void assertInitialized() {
        checkState(interruptedState != null, "Interrupted state not set");
    }

    @Override
    public String name() {
        return "FAILED";
    }

    private static boolean shouldIgnore(RequestMessage message) {
        // We assume when a connection is in a FAILED state,
        // the user on the client side should not be allowed to start another transaction (e.g. Session#run or
        // Session#BeginTx).
        // Thus the BEGIN message is not considered to be one of ignored message but an illegal message.
        return message instanceof RunMessage || message instanceof CommitMessage || message instanceof RollbackMessage;
    }
}
