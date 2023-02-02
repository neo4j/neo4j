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
package org.neo4j.bolt.protocol.v40.fsm;

import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;

public abstract class FailSafeState implements State {
    protected State failedState;
    private State interruptedState;

    @Override
    public State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();

        if (message == Signal.INTERRUPT) {
            return interruptedState;
        }

        try {
            return processUnsafe(message, context);
        } catch (AuthorizationExpiredException | AuthenticationException e) {
            context.handleFailure(e, true);
            return failedState;
        } catch (Throwable t) {
            context.handleFailure(t, false);
            return failedState;
        }
    }

    public void setFailedState(State failedState) {
        this.failedState = failedState;
    }

    public void setInterruptedState(State interruptedState) {
        this.interruptedState = interruptedState;
    }

    protected void assertInitialized() {
        checkState(failedState != null, "Failed state not set");
        checkState(interruptedState != null, "Interrupted state not set");
    }

    protected abstract State processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable;
}
