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
package org.neo4j.bolt.protocol.common.fsm.state;

import static org.neo4j.util.Preconditions.checkState;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;

public abstract class AbstractState implements State {
    protected State failedState;

    @Override
    public State process(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        assertInitialized();

        try {
            return processUnsafe(message, context);
        } catch (AuthorizationExpiredException | AuthenticationException e) {
            context.handleFailure(e, true);
            return failedState;
        } catch (TransactionException e) {
            if (!(e instanceof HasStatus) && e.getCause() instanceof HasStatus) {
                context.handleFailure(e.getCause(), false);
            } else {
                context.handleFailure(e, false);
            }

            return failedState;
        } catch (Throwable t) {
            context.handleFailure(t, false);
            return failedState;
        }
    }

    public void setFailedState(State failedState) {
        this.failedState = failedState;
    }

    protected void assertInitialized() {
        checkState(failedState != null, "Failed state not set");
    }

    protected abstract State processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable;
}
