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
package org.neo4j.bolt.protocol.common.fsm;

import java.time.Clock;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.memory.MemoryTracker;

/**
 * This state machine oversees the exchange of messages for the Bolt protocol.
 * Central to this are the five active states -- CONNECTED, READY, STREAMING,
 * FAILED and INTERRUPTED -- as well as the transitions between them which
 * correspond to the Bolt protocol request messages INIT, ACK_FAILURE, RESET,
 * RUN, DISCARD_ALL and PULL_ALL. Of particular note is RESET which exhibits
 * dual behaviour in both marking the current query for termination and clearing
 * down the current connection state.
 * <p>
 * To help ensure a secure protocol, any transition not explicitly defined here
 * (i.e. a message sent out of sequence) will result in an immediate failure
 * response and a closed connection.
 */
public abstract class AbstractStateMachine implements StateMachine {
    private final Connection connection;
    private final StateMachineSPI spi;
    protected final MutableConnectionState connectionState;
    private final StateMachineContext context;

    private State state;
    private final State failedState;
    private final State interruptedState;

    public AbstractStateMachine(StateMachineSPI spi, Connection connection, Clock clock) {
        connection.memoryTracker().allocateHeap(StateMachineContextImpl.SHALLOW_SIZE);

        this.connection = connection;
        this.spi = spi;
        this.connectionState = new MutableConnectionState();
        this.context = new StateMachineContextImpl(connection, this, spi, connectionState, clock);

        var states = buildStates(connection.memoryTracker());
        this.state = states.initial;
        this.failedState = states.failed;
        this.interruptedState = states.interrupted;
    }

    @Override
    public Connection connection() {
        return this.connection;
    }

    @Override
    public void process(RequestMessage message, ResponseHandler handler) throws BoltConnectionFatality {
        before(handler);
        try {
            if (message.safeToProcessInAnyState() || connectionState.canProcessMessage()) {
                nextState(message, context);
            }
        } finally {
            after();
        }
    }

    private void before(ResponseHandler handler) throws BoltConnectionFatality {
        // if the connection has transitioned into an interrupted state since processing the last
        // message was processed, we'll immediately transition the state machine to its interrupted
        // state in order to respond appropriately (e.g. respond with IGNORED)
        if (connection.isInterrupted()) {
            this.state = this.interruptedState;
        }

        connectionState.setResponseHandler(handler);
    }

    protected void after() {
        if (connectionState.getResponseHandler() != null) {
            try {
                var pendingError = connectionState.getPendingError();
                if (pendingError != null) {
                    connectionState.getResponseHandler().onFailure(pendingError);
                } else if (connectionState.hasPendingIgnore()) {
                    connectionState.getResponseHandler().onIgnored();
                } else {
                    connectionState.getResponseHandler().onSuccess();
                }

                connectionState.resetPendingFailedAndIgnored();
            } finally {
                connectionState.setResponseHandler(null);
            }
        }
    }

    private void nextState(RequestMessage message, StateMachineContext context) throws BoltConnectionFatality {
        State preState = state;

        state = state.process(message, context);

        if (state == null) {
            String msg =
                    "Message '" + message + "' cannot be handled by a session in the " + preState.name() + " state.";
            fail(Error.fatalFrom(Status.Request.Invalid, msg));
            throw new BoltProtocolBreachFatality(msg);
        }
    }

    @Override
    public void markFailed(Error error) {
        fail(error);
        state = failedState;
    }

    /**
     * When this is invoked, the machine will check whether the related transaction is
     * marked for termination and releasing the related transactional resources.
     */
    @Override
    public void validateTransaction() throws KernelException {
        var tx = this.connection.transaction().orElse(null);
        if (tx == null) {
            return;
        }

        tx.validate().ifPresent(this.connectionState::setPendingTerminationNotice);
    }

    @Override
    public void handleExternalFailure(Error error, ResponseHandler handler) throws BoltConnectionFatality {
        before(handler);
        try {
            if (connectionState.canProcessMessage()) {
                fail(error);
                state = failedState;
            }
        } finally {
            after();
        }
    }

    @Override
    public void handleFailure(Throwable cause, boolean fatal) throws BoltConnectionFatality {
        if (ExceptionUtils.indexOfType(cause, BoltConnectionFatality.class) != -1) {
            fatal = true;
        }

        Error error = fatal ? Error.fatalFrom(cause) : Error.from(cause);
        fail(error);

        if (error.isFatal()) {
            if (ExceptionUtils.indexOfType(cause, AuthorizationExpiredException.class) != -1) {
                throw new BoltConnectionAuthFatality("Failed to process a bolt message", cause);
            }
            if (cause instanceof AuthenticationException) {
                throw new BoltConnectionAuthFatality((AuthenticationException) cause);
            }

            throw new BoltConnectionFatality("Failed to process a bolt message", cause);
        }
    }

    public State state() {
        return state;
    }

    public MutableConnectionState connectionState() {
        return connectionState;
    }

    public StateMachineContext stateMachineContext() {
        return context;
    }

    private void fail(Error error) {
        spi.reportError(error);

        if (state == failedState) {
            connectionState.markIgnored();
        } else {
            connectionState.markFailed(error);
        }
    }

    protected abstract States buildStates(MemoryTracker memoryTracker);

    public record States(State initial, State failed, State interrupted) {}
}
