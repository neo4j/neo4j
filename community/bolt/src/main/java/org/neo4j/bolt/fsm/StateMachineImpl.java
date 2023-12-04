/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.fsm;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Classification.DatabaseError;

import org.neo4j.bolt.fsm.error.ConnectionTerminating;
import org.neo4j.bolt.fsm.error.NoSuchStateException;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.error.state.IllegalRequestParameterException;
import org.neo4j.bolt.fsm.state.State;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

final class StateMachineImpl implements StateMachine, Context {
    private final ConnectionHandle connection;
    private final StateMachineConfiguration configuration;

    private final Log userLog;
    private final Log internalLog;

    private State defaultState;
    private State currentState;

    private boolean failed;
    private volatile boolean interrupted;

    StateMachineImpl(
            ConnectionHandle connection,
            StateMachineConfiguration configuration,
            LogService logging,
            State initialState) {
        this.connection = connection;
        this.configuration = configuration;

        this.userLog = logging.getUserLog(StateMachineImpl.class);
        this.internalLog = logging.getInternalLog(StateMachineImpl.class);

        this.currentState = this.defaultState = initialState;
    }

    @Override
    public ConnectionHandle connection() {
        return this.connection;
    }

    @Override
    public StateMachineConfiguration configuration() {
        return this.configuration;
    }

    @Override
    public StateReference state() {
        return this.currentState.reference();
    }

    @Override
    public State lookup(StateReference reference) throws NoSuchStateException {
        return this.configuration.lookup(reference);
    }

    @Override
    public StateReference defaultState() {
        return this.defaultState.reference();
    }

    @Override
    public void defaultState(StateReference state) throws NoSuchStateException {
        this.defaultState = this.lookup(state);
    }

    @Override
    public boolean hasFailed() {
        return this.failed;
    }

    @Override
    public boolean isInterrupted() {
        return this.interrupted;
    }

    @Override
    public void interrupt() {
        this.interrupted = true;
    }

    @Override
    public void reset() {
        this.failed = false;
        this.interrupted = false;

        this.currentState = this.defaultState;
    }

    @Override
    public boolean validate() {
        var tx = this.connection.transaction().orElse(null);

        if (tx == null) {
            return false;
        }

        // ensure that the transaction remains valid for this operation as it may have been
        // terminated through an administrative command or by timing out in the meantime
        return tx.validate();
    }

    @Override
    @SuppressWarnings("removal") // Removal of isIgnoredWhenFailed - see RequestMessage
    public void process(RequestMessage message, ResponseHandler handler) throws StateMachineException {
        if (this.failed || this.interrupted) {
            if (!message.isIgnoredWhenFailed()) {
                handler.onFailure(Error.from(
                        Request.Invalid,
                        "Message '" + message + "' cannot be handled by session in the "
                                + this.state().name() + " state"));

                throw new IllegalRequestParameterException("Request of type "
                        + message.getClass().getName() + " is not permitted while failed or interrupted");
            }

            handler.onIgnored();
            return;
        }

        try {
            var nextStateReference = this.currentState.process(this, message, handler);
            this.currentState = this.lookup(nextStateReference);

            handler.onSuccess();
        } catch (Throwable ex) {
            this.failed = true;
            var error = Error.from(ex);

            // when dealing with database errors, we'll also generate a log message to provide
            // helpful debug information for server administrators
            if (error.status().code().classification() == DatabaseError) {
                String errorMessage;
                if (error.queryId() != null) {
                    errorMessage = format(
                            "Client triggered an unexpected error [%s]: %s, reference %s, queryId: %s.",
                            error.status().code().serialize(), error.message(), error.reference(), error.queryId());
                } else {
                    errorMessage = format(
                            "Client triggered an unexpected error [%s]: %s, reference %s.",
                            error.status().code().serialize(), error.message(), error.reference());
                }

                this.userLog.error(errorMessage);
                if (error.cause() != null) {
                    this.internalLog.error(errorMessage, error.cause());
                }
            }

            // notify the response handler to generate an appropriate response to the client
            handler.onFailure(error);

            // when an exception indicates that it should lead to connection termination,
            // rethrow it to be handled within the parent context (these are generally log
            // worthy conditions)
            if (error.isFatal()
                    || (ex instanceof ConnectionTerminating terminating && terminating.shouldTerminateConnection())) {
                throw ex;
            }
        }
    }
}
