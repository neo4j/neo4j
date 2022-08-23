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
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.common.MutableConnectionState;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.Signal;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.transaction.CleanUpConnectionContext;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.bolt.transaction.TransactionStatus;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
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
    private final String id;
    private final BoltChannel boltChannel;
    private final StateMachineSPI spi;
    protected DefaultDatabaseResolver defaultDatabaseResolver;
    protected final MutableConnectionState connectionState;
    private final StateMachineContext context;

    private State state;
    private final State failedState;

    public AbstractStateMachine(
            StateMachineSPI spi,
            BoltChannel channel,
            Clock clock,
            DefaultDatabaseResolver defaultDatabaseResolver,
            TransactionManager transactionManager) {
        channel.memoryTracker().allocateHeap(StateMachineContextImpl.SHALLOW_SIZE);

        this.id = channel.id();
        this.boltChannel = channel;
        this.spi = spi;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.connectionState = new MutableConnectionState();
        this.context = new StateMachineContextImpl(
                this, channel, spi, connectionState, clock, defaultDatabaseResolver, transactionManager);

        var states = buildStates(channel.memoryTracker());
        this.state = states.initial;
        this.failedState = states.failed;
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
        if (connectionState.isTerminated()) {
            close();
        } else if (connectionState.isInterrupted()) {
            nextState(Signal.INTERRUPT, context);
        }

        connectionState.setResponseHandler(handler);
    }

    protected void after() {
        if (connectionState.getResponseHandler() != null) {
            try {
                Error pendingError = connectionState.getPendingError();
                if (pendingError != null) {
                    connectionState.markFailed(pendingError);
                }

                if (connectionState.hasPendingIgnore()) {
                    connectionState.markIgnored();
                }

                connectionState.resetPendingFailedAndIgnored();
                connectionState.getResponseHandler().onFinish();
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
     * When this is invoked, the machine will make attempts
     * at interrupting any currently running action,
     * and will then ignore all inbound messages until a {@code RESET}
     * message is received. If this is called multiple times, an equivalent number
     * of reset messages must be received before the SSM goes back to a good state.
     * <p>
     * You can imagine this is as a "call ahead" mechanism used by RESET to
     * cancel any statements ahead of it in line, without compromising the single-
     * threaded processing of messages that the state machine does.
     * <p>
     * This can be used to cancel a long-running statement or transaction.
     */
    @Override
    public void interrupt() {
        connectionState.incrementInterruptCounter();
        if (connectionState.getCurrentTransactionId() != null) {
            transactionManager().interrupt(connectionState.getCurrentTransactionId());
        }
    }

    /**
     * When this is invoked, the machine will check whether the related transaction is
     * marked for termination and releasing the related transactional resources.
     */
    @Override
    public void validateTransaction() throws KernelException {
        var currentTxId = connectionState.getCurrentTransactionId();
        if (currentTxId == null) {
            return;
        }

        var status = transactionManager().transactionStatus(currentTxId);
        if (status.value().equals(TransactionStatus.Value.INTERRUPTED)) {
            connectionState().setPendingTerminationNotice(status.error());

            try {
                transactionManager().rollback(currentTxId);
            } catch (TransactionNotFoundException ignore) {
            }
        }
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
    public boolean isClosed() {
        return connectionState.isClosed();
    }

    @Override
    public void close() {
        try {
            boltChannel.close();
        } finally {
            connectionState.markClosed();
            // However a new transaction may have been created so we must always to reset
            resetTransactionState();
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void markForTermination() {
        /*
         * This is a side-channel call and we should not close anything directly.
         * Just mark the transaction and set isTerminated to true and then the session
         * thread will close down the connection eventually.
         */
        connectionState.markTerminated();
        var txId = connectionState.getCurrentTransactionId();
        if (txId != null) {
            transactionManager().interrupt(txId);
        }
        transactionManager().cleanUp(new CleanUpConnectionContext(context.connectionId()));
    }

    @Override
    public boolean shouldStickOnThread() {
        // Currently, we're doing our best to keep things together
        // We should not switch threads when there's an active statement (executing/streaming)
        // Also, we're currently sticking to the thread when there's an open transaction due to
        // cursor errors we receive when a transaction is picked up by another thread linearly.
        if (connectionState.getCurrentTransactionId() == null) {
            return false;
        } else {
            var transactionState = transactionManager().transactionStatus(connectionState.getCurrentTransactionId());
            return transactionState.value().equals(TransactionStatus.Value.IN_TRANSACTION_OPEN_STATEMENT)
                    || transactionState.value().equals(TransactionStatus.Value.IN_TRANSACTION_NO_OPEN_STATEMENTS);
        }
    }

    @Override
    public boolean hasOpenStatement() {
        if (connectionState.getCurrentTransactionId() == null) {
            return false;
        } else {
            var transactionState = transactionManager().transactionStatus(connectionState.getCurrentTransactionId());
            return transactionState.value().equals(TransactionStatus.Value.IN_TRANSACTION_OPEN_STATEMENT);
        }
    }

    @Override
    public boolean reset() throws BoltConnectionFatality {
        try {
            resetTransactionState();
            return true;
        } catch (Throwable t) {
            handleFailure(t, true);
            return false;
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

    public TransactionManager transactionManager() {
        return context.transactionManager();
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

    private void resetTransactionState() {
        try {
            if (connectionState.getCurrentTransactionId() != null) {
                transactionManager().rollback(connectionState.getCurrentTransactionId());
            }
        } catch (TransactionNotFoundException e) {
            // if the transaction cannot be found then it has already been reset/removed.
        } finally {
            connectionState.clearCurrentTransactionId();
        }
    }

    protected abstract States buildStates(MemoryTracker memoryTracker);

    public record States(State initial, State failed) {}
}
