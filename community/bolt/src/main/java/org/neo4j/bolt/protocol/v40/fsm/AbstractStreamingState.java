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

import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.protocol.v40.messaging.request.DiscardMessage;
import org.neo4j.bolt.protocol.v40.messaging.request.PullMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementExecutionException;
import org.neo4j.bolt.tx.error.statement.StatementStreamingException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;

/**
 * When STREAMING, a result is available as a stream of records. These must be PULLed or DISCARDed before any further statements can be executed.
 */
public abstract class AbstractStreamingState extends FailSafeState {
    protected State readyState;

    @Override
    @SuppressWarnings("removal")
    protected State processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        State nextState = null;
        try {
            if (message instanceof PullMessage pullMessage) {
                nextState = processStreamPullResultMessage(pullMessage.statementId(), context, pullMessage.n());
            } else if (message instanceof DiscardMessage discardMessage) {
                nextState =
                        processStreamDiscardResultMessage(discardMessage.statementId(), context, discardMessage.n());
            }
        } catch (StatementExecutionException | StatementStreamingException ex) {
            // in case of statement execution errors, we typically need to unpack the exception in
            // order to find its actual status bearing cause
            var cause = ex.getCause();

            if (!(ex instanceof HasStatus) && cause != null) {
                throw cause;
            } else {
                throw ex;
            }
        }

        // TODO: Remove along with EXIT_STREAMING
        if (nextState != this) {
            context.connection().write(StateSignal.EXIT_STREAMING);
        }

        return nextState;
    }

    public void setReadyState(State readyState) {
        this.readyState = readyState;
    }

    protected State processStreamPullResultMessage(long statementId, StateMachineContext context, long noToPull)
            throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        var tx = context.connection()
                .transaction()
                .orElseThrow(() -> new IllegalStateException("Transaction has already been closed"));

        if (statementId == -1) {
            statementId = tx.latestStatementId();
        }

        // TODO: Status code - this could be caused by a driver bug
        var statement = tx.getStatement(statementId)
                .orElseThrow(() -> new IllegalStateException("Statement has already been closed"));

        return this.processStreamPullResultMessage(tx, statement, context, noToPull);
    }

    protected State processStreamPullResultMessage(
            Transaction tx, Statement statement, StateMachineContext context, long noToPull) throws Throwable {
        var responseHandler = context.connectionState().getResponseHandler();

        try {
            statement.consume(responseHandler, noToPull);
        } finally {
            if (!statement.hasRemaining()) {
                statement.close();
            }
        }
        return this;
    }

    protected State processStreamDiscardResultMessage(long statementId, StateMachineContext context, long noToDiscard)
            throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        var tx = context.connection()
                .transaction()
                .orElseThrow(() -> new IllegalStateException("Transaction has already been closed"));

        if (statementId == -1) {
            statementId = tx.latestStatementId();
        }

        // TODO: Status code - this could be caused by a driver bug
        var statement = tx.getStatement(statementId)
                .orElseThrow(() -> new IllegalStateException("Statement has already been closed"));

        return this.processStreamDiscardResultMessage(tx, statement, context, noToDiscard);
    }

    protected State processStreamDiscardResultMessage(
            Transaction tx, Statement statement, StateMachineContext context, long noToDiscard) throws Throwable {
        var responseHandler = context.connectionState().getResponseHandler();

        try {
            statement.discard(responseHandler, noToDiscard);
        } finally {
            if (!statement.hasRemaining()) {
                statement.close();
            }
        }

        return this;
    }

    protected void commit(StateMachineContext ctx, Transaction tx) throws TransactionException {
        var responseHandler = ctx.connectionState().getResponseHandler();

        Bookmark bookmark;
        try {
            bookmark = tx.commit();
        } finally {
            ctx.connection().closeTransaction();
        }

        bookmark.attachTo(responseHandler);
    }

    protected void rollback(StateMachineContext ctx, Transaction tx) throws TransactionException {
        try {
            tx.rollback();
        } finally {
            ctx.connection().closeTransaction();
        }
    }

    @Override
    protected void assertInitialized() {
        checkState(readyState != null, "Ready state not set");
        super.assertInitialized();
    }
}
