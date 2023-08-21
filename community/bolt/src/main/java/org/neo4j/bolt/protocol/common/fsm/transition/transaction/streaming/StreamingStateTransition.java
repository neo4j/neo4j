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
package org.neo4j.bolt.protocol.common.fsm.transition.transaction.streaming;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.error.state.IllegalRequestParameterException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.TransactionalStateTransition;
import org.neo4j.bolt.protocol.common.message.request.streaming.AbstractStreamingMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;

public abstract sealed class StreamingStateTransition<R extends AbstractStreamingMessage>
        extends TransactionalStateTransition<R>
        permits AutocommitStateTransition, DiscardResultsStreamingStateTransition, PullResultsStreamingStateTransition {

    protected StreamingStateTransition(Class<R> requestType) {
        super(requestType);
    }

    @Override
    protected StateReference process(Context ctx, Transaction tx, R message, ResponseHandler handler)
            throws StateMachineException {
        long statementId;
        if (message.statementId() == -1) {
            statementId = tx.latestStatementId();
        } else {
            statementId = message.statementId();
        }

        var statement = tx.getStatement(statementId)
                .orElseThrow(() -> new IllegalRequestParameterException("No such statement: " + statementId));

        return this.process(ctx, tx, statement, message, handler);
    }

    protected StateReference process(
            Context ctx, Transaction tx, Statement statement, R message, ResponseHandler handler)
            throws StateMachineException {
        try {
            this.process(ctx, tx, statement, message.n(), handler);
        } catch (TransactionException ex) {
            throw new TransactionStateTransitionException(ex);
        } finally {
            // when a statement has no more remaining elements, we'll close it immediately as it is
            // no longer of any use to us and would otherwise occupy memory
            if (!statement.hasRemaining()) {
                statement.close();
            }
        }

        return ctx.state();
    }

    protected abstract void process(
            Context ctx, Transaction tx, Statement statement, long noToProcess, ResponseHandler handler)
            throws StateMachineException, TransactionException;
}
