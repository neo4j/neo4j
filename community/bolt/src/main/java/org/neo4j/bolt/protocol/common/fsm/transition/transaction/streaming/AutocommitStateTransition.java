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
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.streaming.AbstractStreamingMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;

public abstract sealed class AutocommitStateTransition<R extends AbstractStreamingMessage>
        extends StreamingStateTransition<R>
        permits AutocommitDiscardStreamingStateTransition, AutocommitPullStreamingStateTransition {

    protected AutocommitStateTransition(Class<R> requestType) {
        super(requestType);
    }

    @Override
    protected StateReference process(
            Context ctx, Transaction tx, Statement statement, R message, ResponseHandler handler)
            throws StateMachineException {
        super.process(ctx, tx, statement, message, handler);

        // if there are still records remaining within the statement, we will remain within the
        // current state in order to permit retrieving them via future requests
        if (statement.hasRemaining()) {
            return ctx.state();
        }

        // otherwise, we'll attempt to commit the transaction and transition back to ready state
        // automatically
        String bookmark;
        try {
            try {
                bookmark = tx.commit();
            } finally {
                ctx.connection().closeTransaction();
                ctx.connection().clearImpersonation();
            }
        } catch (TransactionException ex) {
            throw new TransactionStateTransitionException(ex);
        }

        handler.onBookmark(bookmark);
        return States.READY;
    }
}
