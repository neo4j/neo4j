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
package org.neo4j.bolt.protocol.common.fsm.transition.transaction;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.transaction.CommitMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;

public final class CommitTransactionalStateTransition extends TransactionalStateTransition<CommitMessage> {
    private static final CommitTransactionalStateTransition INSTANCE = new CommitTransactionalStateTransition();

    private CommitTransactionalStateTransition() {
        super(CommitMessage.class);
    }

    public static CommitTransactionalStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    protected StateReference process(Context ctx, Transaction tx, CommitMessage message, ResponseHandler handler)
            throws StateMachineException {
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
