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
package org.neo4j.bolt.protocol.v40.fsm.state;

import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.fsm.state.AbstractStreamingState;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.CommitMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RollbackMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.memory.HeapEstimator;

public class InTransactionState extends AbstractStreamingState {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(InTransactionState.class);

    @Override
    protected State processUnsafe(RequestMessage message, StateMachineContext context) throws Throwable {
        context.connectionState().ensureNoPendingTerminationNotice();

        if (message == CommitMessage.getInstance()) {
            return processCommitMessage(context);
        }
        if (message == RollbackMessage.getInstance()) {
            return processRollbackMessage(context);
        }

        if (message instanceof RunMessage runMessage) {
            return processRunMessage(runMessage, context);
        }

        return super.processUnsafe(message, context);
    }

    @Override
    public String name() {
        return "IN_TRANSACTION";
    }

    private State processRunMessage(RunMessage message, StateMachineContext context) throws TransactionException {
        context.connectionState().ensureNoPendingTerminationNotice();

        var tx = context.connection()
                .transaction()
                .orElseThrow(() -> new IllegalStateException("Transaction has already been closed"));

        long start = context.clock().millis();
        Statement statement;
        statement = tx.run(message.statement(), message.params());
        long end = context.clock().millis();

        context.connectionState()
                .getResponseHandler()
                .onStatementPrepared(TransactionType.EXPLICIT, statement.id(), end - start, statement.fieldNames());

        return this;
    }

    protected State processCommitMessage(StateMachineContext context)
            throws TransactionException, AuthenticationException {
        context.connectionState().ensureNoPendingTerminationNotice();

        var tx = context.connection()
                .transaction()
                .orElseThrow(() -> new IllegalStateException("Transaction has already been closed"));

        this.commit(context, tx);
        return readyState;
    }

    protected State processRollbackMessage(StateMachineContext context)
            throws TransactionException, AuthenticationException {
        context.connectionState().ensureNoPendingTerminationNotice();

        var tx = context.connection()
                .transaction()
                .orElseThrow(() -> new IllegalStateException("Transaction has already been closed"));

        this.rollback(context, tx);
        return readyState;
    }
}
