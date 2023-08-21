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
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;

public final class CreateStatementStateTransition extends TransactionalStateTransition<RunMessage> {
    private static final CreateStatementStateTransition INSTANCE = new CreateStatementStateTransition();

    private CreateStatementStateTransition() {
        super(RunMessage.class);
    }

    public static CreateStatementStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    protected StateReference process(Context ctx, Transaction tx, RunMessage message, ResponseHandler handler)
            throws StateMachineException {
        long start = ctx.clock().millis();
        try {
            var statement = tx.run(message.statement(), message.params());
            long end = ctx.clock().millis();

            // note that we are actively lying to our response handlers here in order to simulate
            // explicit transaction behavior even if an implicit transaction makes use of this
            // state transition in order to support internal Fabric functionality
            handler.onStatementPrepared(TransactionType.EXPLICIT, statement.id(), end - start, statement.fieldNames());
        } catch (TransactionException ex) {
            throw new TransactionStateTransitionException(ex);
        }

        // transactions support multiple statements meaning that we remain within the same state
        return ctx.state();
    }
}
