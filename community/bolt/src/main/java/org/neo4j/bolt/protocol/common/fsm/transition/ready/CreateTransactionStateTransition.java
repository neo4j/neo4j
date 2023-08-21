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
package org.neo4j.bolt.protocol.common.fsm.transition.ready;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.error.TransactionException;

/**
 * Handles the creation of transactions.
 * <p />
 * Transitions to {@link States#IN_TRANSACTION} when executed successfully.
 */
public final class CreateTransactionStateTransition extends AbstractStateTransition<BeginMessage> {
    private static final CreateTransactionStateTransition INSTANCE = new CreateTransactionStateTransition();

    private CreateTransactionStateTransition() {
        super(BeginMessage.class);
    }

    public static CreateTransactionStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, BeginMessage message, ResponseHandler handler)
            throws StateMachineException {
        if (message.impersonatedUser() != null) {
            try {
                ctx.connection().impersonate(message.impersonatedUser());
            } catch (AuthenticationException ex) {
                throw new AuthenticationStateTransitionException(ex);
            }
        }

        try {
            ctx.connection()
                    .beginTransaction(
                            message.type(),
                            message.databaseName(),
                            message.getAccessMode(),
                            message.bookmarks(),
                            message.transactionTimeout(),
                            message.transactionMetadata(),
                            message.notificationsConfig());
        } catch (TransactionException ex) {
            throw new TransactionStateTransitionException(ex);
        }

        return States.IN_TRANSACTION;
    }
}
