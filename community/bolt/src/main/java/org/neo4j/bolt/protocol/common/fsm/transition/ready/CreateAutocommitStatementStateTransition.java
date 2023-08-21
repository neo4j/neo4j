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
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.protocol.common.signal.StateSignal;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;

/**
 * Handles the creation of auto-commit transactions.
 * <p />
 * This transition will allocate a single transaction and matching statement within the connection
 * context which remain active until closed through a transition from the follow-up state.
 * <p />
 * Transitions to {@link States#AUTO_COMMIT} when successfully executed.
 */
public final class CreateAutocommitStatementStateTransition extends AbstractStateTransition<RunMessage> {
    private static final CreateAutocommitStatementStateTransition INSTANCE =
            new CreateAutocommitStatementStateTransition();

    private CreateAutocommitStatementStateTransition() {
        super(RunMessage.class);
    }

    public static CreateAutocommitStatementStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    @SuppressWarnings("removal")
    public StateReference process(Context ctx, RunMessage message, ResponseHandler handler)
            throws StateMachineException {
        if (message.impersonatedUser() != null) {
            try {
                ctx.connection().impersonate(message.impersonatedUser());
            } catch (AuthenticationException ex) {
                throw new AuthenticationStateTransitionException(ex);
            }
        }

        long start = ctx.clock().millis();
        try {
            var tx = ctx.connection()
                    .beginTransaction(
                            TransactionType.IMPLICIT,
                            message.databaseName(),
                            message.getAccessMode(),
                            message.bookmarks(),
                            message.transactionTimeout(),
                            message.transactionMetadata(),
                            message.notificationsConfig());
            var statement = tx.run(message.statement(), message.params());
            long end = ctx.clock().millis();

            handler.onStatementPrepared(TransactionType.IMPLICIT, statement.id(), end - start, statement.fieldNames());
        } catch (TransactionException ex) {
            throw new TransactionStateTransitionException(ex);
        }

        // TODO: Remove along with ENTER_STREAMING
        ctx.write(StateSignal.ENTER_STREAMING);

        return States.AUTO_COMMIT;
    }
}
