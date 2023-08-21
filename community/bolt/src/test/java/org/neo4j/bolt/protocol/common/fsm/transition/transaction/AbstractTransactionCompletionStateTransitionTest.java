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

import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.error.TransactionException;

public abstract class AbstractTransactionCompletionStateTransitionTest<
                R extends RequestMessage, T extends TransactionalStateTransition<R>>
        extends AbstractStateTransitionTest<R, T> {

    protected Transaction transaction;

    @BeforeEach
    void prepareTransaction() {
        this.transaction = Mockito.mock(Transaction.class);

        Mockito.doReturn(Optional.of(this.transaction)).when(this.connection).transaction();
    }

    @Test
    void shouldProcessMessage() throws StateMachineException, TransactionException {
        var request = this.getMessage();

        this.transition.process(this.context, request, this.responseHandler);

        var inOrder = this.createInOrder();

        inOrder.verify(this.connection).transaction();
        this.verifyInteractions(inOrder);
        inOrder.verify(this.connection).closeTransaction();
        this.verifyCompletion(inOrder);
    }

    @Test
    void shouldFailWithIllegalStateExceptionWhenTransactionDoesNotExist() {
        Mockito.doReturn(Optional.empty()).when(this.connection).transaction();

        var request = this.getMessage();

        Assertions.assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("No active transaction within connection")
                .withNoCause();
    }

    protected abstract R getMessage();

    protected InOrder createInOrder() {
        return Mockito.inOrder(this.connection, this.transaction);
    }

    protected abstract void verifyInteractions(InOrder inOrder) throws TransactionException;

    protected void verifyCompletion(InOrder inOrder) throws TransactionException {}
}
