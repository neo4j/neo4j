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

import java.util.Collections;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementExecutionException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.General;
import org.neo4j.kernel.api.exceptions.Status.HasStatus;
import org.neo4j.values.virtual.MapValue;

class CreateStatementStateTransitionTest
        extends AbstractTransactionalTransitionTest<RunMessage, CreateStatementStateTransition> {

    @Override
    protected CreateStatementStateTransition getTransition() {
        return CreateStatementStateTransition.getInstance();
    }

    @Override
    @BeforeEach
    protected void prepareContext() throws Exception {
        this.statement = Mockito.mock(Statement.class);

        super.prepareContext();

        Mockito.doReturn(this.statement).when(this.transaction).run(Mockito.anyString(), Mockito.any());
    }

    @Override
    protected StateReference initialState() {
        return States.IN_TRANSACTION;
    }

    /**
     * Evaluates whether the transition is capable of handling common {@link RunMessage RUN}
     * messages.
     */
    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return Stream.of("RETURN 1", "MATCH (n) RETURN n")
                .map(statement -> DynamicTest.dynamicTest(statement, () -> {
                    this.prepareContext();

                    var request = new RunMessage(statement);

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(States.IN_TRANSACTION);

                    var inOrder =
                            Mockito.inOrder(this.connection, this.transaction, this.statement, this.responseHandler);

                    inOrder.verify(this.transaction).run(statement, MapValue.EMPTY);

                    inOrder.verify(this.statement).id();
                    inOrder.verify(this.statement).fieldNames();

                    inOrder.verify(this.responseHandler)
                            .onStatementPrepared(TransactionType.EXPLICIT, 0, 42, Collections.emptyList());

                    inOrder.verifyNoMoreInteractions();
                }));
    }

    /**
     * Evaluates whether the transition is capable of surfacing transaction errors caused by
     * {@link RunMessage RUN} messages.
     */
    @Test
    void shouldFailWithTransactionStateTransitionExceptionOnTransactionError() throws TransactionException {
        var request = new RunMessage(
                "RETURN 1",
                MapValue.EMPTY,
                Collections.emptyList(),
                null,
                AccessMode.WRITE,
                Collections.emptyMap(),
                "neo5j",
                "bob",
                null);

        Mockito.doThrow(new MockStatementExecutionException())
                .when(this.transaction)
                .run(request.statement(), request.params());

        Assertions.assertThatExceptionOfType(TransactionStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Something went wrong")
                .withCauseInstanceOf(StatementExecutionException.class);
    }

    private class MockStatementExecutionException extends StatementExecutionException implements HasStatus {

        public MockStatementExecutionException() {
            super("Something went wrong");
        }

        @Override
        public Status status() {
            return General.UnknownError;
        }
    }
}
