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

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.error.state.IllegalRequestParameterException;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.AbstractTransactionalTransitionTest;
import org.neo4j.bolt.protocol.common.fsm.transition.transaction.TransactionalStateTransition;
import org.neo4j.bolt.protocol.common.message.request.streaming.AbstractStreamingMessage;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;

abstract class AbstractStreamingStateTransitionTest<
                R extends AbstractStreamingMessage, D extends TransactionalStateTransition<R>>
        extends AbstractTransactionalTransitionTest<R, D> {

    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return LongStream.range(1, 5)
                .boxed()
                .flatMap(statementId -> LongStream.range(-1, 5)
                        .map(n -> {
                            if (n == -1) {
                                return n;
                            }

                            return n * 10;
                        })
                        .boxed()
                        .flatMap(n ->
                                Stream.of(true, false).map(completed -> new TestParameters(statementId, n, completed))))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();

                    Mockito.doReturn(!parameters.completed).when(this.statement).hasRemaining();
                    var request = this.createMessage(parameters.statementId, parameters.n);

                    this.transition.process(this.context, request, this.responseHandler);

                    var inOrder = this.createInOrder();

                    inOrder.verify(this.connection).transaction();
                    if (parameters.statementId != -1) {
                        inOrder.verify(this.transaction).getStatement(parameters.statementId);
                    } else {
                        inOrder.verify(this.transaction).latestStatementId();
                        inOrder.verify(this.transaction).getStatement(42);
                    }

                    this.verifyInteractions(parameters, inOrder);

                    if (parameters.completed) {
                        inOrder.verify(this.statement).close();
                        this.onStatementCompleted(inOrder, parameters);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldFailWithIllegalStateExceptionWhenTransactionDoesNotExist() {
        return LongStream.range(1, 5)
                .boxed()
                .flatMap(statementId -> LongStream.range(-1, 5)
                        .map(n -> {
                            if (n == -1) {
                                return n;
                            }

                            return n * 10;
                        })
                        .boxed()
                        .map(n -> new TestParameters(statementId, n, false)))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();

                    Mockito.doReturn(Optional.empty()).when(this.connection).transaction();

                    var request = this.createMessage(parameters.statementId, parameters.n);

                    Assertions.assertThatExceptionOfType(IllegalStateException.class)
                            .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                            .withMessage("No active transaction within connection")
                            .withNoCause();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldFailWithIllegalRequestParameterExceptionWhenStatementDoesNotExist() {
        return LongStream.range(1, 5)
                .boxed()
                .flatMap(statementId -> LongStream.range(-1, 5)
                        .map(n -> {
                            if (n == -1) {
                                return n;
                            }

                            return n * 10;
                        })
                        .boxed()
                        .map(n -> new TestParameters(statementId, n, false)))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();

                    Mockito.doReturn(Optional.empty()).when(this.transaction).getStatement(Mockito.anyLong());

                    var request = this.createMessage(parameters.statementId, parameters.n);

                    Assertions.assertThatExceptionOfType(IllegalRequestParameterException.class)
                            .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                            .withMessage("No such statement: " + parameters.statementId);
                }));
    }

    protected abstract R createMessage(long statementId, long n);

    protected InOrder createInOrder() {
        return Mockito.inOrder(this.connection, this.transaction, this.statement);
    }

    protected void verifyInteractions(TestParameters parameters, InOrder inOrder) throws StatementException {}

    protected void onStatementCompleted(InOrder inOrder, TestParameters parameters) throws TransactionException {}

    protected record TestParameters(long statementId, long n, boolean completed) {

        @Override
        public String toString() {
            return "statementId=" + statementId + ", n=" + n + ", completed=" + completed;
        }
    }
}
