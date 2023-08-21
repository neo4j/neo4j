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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionCreationException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.values.virtual.MapValue;

class CreateAutocommitStatementStateTransitionTest
        extends AbstractStateTransitionTest<RunMessage, CreateAutocommitStatementStateTransition> {

    private Transaction transaction;
    private Statement statement;

    @Override
    protected CreateAutocommitStatementStateTransition getTransition() {
        return CreateAutocommitStatementStateTransition.getInstance();
    }

    @BeforeEach
    void prepareStatement() throws TransactionException {
        this.transaction = Mockito.mock(Transaction.class);
        this.statement = Mockito.mock(Statement.class);

        Mockito.doReturn(this.transaction)
                .when(this.connection)
                .beginTransaction(
                        Mockito.eq(TransactionType.IMPLICIT),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.anyList(),
                        Mockito.any(),
                        Mockito.anyMap(),
                        Mockito.any());

        Mockito.doReturn(this.statement).when(this.transaction).run(Mockito.anyString(), Mockito.any());
    }

    /**
     * Evaluates whether the transition is capable of handling common {@link RunMessage RUN}
     * messages.
     */
    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return Stream.of("RETURN 1", "MATCH (n) RETURN n")
                .flatMap(statement -> Stream.of(Collections.<String>emptyList(), List.of("bookmark-1234"))
                        .flatMap(bookmarks -> Stream.of(null, Duration.ofSeconds(42))
                                .flatMap(timeout -> Stream.of(AccessMode.values())
                                        .flatMap(mode -> Stream.of("neo4j", "foo")
                                                .flatMap(db -> Stream.of(null, "bob")
                                                        .map(impersonatedUser -> new TestParameters(
                                                                statement,
                                                                db,
                                                                mode,
                                                                bookmarks,
                                                                timeout,
                                                                impersonatedUser,
                                                                Collections.emptyMap())))))))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();
                    this.prepareStatement();

                    var request = new RunMessage(
                            parameters.statement,
                            MapValue.EMPTY,
                            Collections.emptyList(),
                            null,
                            AccessMode.WRITE,
                            Collections.emptyMap(),
                            "neo5j",
                            parameters.impersonatedUser,
                            null);

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(States.AUTO_COMMIT);

                    var inOrder =
                            Mockito.inOrder(this.connection, this.transaction, this.statement, this.responseHandler);

                    if (parameters.impersonatedUser != null) {
                        inOrder.verify(this.connection).impersonate(parameters.impersonatedUser);
                    }

                    inOrder.verify(this.connection)
                            .beginTransaction(
                                    TransactionType.IMPLICIT,
                                    request.databaseName(),
                                    request.getAccessMode(),
                                    request.bookmarks(),
                                    request.transactionTimeout(),
                                    request.transactionMetadata(),
                                    null);

                    inOrder.verify(this.transaction).run(parameters.statement, MapValue.EMPTY);

                    inOrder.verify(this.statement).id();
                    inOrder.verify(this.statement).fieldNames();

                    inOrder.verify(this.responseHandler)
                            .onStatementPrepared(
                                    Mockito.eq(TransactionType.IMPLICIT),
                                    Mockito.anyLong(),
                                    Mockito.anyLong(),
                                    Mockito.any());

                    inOrder.verifyNoMoreInteractions();
                }));
    }

    /**
     * Evaluates whether the transition is capable of surfacing authentication errors caused by
     * {@link RunMessage RUN} messages which incorporate impersonation parameters.
     */
    @Test
    void shouldFailWithAuthenticationStateTransitionExceptionOnImpersonationError() throws AuthenticationException {
        Mockito.doThrow(new AuthenticationException(Request.Invalid, "Something went wrong"))
                .when(this.connection)
                .impersonate("bob");

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

        Assertions.assertThatExceptionOfType(AuthenticationStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Something went wrong")
                .withCauseInstanceOf(AuthenticationException.class);
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

        Mockito.doThrow(new TransactionCreationException("Something went wrong"))
                .when(this.connection)
                .beginTransaction(
                        request.type(),
                        request.databaseName(),
                        request.getAccessMode(),
                        request.bookmarks(),
                        request.transactionTimeout(),
                        request.transactionMetadata(),
                        null);

        Assertions.assertThatExceptionOfType(TransactionStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Something went wrong")
                .withCauseInstanceOf(TransactionCreationException.class);
    }

    private record TestParameters(
            String statement,
            String databaseName,
            AccessMode accessMode,
            List<String> bookmarks,
            Duration timeout,
            String impersonatedUser,
            Map<String, Object> metadata) {

        @Override
        public String toString() {
            return "statement='"
                    + statement + '\'' + ", databaseName='"
                    + databaseName + '\'' + ", accessMode="
                    + accessMode + ", bookmarks="
                    + bookmarks + ", timeout="
                    + timeout + ", impersonatedUser='"
                    + impersonatedUser + '\'' + ", metadata="
                    + metadata;
        }
    }
}
