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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.error.TransactionStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionCreationException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.kernel.api.exceptions.Status.Request;

class CreateTransactionalStateTransitionTest
        extends AbstractStateTransitionTest<BeginMessage, CreateTransactionStateTransition> {

    @Override
    protected CreateTransactionStateTransition getTransition() {
        return CreateTransactionStateTransition.getInstance();
    }

    /**
     * Evaluates whether the transition is capable of handling common {@link BeginMessage BEGIN}
     * messages.
     */
    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return Stream.of(TransactionType.values())
                .flatMap(type -> Stream.of("neo4j", "somedb").flatMap(db -> Stream.of(AccessMode.READ, AccessMode.WRITE)
                        .flatMap(accessMode -> Stream.of(Collections.<String>emptyList(), List.of("bookmark-1234"))
                                .flatMap(bookmarks -> Stream.of(null, Duration.ofSeconds(42))
                                        .flatMap(timeout -> Stream.of(null, "bob", "alice")
                                                .map(impersonatedUser -> new TestParameters(
                                                        type,
                                                        db,
                                                        accessMode,
                                                        bookmarks,
                                                        timeout,
                                                        impersonatedUser,
                                                        Collections.emptyMap())))))))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();

                    var request = new BeginMessage(
                            parameters.bookmarks,
                            parameters.timeout,
                            parameters.accessMode,
                            parameters.metadata,
                            parameters.databaseName,
                            parameters.impersonatedUser,
                            parameters.type,
                            null);

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(States.IN_TRANSACTION);

                    var inOrder = Mockito.inOrder(this.connection);

                    if (parameters.impersonatedUser != null) {
                        inOrder.verify(this.connection).impersonate(parameters.impersonatedUser());
                    }

                    inOrder.verify(this.connection)
                            .beginTransaction(
                                    parameters.type,
                                    parameters.databaseName,
                                    parameters.accessMode,
                                    parameters.bookmarks,
                                    parameters.timeout,
                                    parameters.metadata,
                                    null);
                    inOrder.verifyNoMoreInteractions();
                }));
    }

    /**
     * Evaluates whether the transition is capable of surfacing authentication errors caused by
     * {@link BeginMessage BEGIN} messages which incorporate impersonation parameters.
     */
    @Test
    void shouldFailWithAuthenticationStateTransitionExceptionOnImpersonationError() throws AuthenticationException {
        Mockito.doThrow(new AuthenticationException(Request.Invalid, "Something went wrong"))
                .when(this.connection)
                .impersonate("bob");

        var request = new BeginMessage(
                Collections.emptyList(), null, AccessMode.WRITE, Collections.emptyMap(), "neo5j", "bob", null, null);

        Assertions.assertThatExceptionOfType(AuthenticationStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Something went wrong")
                .withCauseInstanceOf(AuthenticationException.class);
    }

    @Test
    void shouldFailWithTransactionStateTransitionExceptionOnTransactionError() throws TransactionException {
        var request = new BeginMessage(
                Collections.emptyList(), null, AccessMode.WRITE, Collections.emptyMap(), "neo5j", "bob", null, null);

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
            TransactionType type,
            String databaseName,
            AccessMode accessMode,
            List<String> bookmarks,
            Duration timeout,
            String impersonatedUser,
            Map<String, Object> metadata) {

        @Override
        public String toString() {
            return "type="
                    + type + ", databaseName='"
                    + databaseName + '\'' + ", accessMode="
                    + accessMode + ", bookmarks="
                    + bookmarks + ", timeout="
                    + timeout + ", impersonatedUser='"
                    + impersonatedUser + '\'' + ", metadata="
                    + metadata;
        }
    }
}
