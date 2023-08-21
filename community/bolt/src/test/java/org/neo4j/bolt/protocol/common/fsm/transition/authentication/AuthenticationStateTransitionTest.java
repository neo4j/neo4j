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
package org.neo4j.bolt.protocol.common.fsm.transition.authentication;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.authentication.AuthenticationMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.values.storable.Values;

class AuthenticationStateTransitionTest
        extends AbstractStateTransitionTest<AuthenticationMessage, AuthenticationStateTransition> {

    @Override
    protected AuthenticationStateTransition getTransition() {
        return AuthenticationStateTransition.getInstance();
    }

    private Stream<AuthenticationMessage> createRequests() {
        return Stream.of(
                        Map.<String, Object>of("scheme", "none"),
                        Map.<String, Object>of(
                                "scheme", "basic", "principal", "bob", "credentials", new byte[] {0x42, 0x21}))
                .flatMap(token -> Stream.of(
                        new HelloMessage(
                                "Test/1.0",
                                List.of(Feature.UTC_DATETIME),
                                new RoutingContext(false, Collections.emptyMap()),
                                token),
                        new LogonMessage(token)));
    }

    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return this.createRequests()
                .map(request -> DynamicTest.dynamicTest(request.toString(), () -> {
                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(States.READY);

                    var inOrder = Mockito.inOrder(this.context, this.connection);

                    inOrder.verify(this.context).connection();
                    inOrder.verify(this.connection).logon(request.authToken());
                    inOrder.verify(this.context).defaultState(States.READY);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldProcessMessageWithExpiredCredentials() {
        return this.createRequests()
                .map(request -> DynamicTest.dynamicTest(request.toString(), () -> {
                    // required since junit does not invoke @BeforeEach / @AfterEach callbacks for
                    // dynamic tests (see https://github.com/junit-team/junit5/issues/694)
                    this.prepareContext();

                    Mockito.doReturn(AuthenticationFlag.CREDENTIALS_EXPIRED)
                            .when(this.connection)
                            .logon(Mockito.anyMap());

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(States.READY);

                    var inOrder = Mockito.inOrder(this.context, this.connection, this.responseHandler);

                    inOrder.verify(this.context).connection();
                    inOrder.verify(this.connection).logon(request.authToken());
                    inOrder.verify(this.responseHandler)
                            .onMetadata(
                                    AuthenticationFlag.CREDENTIALS_EXPIRED
                                            .name()
                                            .toLowerCase(),
                                    Values.TRUE);
                    inOrder.verify(this.context).defaultState(States.READY);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldFailWithAuthenticationStateTransitionExceptionOnAuthenticationError() {
        return this.createRequests()
                .map(request -> DynamicTest.dynamicTest(request.toString(), () -> {
                    // required since junit does not invoke @BeforeEach / @AfterEach callbacks for
                    // dynamic tests (see https://github.com/junit-team/junit5/issues/694)
                    this.prepareContext();

                    Mockito.doThrow(new AuthenticationException(Request.Invalid, "Something went wrong"))
                            .when(this.connection)
                            .logon(Mockito.anyMap());

                    Assertions.assertThatExceptionOfType(AuthenticationStateTransitionException.class)
                            .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                            .withMessage("Something went wrong")
                            .withCauseInstanceOf(AuthenticationException.class);

                    Mockito.verify(this.context).connection();
                    Mockito.verifyNoMoreInteractions(this.context);

                    Mockito.verify(this.connection).logon(request.authToken());
                    Mockito.verifyNoMoreInteractions(this.connection);

                    Mockito.verifyNoInteractions(this.responseHandler);
                }));
    }
}
