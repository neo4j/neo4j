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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.error.state.InternalStateTransitionException;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.fsm.error.AuthenticationStateTransitionException;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.connection.RouteMessage;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.dbms.routing.RoutingException;
import org.neo4j.dbms.routing.RoutingResult;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.kernel.api.exceptions.Status.General;
import org.neo4j.kernel.api.exceptions.Status.Request;
import org.neo4j.values.virtual.MapValue;

class RouteStateTransitionTest extends AbstractStateTransitionTest<RouteMessage, RouteStateTransition> {

    private Connector connector;
    private RoutingService routingService;

    @Override
    @BeforeEach
    protected void prepareContext() throws Exception {
        super.prepareContext();

        this.connector = Mockito.mock(Connector.class);
        this.routingService = Mockito.mock(RoutingService.class);

        var impersonationCaptor = new AtomicReference<String>();
        Mockito.doAnswer(invocation -> {
                    var impersonatedUser = impersonationCaptor.get();
                    if (impersonatedUser == null) {
                        return "neo4j";
                    }

                    return impersonatedUser;
                })
                .when(this.connection)
                .username();
        Mockito.doAnswer(invocation -> {
                    impersonationCaptor.set(invocation.getArgument(0));
                    return null;
                })
                .when(this.connection)
                .impersonate(Mockito.anyString());
        Mockito.doAnswer(invocation -> {
                    impersonationCaptor.set(null);
                    return null;
                })
                .when(this.connection)
                .clearImpersonation();

        Mockito.doReturn(this.connector).when(this.connection).connector();
        Mockito.doReturn(this.routingService).when(this.connector).routingService();

        var routingTable = this.createRoutingTable();

        Mockito.doReturn(routingTable)
                .when(this.routingService)
                .route(Mockito.anyString(), Mockito.any(), Mockito.notNull());
    }

    @Override
    protected RouteStateTransition getTransition() {
        return RouteStateTransition.getInstance();
    }

    @TestFactory
    Stream<DynamicTest> shouldProcessMessage() {
        return Stream.of(Collections.<String>emptyList(), List.of("bookmark-1234"))
                .flatMap(bookmarks -> Stream.of(null, "neo4j", "foo").flatMap(db -> Stream.of(null, "bob")
                        .map(impersonatedUser -> new TestParameters(bookmarks, db, impersonatedUser))))
                .map(parameters -> DynamicTest.dynamicTest(parameters.toString(), () -> {
                    this.prepareContext();

                    var username = parameters.impersonatedUser;
                    if (username == null) {
                        username = "neo4j";
                    }

                    var databaseName = parameters.databaseName;
                    if (databaseName == null) {
                        databaseName = "neo4j";
                    }

                    var request = new RouteMessage(
                            MapValue.EMPTY, List.of(), parameters.databaseName, parameters.impersonatedUser);

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(this.initialState());

                    Mockito.verify(this.routingService).route(databaseName, username, request.getRequestContext());
                    Mockito.verify(this.responseHandler).onRoutingTable(Mockito.eq(databaseName), Mockito.notNull());
                }));
    }

    @Test
    void shouldFailWithInternalStateTransitionExceptionOnGenericGetterError() throws RoutingException {
        var request = new RouteMessage(MapValue.EMPTY, List.of(), "databaseName", null);

        Mockito.doThrow(new RoutingException(General.UnknownError, "Something went wrong!"))
                .when(this.routingService)
                .route(Mockito.any(), Mockito.any(), Mockito.any());

        Assertions.assertThatExceptionOfType(InternalStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Failed to retrieve routing table")
                .withCauseInstanceOf(RoutingException.class);
    }

    @Test
    void shouldFailWithAuthenticationStateTransitionExceptionOnAuthenticationError() throws AuthenticationException {
        Mockito.doThrow(new AuthenticationException(Request.Invalid, "Something went wrong"))
                .when(this.connection)
                .impersonate("bob");

        var request = new RouteMessage(MapValue.EMPTY, List.of(), "databaseName", "bob");

        Assertions.assertThatExceptionOfType(AuthenticationStateTransitionException.class)
                .isThrownBy(() -> this.transition.process(this.context, request, this.responseHandler))
                .withMessage("Something went wrong")
                .withCauseInstanceOf(AuthenticationException.class);
    }

    private RoutingResult createRoutingTable() {
        return new RoutingResult(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 300);
    }

    private record TestParameters(List<String> bookmarks, String databaseName, String impersonatedUser) {}
}
