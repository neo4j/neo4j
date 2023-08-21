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
package org.neo4j.bolt.protocol.common.fsm.transition.negotiation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.testing.assertions.ListValueAssertions;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.kernel.internal.Version;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

class HelloStateTransitionTest extends AbstractStateTransitionTest<HelloMessage, HelloStateTransition> {

    private Connector connector;
    private ConnectionHintProvider connectionHintProvider;

    @Override
    protected HelloStateTransition getTransition() {
        return HelloStateTransition.getInstance();
    }

    @BeforeEach
    void prepareConnector() {
        this.connector = Mockito.mock(Connector.class);
        this.connectionHintProvider = Mockito.mock(ConnectionHintProvider.class);

        Mockito.doReturn(this.connector).when(this.connection).connector();
        Mockito.doReturn(this.connectionHintProvider).when(this.connector).connectionHintProvider();
    }

    @Test
    void shouldProcessMessage() throws StateMachineException {
        Mockito.doReturn("bolt-42").when(this.connection).id();

        Mockito.doAnswer(invocation -> {
                    MapValueBuilder builder = invocation.getArgument(0);
                    builder.add("someHint", Values.stringValue("42"));
                    return null;
                })
                .when(this.connectionHintProvider)
                .append(Mockito.notNull());

        var request = new HelloMessage(
                "Test/1.0",
                Collections.emptyList(),
                new RoutingContext(true, Map.of("address", "example.org:7687")),
                Map.of("scheme", "none"));

        var targetState = this.transition.process(this.context, request, this.responseHandler);

        Assertions.assertThat(targetState).isEqualTo(States.AUTHENTICATION);

        var inOrder = Mockito.inOrder(
                this.context, this.connection, this.connector, this.responseHandler, this.connectionHintProvider);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection)
                .negotiate(
                        request.features(), request.userAgent(), request.routingContext(), null, request.boltAgent());

        inOrder.verify(this.responseHandler, Mockito.never()).onMetadata(Mockito.eq("patch_bolt"), Mockito.any());

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).id();

        inOrder.verify(this.responseHandler).onMetadata("connection_id", Values.stringValue("bolt-42"));
        inOrder.verify(this.responseHandler)
                .onMetadata("server", Values.stringValue("Neo4j/" + Version.getNeo4jVersion()));

        var hintsCaptor = ArgumentCaptor.forClass(MapValue.class);
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).connector();
        inOrder.verify(this.connectionHintProvider).append(Mockito.notNull());
        inOrder.verify(this.responseHandler).onMetadata(Mockito.eq("hints"), hintsCaptor.capture());

        MapValueAssertions.assertThat(hintsCaptor.getValue())
                .hasSize(1)
                .containsEntry("someHint", Values.stringValue("42"));

        inOrder.verify(this.context).defaultState(States.AUTHENTICATION);
    }

    @Test
    void shouldProcessMessageWithFeatures() throws StateMachineException {
        Mockito.doReturn("bolt-42").when(this.connection).id();
        Mockito.doReturn(List.of(Feature.UTC_DATETIME))
                .when(this.connection)
                .negotiate(Mockito.anyList(), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any());

        Mockito.doAnswer(invocation -> {
                    MapValueBuilder builder = invocation.getArgument(0);
                    builder.add("someHint", Values.stringValue("42"));
                    return null;
                })
                .when(this.connectionHintProvider)
                .append(Mockito.notNull());

        var request = new HelloMessage(
                "Test/1.0",
                List.of(Feature.UTC_DATETIME),
                new RoutingContext(true, Map.of("address", "example.org:7687")),
                Map.of("scheme", "none"));

        var targetState = this.transition.process(this.context, request, this.responseHandler);

        Assertions.assertThat(targetState).isEqualTo(States.AUTHENTICATION);

        var inOrder = Mockito.inOrder(
                this.context, this.connection, this.connector, this.responseHandler, this.connectionHintProvider);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection)
                .negotiate(request.features(), request.userAgent(), request.routingContext(), null, null);

        var featureCaptor = ArgumentCaptor.forClass(ListValue.class);
        inOrder.verify(this.responseHandler).onMetadata(Mockito.eq("patch_bolt"), featureCaptor.capture());

        ListValueAssertions.assertThat(featureCaptor.getValue()).isNotNull().containsOnly(Values.stringValue("utc"));

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).id();

        inOrder.verify(this.responseHandler).onMetadata("connection_id", Values.stringValue("bolt-42"));
        inOrder.verify(this.responseHandler)
                .onMetadata("server", Values.stringValue("Neo4j/" + Version.getNeo4jVersion()));

        var hintsCaptor = ArgumentCaptor.forClass(MapValue.class);
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).connector();
        inOrder.verify(this.connectionHintProvider).append(Mockito.notNull());
        inOrder.verify(this.responseHandler).onMetadata(Mockito.eq("hints"), hintsCaptor.capture());

        MapValueAssertions.assertThat(hintsCaptor.getValue())
                .hasSize(1)
                .containsEntry("someHint", Values.stringValue("42"));

        inOrder.verify(this.context).defaultState(States.AUTHENTICATION);
    }

    @Test
    void shouldProcessMessagesWithAgent() throws StateMachineException {
        Mockito.doReturn("bolt-42").when(this.connection).id();

        Mockito.doAnswer(invocation -> {
                    MapValueBuilder builder = invocation.getArgument(0);
                    builder.add("someHint", Values.stringValue("42"));
                    return null;
                })
                .when(this.connectionHintProvider)
                .append(Mockito.notNull());

        var agent = Map.of(
                "product", "neo4j-bogus/0.0",
                "platform", "Test OS/1.0",
                "language", "Whitespace/0.1.0",
                "language_details", "Why?");

        var request = new HelloMessage(
                "Test/1.0",
                Collections.emptyList(),
                new RoutingContext(true, Map.of("address", "example.org:7687")),
                Map.of("scheme", "none"),
                null,
                agent);

        var targetState = this.transition.process(this.context, request, this.responseHandler);

        Assertions.assertThat(targetState).isEqualTo(States.AUTHENTICATION);

        var inOrder = Mockito.inOrder(
                this.context, this.connection, this.connector, this.responseHandler, this.connectionHintProvider);

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection)
                .negotiate(
                        request.features(), request.userAgent(), request.routingContext(), null, request.boltAgent());

        inOrder.verify(this.responseHandler, Mockito.never()).onMetadata(Mockito.eq("patch_bolt"), Mockito.any());

        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).id();

        inOrder.verify(this.responseHandler).onMetadata("connection_id", Values.stringValue("bolt-42"));
        inOrder.verify(this.responseHandler)
                .onMetadata("server", Values.stringValue("Neo4j/" + Version.getNeo4jVersion()));

        var hintsCaptor = ArgumentCaptor.forClass(MapValue.class);
        inOrder.verify(this.context).connection();
        inOrder.verify(this.connection).connector();
        inOrder.verify(this.connectionHintProvider).append(Mockito.notNull());
        inOrder.verify(this.responseHandler).onMetadata(Mockito.eq("hints"), hintsCaptor.capture());

        MapValueAssertions.assertThat(hintsCaptor.getValue())
                .hasSize(1)
                .containsEntry("someHint", Values.stringValue("42"));

        inOrder.verify(this.context).defaultState(States.AUTHENTICATION);
    }
}
