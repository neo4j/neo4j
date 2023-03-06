/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.bolt.protocol.v51.fsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.v51.fsm.state.AuthenticationState;
import org.neo4j.bolt.protocol.v51.fsm.state.NegotiationState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.values.storable.Values;

public class NegotiationStateTest {

    private StateMachineContext context;
    private MutableConnectionState connectionState;
    private ConnectionHintProvider connectionHintProvider;

    private NegotiationState unAuthenticatedState;
    private AuthenticationState authenticationState;

    private final String connectionIdString = "12345Conn";
    private final String version = "5.1";

    @BeforeEach
    public void prepareState() {
        authenticationState = mock();
        Connection connection = mock();
        context = mock();
        connectionState = mock();
        StateMachineSPI stateMachineSPI = mock();
        Connector connector = mock();
        connectionHintProvider = mock();

        when(connection.connector()).thenReturn(connector);

        when(context.connection()).thenReturn(connection);
        when(context.connectionId()).thenReturn(connectionIdString);
        when(context.connectionState()).thenReturn(connectionState);
        when(context.boltSpi()).thenReturn(stateMachineSPI);

        when(stateMachineSPI.version()).thenReturn(version);

        when(connector.connectionHintProvider()).thenReturn(connectionHintProvider);

        unAuthenticatedState = new NegotiationState();
        unAuthenticatedState.setAuthenticationState(authenticationState);
    }

    @Test
    public void shouldReturnNullWhenHelloMessageNotSet() throws BoltConnectionFatality {
        LogonMessage logonMessage = new LogonMessage(Collections.emptyMap());
        State returnedState = unAuthenticatedState.process(logonMessage, context);

        assertNull(returnedState);
    }

    @Test
    public void shouldReturnNullWhenNonHelloMessageSend() {
        unAuthenticatedState.setAuthenticationState(null);

        var message = new HelloMessage(
                "SomeAgent/1.0",
                Collections.emptyList(),
                new RoutingContext(false, Collections.emptyMap()),
                Collections.emptyMap());

        assertThrows(IllegalStateException.class, () -> unAuthenticatedState.process(message, context));
    }

    @Test
    public void shouldReturnAuthenticationStateWhenCorrectHelloMessageSend() throws BoltConnectionFatality {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("user_agent", "text/5.1");

        var message = new HelloMessage(
                "SomeAgent/1.0",
                Collections.emptyList(),
                new RoutingContext(false, Collections.emptyMap()),
                Collections.emptyMap());

        State returnedState = unAuthenticatedState.process(message, context);

        // should get the bolt version
        verify(context).boltSpi();

        // should set relevant metadata
        verify(connectionState).onMetadata("connection_id", Values.utf8Value(connectionIdString));
        verify(connectionState).onMetadata("server", Values.utf8Value(version));
        verify(connectionHintProvider).append(any());
        verify(connectionState).onMetadata(eq("hints"), any());

        assertEquals(authenticationState, returnedState);
    }
}
