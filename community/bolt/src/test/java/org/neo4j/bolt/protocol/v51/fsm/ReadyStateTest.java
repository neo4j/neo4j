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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogoffMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.bolt.protocol.v51.fsm.state.AuthenticationState;
import org.neo4j.bolt.protocol.v51.fsm.state.ReadyState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;

public class ReadyStateTest {
    ReadyState stateInTest;
    StateMachineContext context;
    AuthenticationState authenticationState;
    Connection connection;

    @BeforeEach
    public void setupState() {
        stateInTest = new ReadyState();
        authenticationState = new AuthenticationState();

        stateInTest.setAuthenticationState(authenticationState);
        stateInTest.setStreamingState(mock());
        stateInTest.setFailedState(mock());
        stateInTest.setTransactionReadyState(mock());

        context = mock();
        connection = mock();
        when(context.connection()).thenReturn(connection);
    }

    @Test
    public void shouldReturnAuthenticationStateWhenLogoffMessageReceived() throws BoltConnectionFatality {
        State returnedState = stateInTest.process(LogoffMessage.getInstance(), context);
        assertEquals(authenticationState, returnedState);
    }

    @Test
    public void shouldReturnNullWhenLogonMessageSent() throws BoltConnectionFatality {
        LogonMessage logonMessage = new LogonMessage(Collections.emptyMap());

        assertNull(stateInTest.process(logonMessage, context));
    }
}
