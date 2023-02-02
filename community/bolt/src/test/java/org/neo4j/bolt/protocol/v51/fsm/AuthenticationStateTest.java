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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.protocol.common.fsm.State;
import org.neo4j.bolt.protocol.common.fsm.StateMachineContext;
import org.neo4j.bolt.protocol.v40.messaging.request.HelloMessage;
import org.neo4j.bolt.protocol.v51.message.request.LogonMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.kernel.api.exceptions.Status;

public class AuthenticationStateTest {

    StateMachineContext context;
    Connection connection;
    MutableConnectionState connectionState;

    AuthenticationState stateInTest;

    ReadyState readyState;

    @BeforeEach
    public void prepareState() {
        readyState = mock();
        context = mock();
        connection = mock();

        when(context.connection()).thenReturn(connection);
        when(context.connectionState()).thenReturn(connectionState);

        stateInTest = new AuthenticationState();
        stateInTest.setReadyState(readyState);
        stateInTest.setFailedState(mock());
        stateInTest.setInterruptedState(mock());
    }

    @Test
    public void shouldCallAuthenticateWhenLogonMessageSent() throws BoltConnectionFatality, AuthenticationException {
        var authToken = new HashMap<String, Object>();
        authToken.put("schema", "none");
        LogonMessage message = new LogonMessage(authToken);

        when(connection.logon(authToken)).thenReturn(null);
        State returnedState = stateInTest.process(message, context);

        verify(connection).logon(anyMap());
        assertEquals(readyState, returnedState);
    }

    @Test
    public void shouldReturnNullWhenWrongMessageSent() throws BoltConnectionFatality {
        HelloMessage message = new HelloMessage(Collections.emptyMap());
        State returnedState = stateInTest.process(message, context);

        assertNull(returnedState);
    }

    @Test
    public void shouldCallHandleFailureWithFatalBeingTrueWhenExceptionThrwnOutOfLogon()
            throws BoltConnectionFatality, AuthenticationException {
        LogonMessage message = new LogonMessage(Collections.emptyMap());

        when(connection.logon(anyMap())).thenThrow(new AuthenticationException(Status.Security.Unauthorized));
        State returnedState = stateInTest.process(message, context);

        verify(connection).logon(anyMap());
        verify(context).handleFailure(any(), eq(true));
    }
}
