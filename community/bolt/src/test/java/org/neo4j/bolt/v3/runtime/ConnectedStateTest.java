/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v3.runtime;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.values.storable.StringValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.security.AuthToken.CREDENTIALS;
import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.values.storable.Values.stringValue;

class ConnectedStateTest
{
    @Test
    void shouldAddServerVersionMetadataOnHelloMessage() throws Exception
    {
        // Given
        // hello message
        Map<String,Object> meta = map( "user_agent", "3.0", PRINCIPAL, "neo4j", CREDENTIALS, "password" );
        HelloMessage helloMessage = new HelloMessage( meta );

        // setup state machine
        ConnectedState state = new ConnectedState();
        BoltStateMachineState readyState = mock( BoltStateMachineState.class );

        StateMachineContext context = mock( StateMachineContext.class );
        BoltStateMachineSPI boltSpi = mock( BoltStateMachineSPI.class, RETURNS_MOCKS );
        MutableConnectionState connectionState = new MutableConnectionState();

        state.setReadyState( readyState );

        when( context.boltSpi() ).thenReturn( boltSpi );
        when( context.connectionState() ).thenReturn( connectionState );

        when( boltSpi.version() ).thenReturn( "42.42.42" );
        MutableConnectionState connectionStateMock = mock( MutableConnectionState.class );
        when( context.connectionState() ).thenReturn( connectionStateMock );
        when( context.connectionId() ).thenReturn( "connection-uuid" );

        when( boltSpi.authenticate( meta ) ).thenReturn( AuthenticationResult.AUTH_DISABLED );

        // When
        BoltStateMachineState newState = state.process( helloMessage, context );

        // Then
        assertEquals( readyState, newState );
        verify( connectionStateMock ).onMetadata( "server", stringValue( "42.42.42" ) );
        verify( connectionStateMock ).onMetadata( eq( "connection_id" ), any( StringValue.class ) );
    }
}
