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
package org.neo4j.bolt.v3.runtime;

import java.util.Collections;
import java.util.Map;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v3.messaging.BoltAuthenticationHelper.processAuthentication;
import static org.neo4j.util.Preconditions.checkState;

/**
 * Following the socket connection and a small handshake exchange to
 * establish protocol version, the machine begins in the CONNECTED
 * state. The <em>only</em> valid transition from here is through a
 * correctly authorised HELLO into the READY state. Any other action
 * results in disconnection.
 */
public class ConnectedState implements BoltStateMachineState
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( ConnectedState.class );

    private static final String CONNECTION_ID_KEY = "connection_id";

    private final MapValue connectionHints;
    private BoltStateMachineState readyState;

    public ConnectedState( MapValue connectionHints )
    {
        this.connectionHints = connectionHints;
    }

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        assertInitialized();
        if ( message instanceof HelloMessage helloMessage )
        {
            String userAgent = helloMessage.userAgent();
            Map<String,Object> authToken = helloMessage.authToken();

            if ( processAuthentication( userAgent, authToken, context  ) )
            {
                context.initStatementProcessorProvider( new RoutingContext( false, Collections.emptyMap() ) );

                context.connectionState().onMetadata( CONNECTION_ID_KEY, Values.utf8Value( context.connectionId() ) );
                context.connectionState().onMetadata( "hints", connectionHints );
                return readyState;
            }
            else
            {
                return null;
            }
        }
        return null;
    }

    @Override
    public String name()
    {
        return "CONNECTED";
    }

    public void setReadyState( BoltStateMachineState readyState )
    {
        this.readyState = readyState;
    }

    private void assertInitialized()
    {
        checkState( readyState != null, "Ready state not set" );
    }
}
