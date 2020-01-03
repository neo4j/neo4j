/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import java.util.Map;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v1.messaging.request.InitMessage;

import static org.neo4j.bolt.v1.runtime.BoltAuthenticationHelper.processAuthentication;
import static org.neo4j.util.Preconditions.checkState;

/**
 * Following the socket connection and a small handshake exchange to
 * establish protocol version, the machine begins in the CONNECTED
 * state. The <em>only</em> valid transition from here is through a
 * correctly authorised INIT into the READY state. Any other action
 * results in disconnection.
 */
public class ConnectedState implements BoltStateMachineState
{
    private BoltStateMachineState readyState;
    private BoltStateMachineState failedState;

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        assertInitialized();
        if ( message instanceof InitMessage )
        {
            InitMessage initMessage = (InitMessage) message;
            String userAgent = initMessage.userAgent();
            Map<String,Object> authToken = initMessage.authToken();

            if ( processAuthentication( userAgent, authToken, context ) )
            {
                return readyState;
            }
            else
            {
                return failedState;
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

    public void setFailedState( BoltStateMachineState failedState )
    {
        this.failedState = failedState;
    }

    private void assertInitialized()
    {
        checkState( readyState != null, "Ready state not set" );
        checkState( failedState != null, "Failed state not set" );
    }
}
