/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v1.runtime.ConnectedState;
import org.neo4j.bolt.v3.messaging.HelloMessage;
import org.neo4j.values.storable.Values;

/**
 * Following the socket connection and a small handshake exchange to
 * establish protocol version, the machine begins in the CONNECTED
 * state. The <em>only</em> valid transition from here is through a
 * correctly authorised HELLO into the READY state. Any other action
 * results in disconnection.
 */
public class ExtraMetaDataConnectedState extends ConnectedState
{
    private static final String ROUTING_TABLE_VALUE = "dbms.cluster.routing.getRoutingTable";
    private static final String ROUTING_TABLE_KEY = "routing_table";
    private static final String CONNECTION_ID_KEY = "connection_id";

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        if ( message instanceof HelloMessage )
        {
            BoltStateMachineState processResult = super.process( message, context );
            context.connectionState().onMetadata( ROUTING_TABLE_KEY, Values.stringValue( ROUTING_TABLE_VALUE ) );
            context.connectionState().onMetadata( CONNECTION_ID_KEY, Values.stringValue( context.connectionId() ) );
            return processResult;
        }
        return null;
    }
}
