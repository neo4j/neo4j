/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.transport;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.BoltMessagingProtocolHandlerImpl;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.kernel.impl.logging.LogService;

public class DefaultBoltProtocolHandlerFactory implements BoltProtocolHandlerFactory
{
    private final BoltConnectionFactory connectionFactory;
    private final TransportThrottleGroup throttleGroup;
    private final LogService logService;

    public DefaultBoltProtocolHandlerFactory( BoltConnectionFactory connectionFactory, TransportThrottleGroup throttleGroup,
            LogService logService )
    {
        this.connectionFactory = connectionFactory;
        this.throttleGroup = throttleGroup;
        this.logService = logService;
    }

    @Override
    public BoltMessagingProtocolHandler create( long protocolVersion, BoltChannel channel )
    {
        if ( protocolVersion == BoltProtocolV1.VERSION )
        {
            return newMessagingProtocolHandler( channel, new Neo4jPackV1() );
        }
        else
        {
            return null;
        }
    }

    private BoltMessagingProtocolHandler newMessagingProtocolHandler( BoltChannel channel, Neo4jPackV1 neo4jPack )
    {
        return new BoltMessagingProtocolHandlerImpl( channel, newBoltConnection( channel ), neo4jPack, throttleGroup, logService );
    }

    private BoltConnection newBoltConnection( BoltChannel channel )
    {
        return connectionFactory.newConnection( channel );
    }
}
