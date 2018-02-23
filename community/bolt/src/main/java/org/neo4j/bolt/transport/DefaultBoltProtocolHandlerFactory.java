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
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.runtime.BoltChannelAutoReadLimiter;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.bolt.v1.transport.BoltMessagingProtocolHandlerImpl;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

public class DefaultBoltProtocolHandlerFactory implements BoltProtocolHandlerFactory
{
    private final WorkerFactory workerFactory;
    private final TransportThrottleGroup throttleGroup;
    private final LogService logService;

    public DefaultBoltProtocolHandlerFactory( WorkerFactory workerFactory, TransportThrottleGroup throttleGroup,
            LogService logService )
    {
        this.workerFactory = workerFactory;
        this.throttleGroup = throttleGroup;
        this.logService = logService;
    }

    @Override
    public BoltMessagingProtocolHandler create( long protocolVersion, BoltChannel channel )
    {
        if ( protocolVersion == Neo4jPackV1.VERSION )
        {
            return newMessagingProtocolHandler( channel, new Neo4jPackV1() );
        }
        else if ( protocolVersion == Neo4jPackV2.VERSION )
        {
            return newMessagingProtocolHandler( channel, new Neo4jPackV2() );
        }
        else
        {
            return null;
        }
    }

    private BoltMessagingProtocolHandler newMessagingProtocolHandler( BoltChannel channel, Neo4jPack neo4jPack )
    {
        return new BoltMessagingProtocolHandlerImpl( channel, newBoltWorker( channel ), neo4jPack, throttleGroup, logService );
    }

    private BoltWorker newBoltWorker( BoltChannel channel )
    {
        Log log = logService.getInternalLog( BoltChannelAutoReadLimiter.class );
        BoltChannelAutoReadLimiter limiter = new BoltChannelAutoReadLimiter( channel.rawChannel(), log );
        return workerFactory.newWorker( channel, limiter );
    }
}
