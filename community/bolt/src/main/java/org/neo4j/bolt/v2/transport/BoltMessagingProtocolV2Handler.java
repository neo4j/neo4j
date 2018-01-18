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
package org.neo4j.bolt.v2.transport;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.transport.BoltMessagingProtocolV1Handler;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.kernel.impl.logging.LogService;

public class BoltMessagingProtocolV2Handler extends BoltMessagingProtocolV1Handler
{
    public static final int VERSION = 2;

    public BoltMessagingProtocolV2Handler( BoltChannel boltChannel, BoltWorker worker,
            TransportThrottleGroup throttleGroup, LogService logging )
    {
        super( boltChannel, worker, new Neo4jPackV2(), throttleGroup, logging );
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
