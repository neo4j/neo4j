/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.net;

import java.io.Serializable;

import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.logging.MessageLogger;

public class LoggingInbound implements Inbound
{
    private final Inbound inbound;
    private final MessageLogger<AdvertisedSocketAddress> messageLogger;
    private final AdvertisedSocketAddress me;

    public LoggingInbound( Inbound inbound, MessageLogger<AdvertisedSocketAddress> messageLogger,
                           AdvertisedSocketAddress me )
    {
        this.inbound = inbound;
        this.messageLogger = messageLogger;
        this.me = me;
    }

    @Override
    public void registerHandler( final MessageHandler handler )
    {
        inbound.registerHandler( new MessageHandler()
        {
            public synchronized void handle( Serializable message )
            {
                messageLogger.log( me, message );
                handler.handle( message );
            }
        } );
    }
}
