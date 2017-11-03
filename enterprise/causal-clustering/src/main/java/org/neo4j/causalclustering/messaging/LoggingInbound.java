/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.logging.MessageLogger;

public class LoggingInbound<M extends RaftMessages.RaftMessage> implements Inbound<M>
{
    private final Inbound<M> inbound;
    private final MessageLogger<MemberId> messageLogger;
    private final MemberId me;

    public LoggingInbound( Inbound<M> inbound, MessageLogger<MemberId> messageLogger, MemberId me )
    {
        this.inbound = inbound;
        this.messageLogger = messageLogger;
        this.me = me;
    }

    @Override
    public void registerHandler( final MessageHandler<M> handler )
    {
        inbound.registerHandler( new MessageHandler<M>()
        {
            public synchronized void handle( M message )
            {
                messageLogger.logInbound( message.from(), message, me );
                handler.handle( message );
            }
        } );
    }
}
