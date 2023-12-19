/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
