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
import org.neo4j.causalclustering.logging.MessageLogger;

public class LoggingOutbound<MEMBER, MESSAGE extends RaftMessages.RaftMessage> implements Outbound<MEMBER, MESSAGE>
{
    private final Outbound<MEMBER,MESSAGE> outbound;
    private final MEMBER me;
    private final MessageLogger<MEMBER> messageLogger;

    public LoggingOutbound( Outbound<MEMBER,MESSAGE> outbound, MEMBER me, MessageLogger<MEMBER> messageLogger )
    {
        this.outbound = outbound;
        this.me = me;
        this.messageLogger = messageLogger;
    }

    @Override
    public void send( MEMBER to, MESSAGE message, boolean block )
    {
        messageLogger.logOutbound( me, message, to );
        outbound.send( to, message );
    }

}
