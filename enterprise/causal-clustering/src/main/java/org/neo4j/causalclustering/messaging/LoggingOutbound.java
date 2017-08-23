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

import org.neo4j.causalclustering.logging.MessageLogger;

public class LoggingOutbound<MEMBER, MESSAGE extends Message> implements Outbound<MEMBER, MESSAGE>
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
        messageLogger.log( me, to, message );
        outbound.send( to, message );
    }

}
