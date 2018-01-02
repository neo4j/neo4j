/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.timeout;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * Timeout strategy that allows you to specify per message type what timeout to use
 */
public class MessageTimeoutStrategy
        implements TimeoutStrategy
{
    Map<MessageType, Long> timeouts = new HashMap<MessageType, Long>();

    private TimeoutStrategy delegate;

    public MessageTimeoutStrategy( TimeoutStrategy delegate )
    {
        this.delegate = delegate;
    }

    public MessageTimeoutStrategy timeout( MessageType messageType, long timeout )
    {
        timeouts.put( messageType, timeout );
        return this;
    }

    public MessageTimeoutStrategy relativeTimeout( MessageType messageType, MessageType relativeTo, long timeout )
    {
        timeouts.put( messageType, timeouts.get( relativeTo ) + timeout );
        return this;
    }

    @Override
    public long timeoutFor( Message message )
    {
        Long timeout = timeouts.get( message.getMessageType() );
        if ( timeout == null )
        {
            return delegate.timeoutFor( message );
        }
        else
        {
            return timeout;
        }
    }

    @Override
    public void timeoutTriggered( Message timeoutMessage )
    {
        delegate.timeoutTriggered( timeoutMessage );
    }

    @Override
    public void timeoutCancelled( Message timeoutMessage )
    {
        delegate.timeoutCancelled( timeoutMessage );
    }

    @Override
    public void tick( long now )
    {
        delegate.tick( now );
    }
}
