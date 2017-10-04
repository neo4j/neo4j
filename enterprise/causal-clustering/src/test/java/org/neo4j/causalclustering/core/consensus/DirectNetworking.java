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
package org.neo4j.causalclustering.core.consensus;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.identity.MemberId;

public class DirectNetworking
{
    private final Map<MemberId, org.neo4j.causalclustering.messaging.Inbound.MessageHandler> handlers = new HashMap<>();
    private final Map<MemberId, Queue<Message>> messageQueues = new HashMap<>();
    private final Set<MemberId> disconnectedMembers = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public void processMessages()
    {
        while ( messagesToBeProcessed() )
        {
            for ( Map.Entry<MemberId, Queue<Message>> entry : messageQueues.entrySet() )
            {
                MemberId id = entry.getKey();
                Queue<Message> queue = entry.getValue();
                if ( !queue.isEmpty() )
                {
                    Message message = queue.remove();
                    handlers.get( id ).handle( message );
                }
            }
        }
    }

    private boolean messagesToBeProcessed()
    {
        for ( Queue<Message> queue : messageQueues.values() )
        {
            if ( !queue.isEmpty() )
            {
                return true;
            }
        }
        return false;
    }

    public void disconnect( MemberId id )
    {
        disconnectedMembers.add( id );
    }

    public void reconnect( MemberId id )
    {
        disconnectedMembers.remove( id );
    }

    public class Outbound implements
            org.neo4j.causalclustering.messaging.Outbound<MemberId, RaftMessages.RaftMessage>
    {
        private final MemberId me;

        public Outbound( MemberId me )
        {
            this.me = me;
        }

        @Override
        public synchronized void send( MemberId to, final RaftMessages.RaftMessage message, boolean block )
        {
            if ( canDeliver( to ) )
            {
                messageQueues.get( to ).add( message );
            }
        }

        private boolean canDeliver( MemberId to )
        {
            return messageQueues.containsKey( to ) &&
                    !disconnectedMembers.contains( to ) &&
                    !disconnectedMembers.contains( me );
        }
    }

    public class Inbound implements org.neo4j.causalclustering.messaging.Inbound
    {
        private final MemberId id;

        public Inbound( MemberId id )
        {
            this.id = id;
        }

        @Override
        public void registerHandler( MessageHandler handler )
        {
            handlers.put( id, handler );
            messageQueues.put( id, new LinkedList<>() );
        }
    }
}
