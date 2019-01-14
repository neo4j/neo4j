/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

    public class Inbound<M extends Message> implements org.neo4j.causalclustering.messaging.Inbound<M>
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
