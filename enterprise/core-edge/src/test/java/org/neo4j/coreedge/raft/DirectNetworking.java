/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft;

import org.neo4j.coreedge.network.Message;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.server.CoreMember;

public class DirectNetworking
{
    private final Map<CoreMember, org.neo4j.coreedge.raft.net.Inbound.MessageHandler> handlers = new HashMap<>();
    private final Map<CoreMember, Queue<Message>> messageQueues = new HashMap<>();
    private final Set<CoreMember> disconnectedMembers = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public void processMessages()
    {
        while ( messagesToBeProcessed() )
        {
            for ( Map.Entry<CoreMember, Queue<Message>> entry : messageQueues.entrySet() )
            {
                CoreMember id = entry.getKey();
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

    public void disconnect( CoreMember id )
    {
        disconnectedMembers.add( id );
    }

    public void reconnect( CoreMember id )
    {
        disconnectedMembers.remove( id );
    }

    public class Outbound implements
            org.neo4j.coreedge.raft.net.Outbound<CoreMember, RaftMessages.RaftMessage>
    {
        private final CoreMember me;

        public Outbound( CoreMember me )
        {
            this.me = me;
        }

        @Override
        public synchronized void send( CoreMember to, final RaftMessages.RaftMessage message )
        {
            if ( canDeliver( to ) )
            {
                messageQueues.get( to ).add( message );
            }
        }

        @Override
        public void send( CoreMember to, Collection<RaftMessages.RaftMessage> messages )
        {
            if ( canDeliver( to ) )
            {
                messageQueues.get( to ).addAll( messages );
            }
        }

        private boolean canDeliver( CoreMember to )
        {
            return messageQueues.containsKey( to ) &&
                    !disconnectedMembers.contains( to ) &&
                    !disconnectedMembers.contains( me );
        }
    }

    public class Inbound implements org.neo4j.coreedge.raft.net.Inbound
    {
        private final CoreMember id;

        public Inbound( CoreMember id )
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
