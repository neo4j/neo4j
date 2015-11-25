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
package org.neo4j.coreedge.raft;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.coreedge.server.RaftTestMember;

public class DirectNetworking
{
    private final Map<Long, org.neo4j.coreedge.raft.net.Inbound.MessageHandler> handlers = new HashMap<>();
    private final Map<Long, Queue<Serializable>> messageQueues = new HashMap<>();
    private final Set<Long> disconnectedMembers = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public void processMessages()
    {
        while ( messagesToBeProcessed() )
        {
            for ( Map.Entry<Long, Queue<Serializable>> entry : messageQueues.entrySet() )
            {
                Long id = entry.getKey();
                Queue<Serializable> queue = entry.getValue();
                if ( !queue.isEmpty() )
                {
                    Serializable message = queue.remove();
                    handlers.get( id ).handle( message );
                }
            }
        }
    }

    private boolean messagesToBeProcessed()
    {
        for ( Queue<Serializable> queue : messageQueues.values() )
        {
            if ( !queue.isEmpty() )
            {
                return true;
            }
        }
        return false;
    }

    public void disconnect( long id )
    {
        disconnectedMembers.add( id );
    }

    public void reconnect( long id )
    {
        disconnectedMembers.remove( id );
    }

    public class Outbound implements org.neo4j.coreedge.raft.net.Outbound<RaftTestMember>
    {
        private final long me;

        public Outbound( long me )
        {
            this.me = me;
        }

        @Override
        public synchronized void send( RaftTestMember to, final Serializable... messages )
        {
            if ( !messageQueues.containsKey( to.getId() ) ||
                    disconnectedMembers.contains( to.getId() ) ||
                    disconnectedMembers.contains( me ) )
            {
                return;
            }

            for ( Serializable message : messages )
            {
                messageQueues.get( to.getId() ).add( message );
            }
        }
    }

    public class Inbound implements org.neo4j.coreedge.raft.net.Inbound
    {
        private final long id;

        public Inbound( long id )
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
