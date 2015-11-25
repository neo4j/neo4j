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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.net.Outbound;

public class OutboundMessageCollector implements Outbound<RaftTestMember>
{
    Map<RaftTestMember, List<Serializable>> sentMessages = new HashMap<>();

    public void clear()
    {
        sentMessages.clear();
    }

    @Override
    public void send( RaftTestMember to, Serializable... messages )
    {
        List<Serializable> messagesToMember = sentMessages.get( to );
        if ( messagesToMember == null )
        {
            messagesToMember = new ArrayList<>();
            sentMessages.put( to, messagesToMember );
        }

        Collections.addAll( messagesToMember, messages );
    }

    public List<Serializable> sentTo( RaftTestMember member )
    {
        List<Serializable> messages = sentMessages.get( member );

        if ( messages == null )
        {
            messages = new ArrayList<>();
        }

        return messages;
    }

    public boolean hasEntriesTo( RaftTestMember member, RaftLogEntry... expectedMessages )
    {
        List<RaftLogEntry> actualMessages = new ArrayList<>();

        for ( Serializable message : sentTo( member ) )
        {
            if( message instanceof RaftMessages.AppendEntries.Request )
            {
                for ( RaftLogEntry actualEntry : ((RaftMessages.AppendEntries.Request) message).entries() )
                {
                    actualMessages.add( actualEntry );
                }
            }
        }

        return actualMessages.containsAll( Arrays.asList( expectedMessages ) );
    }
}
