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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;

public class OutboundMessageCollector implements Outbound<MemberId, RaftMessages.RaftMessage>
{
    private Map<MemberId, List<RaftMessages.RaftMessage>> sentMessages = new HashMap<>();

    public void clear()
    {
        sentMessages.clear();
    }

    @Override
    public void send( MemberId to, RaftMessages.RaftMessage message, boolean block )
    {
        raftMessages( to ).add( message );
    }

    private List<RaftMessages.RaftMessage> raftMessages( MemberId to )
    {
        return sentMessages.computeIfAbsent( to, k -> new ArrayList<>() );
    }

    public List<RaftMessages.RaftMessage> sentTo( MemberId member )
    {
        List<RaftMessages.RaftMessage> messages = sentMessages.get( member );

        if ( messages == null )
        {
            messages = new ArrayList<>();
        }

        return messages;
    }

    public boolean hasAnyEntriesTo( MemberId member )
    {
        List<RaftMessages.RaftMessage> messages = sentMessages.get( member );
        return messages != null && messages.size() != 0;
    }

    public boolean hasEntriesTo( MemberId member, RaftLogEntry... expectedMessages )
    {
        List<RaftLogEntry> actualMessages = new ArrayList<>();

        for ( Message message : sentTo( member ) )
        {
            if ( message instanceof RaftMessages.AppendEntries.Request )
            {
                Collections.addAll( actualMessages, ((RaftMessages.AppendEntries.Request) message).entries() );
            }
        }

        return actualMessages.containsAll( Arrays.asList( expectedMessages ) );
    }
}
