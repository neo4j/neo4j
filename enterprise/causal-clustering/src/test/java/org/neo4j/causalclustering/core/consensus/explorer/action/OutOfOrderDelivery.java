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
package org.neo4j.causalclustering.core.consensus.explorer.action;

import java.util.LinkedList;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.explorer.ClusterState;
import org.neo4j.causalclustering.identity.MemberId;

public class OutOfOrderDelivery implements Action
{
    private final MemberId member;

    public OutOfOrderDelivery( MemberId member )
    {
        this.member = member;
    }

    @Override
    public ClusterState advance( ClusterState previous )
    {
        ClusterState newClusterState = new ClusterState( previous );
        LinkedList<RaftMessages.RaftMessage> inboundQueue = new LinkedList<>( previous.queues.get( member ) );
        if ( inboundQueue.size() < 2 )
        {
            return previous;
        }
        RaftMessages.RaftMessage message = inboundQueue.poll();
        inboundQueue.add( 1, message );

        newClusterState.queues.put( member, inboundQueue );
        return newClusterState;
    }
}
