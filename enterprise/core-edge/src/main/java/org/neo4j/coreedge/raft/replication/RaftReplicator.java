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
package org.neo4j.coreedge.raft.replication;

import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.net.Outbound;

public class RaftReplicator<MEMBER> implements Replicator
{
    private final LeaderLocator<MEMBER> leaderLocator;
    private final MEMBER me;
    private final Outbound<MEMBER> outbound;

    public RaftReplicator( LeaderLocator<MEMBER> leaderLocator, MEMBER me, Outbound<MEMBER> outbound )
    {
        this.leaderLocator = leaderLocator;
        this.me = me;
        this.outbound = outbound;
    }

    @Override
    public synchronized void replicate( ReplicatedContent content ) throws ReplicationFailedException
    {
        MEMBER leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new ReplicationFailedException( e );
        }

        outbound.send( leader, new RaftMessages.NewEntry.Request<>( me, content ) );
    }
}
