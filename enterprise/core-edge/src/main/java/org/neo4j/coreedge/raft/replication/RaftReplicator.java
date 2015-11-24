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
package org.neo4j.coreedge.raft.replication;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderTimeoutException;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;

public class RaftReplicator<MEMBER> implements Replicator, RaftLog.Listener
{
    private final LeaderLocator<MEMBER> leaderLocator;
    private final MEMBER me;
    private final Outbound<MEMBER> outbound;
    private final Set<ReplicatedContentListener> listeners = new CopyOnWriteArraySet<>();
    private boolean contentHasStarted;

    public RaftReplicator( LeaderLocator<MEMBER> leaderLocator, MEMBER me, Outbound<MEMBER> outbound )
    {
        this.leaderLocator = leaderLocator;
        this.me = me;
        this.outbound = outbound;
    }

    @Override
    public synchronized void replicate( ReplicatedContent content ) throws ReplicationFailedException
    {
        contentHasStarted = true;
        MEMBER leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderTimeoutException e )
        {
            throw new ReplicationFailedException( e );
        }

        if ( !leader.equals( me ) )
        {
            throw new ReplicationFailedException( "Only leader is allowed to replicate" );
        }

        outbound.send( me, new RaftMessages.NewEntry.Request<>( me, content ) );
    }

    @Override
    public synchronized void subscribe( ReplicatedContentListener listener )
    {
        if ( contentHasStarted )
        {
            System.out.println( "WARNING: Late subscription: " + listener );
        }
        listeners.add( listener );
    }

    @Override
    public void unsubscribe( ReplicatedContentListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void onAppended( ReplicatedContent content )
    {
    }

    @Override
    public void onCommitted( ReplicatedContent content, long index )
    {
        for ( ReplicatedContentListener listener : listeners )
        {
            listener.onReplicated( content, index );
        }
    }

    @Override
    public void onTruncated( long fromIndex )
    {
    }
}
