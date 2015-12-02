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
package org.neo4j.coreedge.raft.replication.shipping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.coreedge.raft.LeaderContext;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.coreedge.raft.outcome.ShipCommand;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.LogProvider;

import static java.lang.String.*;

public class RaftLogShippingManager<MEMBER> implements RaftMembership.Listener
{
    private final Outbound<MEMBER> outbound;
    private final LogProvider logProvider;
    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final MEMBER myself;

    private final RaftMembership<MEMBER> membership;
    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;

    private Map<MEMBER,RaftLogShipper> logShippers = new HashMap<>();
    private LeaderContext lastLeaderContext;

    private boolean running;

    public RaftLogShippingManager( Outbound<MEMBER> outbound, LogProvider logProvider, ReadableRaftLog raftLog,
            Clock clock, MEMBER myself, RaftMembership<MEMBER> membership, long retryTimeMillis,
            int catchupBatchSize, int maxAllowedShippingLag )
    {
        this.outbound = outbound;
        this.logProvider = logProvider;
        this.raftLog = raftLog;
        this.clock = clock;
        this.myself = myself;
        this.membership = membership;
        this.retryTimeMillis = retryTimeMillis;
        this.catchupBatchSize = catchupBatchSize;
        this.maxAllowedShippingLag = maxAllowedShippingLag;

        membership.registerListener( this );
    }

    public synchronized void start( LeaderContext initialLeaderContext )
    {
        running = true;

        for ( MEMBER member : membership.replicationMembers() )
        {
            ensureLogShipperRunning( member, initialLeaderContext );
        }

        lastLeaderContext = initialLeaderContext;
    }

    public synchronized void stop()
    {
        running = false;

        logShippers.values().forEach( RaftLogShipper::stop );
        logShippers.clear();
    }

    private RaftLogShipper ensureLogShipperRunning( MEMBER member, LeaderContext leaderContext )
    {
        RaftLogShipper logShipper = logShippers.get( member );
        if ( logShipper == null && !member.equals( myself ) )
        {
            logShipper = new RaftLogShipper<>( outbound, logProvider, raftLog, clock, myself, member,
                    leaderContext.term, leaderContext.commitIndex, retryTimeMillis, catchupBatchSize, maxAllowedShippingLag );

            logShippers.put( member, logShipper );

            logShipper.start();
        }
        return logShipper;
    }

    public synchronized void handleCommands( Iterable<ShipCommand> shipCommands, LeaderContext leaderContext )
    {
        for ( ShipCommand shipCommand : shipCommands )
        {
            for ( RaftLogShipper logShipper : logShippers.values() )
            {
                try
                {
                    shipCommand.applyTo( logShipper, leaderContext );
                }
                catch ( RaftStorageException e )
                {
                    // TODO: handle
                }
            }
        }

        lastLeaderContext = leaderContext;
    }

    @Override
    public synchronized void onMembershipChanged()
    {
        if ( lastLeaderContext == null || !running )
            return;

        HashSet<MEMBER> toBeRemoved = new HashSet<>( logShippers.keySet() );
        toBeRemoved.removeAll( membership.replicationMembers() );

        for ( MEMBER member : toBeRemoved )
        {
            RaftLogShipper logShipper = logShippers.remove( member );
            if( logShipper != null )
            {
                logShipper.stop();
            }
        }

        for ( MEMBER replicationMember : membership.replicationMembers() )
        {
            ensureLogShipperRunning( replicationMember, lastLeaderContext );
        }
    }

    @Override
    public String toString()
    {
        return format( "RaftLogShippingManager{logShippers=%s, myself=%s}", logShippers, myself );
    }
}
