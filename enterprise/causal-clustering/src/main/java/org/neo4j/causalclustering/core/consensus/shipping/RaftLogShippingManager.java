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
package org.neo4j.causalclustering.core.consensus.shipping;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import org.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public class RaftLogShippingManager extends LifecycleAdapter implements RaftMembership.Listener
{
    private final Outbound<MemberId, RaftMessages.RaftMessage> outbound;
    private final LogProvider logProvider;
    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final MemberId myself;

    private final RaftMembership membership;
    private final long retryTimeMillis;
    private final int catchupBatchSize;
    private final int maxAllowedShippingLag;
    private final InFlightCache inFlightCache;

    private Map<MemberId,RaftLogShipper> logShippers = new HashMap<>();
    private LeaderContext lastLeaderContext;

    private boolean running;
    private boolean stopped = false;

    public RaftLogShippingManager( Outbound<MemberId,RaftMessages.RaftMessage> outbound, LogProvider logProvider,
                                   ReadableRaftLog raftLog,
                                   Clock clock, MemberId myself, RaftMembership membership, long retryTimeMillis,
                                   int catchupBatchSize, int maxAllowedShippingLag,
                                   InFlightCache inFlightCache )
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
        this.inFlightCache = inFlightCache;
        membership.registerListener( this );
    }

    /**
     * Paused when stepping down from leader role.
     */
    public synchronized void pause()
    {
        running = false;

        logShippers.values().forEach( RaftLogShipper::stop );
        logShippers.clear();
    }

    /**
     * Resumed when becoming leader.
     */
    public synchronized void resume( LeaderContext initialLeaderContext )
    {
        if ( stopped )
        {
            return;
        }

        running = true;

        for ( MemberId member : membership.replicationMembers() )
        {
            ensureLogShipperRunning( member, initialLeaderContext );
        }

        lastLeaderContext = initialLeaderContext;
    }

    @Override
    public synchronized void stop()
    {
        pause();
        stopped = true;
    }

    private void ensureLogShipperRunning( MemberId member, LeaderContext leaderContext )
    {
        RaftLogShipper logShipper = logShippers.get( member );
        if ( logShipper == null && !member.equals( myself ) )
        {
            logShipper = new RaftLogShipper( outbound, logProvider, raftLog, clock, myself, member,
                    leaderContext.term, leaderContext.commitIndex, retryTimeMillis, catchupBatchSize,
                    maxAllowedShippingLag, inFlightCache );

            logShippers.put( member, logShipper );

            logShipper.start();
        }
    }

    public synchronized void handleCommands( Iterable<ShipCommand> shipCommands, LeaderContext leaderContext ) throws IOException
    {
        for ( ShipCommand shipCommand : shipCommands )
        {
            for ( RaftLogShipper logShipper : logShippers.values() )
            {
                shipCommand.applyTo( logShipper, leaderContext );
            }
        }

        lastLeaderContext = leaderContext;
    }

    @Override
    public synchronized void onMembershipChanged()
    {
        if ( lastLeaderContext == null || !running )
        {
            return;
        }

        HashSet<MemberId> toBeRemoved = new HashSet<>( logShippers.keySet() );
        toBeRemoved.removeAll( membership.replicationMembers() );

        for ( MemberId member : toBeRemoved )
        {
            RaftLogShipper logShipper = logShippers.remove( member );
            if ( logShipper != null )
            {
                logShipper.stop();
            }
        }

        for ( MemberId replicationMember : membership.replicationMembers() )
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
