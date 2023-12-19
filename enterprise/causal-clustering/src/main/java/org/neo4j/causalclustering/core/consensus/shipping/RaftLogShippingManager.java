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
package org.neo4j.causalclustering.core.consensus.shipping;

import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import org.neo4j.causalclustering.core.consensus.outcome.ShipCommand;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
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
    private boolean stopped;
    private TimerService timerService;

    public RaftLogShippingManager( Outbound<MemberId,RaftMessages.RaftMessage> outbound, LogProvider logProvider,
                                   ReadableRaftLog raftLog, TimerService timerService,
                                   Clock clock, MemberId myself, RaftMembership membership, long retryTimeMillis,
                                   int catchupBatchSize, int maxAllowedShippingLag,
                                   InFlightCache inFlightCache )
    {
        this.outbound = outbound;
        this.logProvider = logProvider;
        this.raftLog = raftLog;
        this.timerService = timerService;
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
            logShipper = new RaftLogShipper( outbound, logProvider, raftLog, clock, timerService, myself, member,
                    leaderContext.term, leaderContext.commitIndex, retryTimeMillis, catchupBatchSize,
                    maxAllowedShippingLag, inFlightCache );

            logShippers.put( member, logShipper );

            logShipper.start();
        }
    }

    public synchronized void handleCommands( Iterable<ShipCommand> shipCommands, LeaderContext leaderContext )
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
