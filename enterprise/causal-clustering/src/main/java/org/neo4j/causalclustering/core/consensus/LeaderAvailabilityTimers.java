/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts;
import org.neo4j.causalclustering.core.consensus.schedule.TimeoutHandler;
import org.neo4j.causalclustering.core.consensus.schedule.Timer;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobScheduler.Groups;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.uniformRandomTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.ASYNC;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.SYNC_WAIT;

class LeaderAvailabilityTimers
{
    private final long electionTimeout;
    private final long heartbeatInterval;
    private final Clock clock;
    private final TimerService timerService;
    private final Log log;

    private volatile long lastElectionRenewalMillis;

    private Timer heartbeatTimer;
    private Timer electionTimer;

    LeaderAvailabilityTimers( Duration electionTimeout, Duration heartbeatInterval, Clock clock, TimerService timerService,
            LogProvider logProvider )
    {
        this.electionTimeout = electionTimeout.toMillis();
        this.heartbeatInterval = heartbeatInterval.toMillis();
        this.clock = clock;
        this.timerService = timerService;
        this.log = logProvider.getLog( getClass() );

        if ( this.electionTimeout < this.heartbeatInterval )
        {
            throw new IllegalArgumentException( String.format(
                            "Election timeout %s should not be shorter than heartbeat interval %s", this.electionTimeout, this.heartbeatInterval
            ) );
        }
    }

    synchronized void start( ThrowingAction<Exception> electionAction, ThrowingAction<Exception> heartbeatAction )
    {
        this.electionTimer = timerService.create( Timeouts.ELECTION, Groups.raft, renewing( electionAction) );
        this.electionTimer.set( uniformRandomTimeout( electionTimeout, electionTimeout * 2, MILLISECONDS ) );

        this.heartbeatTimer = timerService.create( Timeouts.HEARTBEAT, Groups.raft, renewing( heartbeatAction ) );
        this.heartbeatTimer.set( fixedTimeout( heartbeatInterval, MILLISECONDS ) );

        lastElectionRenewalMillis = clock.millis();
    }

    synchronized void stop()
    {
        if ( electionTimer != null )
        {
            electionTimer.cancel( ASYNC );
        }
        if ( heartbeatTimer != null )
        {
            heartbeatTimer.cancel( ASYNC );
        }
    }

    synchronized void renewElection()
    {
        lastElectionRenewalMillis = clock.millis();
        if ( electionTimer != null )
        {
            electionTimer.reset();
        }
    }

    synchronized boolean isElectionTimedOut()
    {
        return clock.millis() - lastElectionRenewalMillis >= electionTimeout;
    }

    // Getters for immutable values
    long getElectionTimeout()
    {
        return electionTimeout;
    }

    private TimeoutHandler renewing( ThrowingAction<Exception> action )
    {
        return timeout ->
        {
            try
            {
                action.apply();
            }
            catch ( Exception e )
            {
                log.error( "Failed to process timeout.", e );
            }
            timeout.reset();
        };
    }
}
