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

import java.time.Clock;
import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts;
import org.neo4j.causalclustering.core.consensus.schedule.TimeoutHandler;
import org.neo4j.causalclustering.core.consensus.schedule.Timer;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler.Groups;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.fixedTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.TimeoutFactory.uniformRandomTimeout;
import static org.neo4j.causalclustering.core.consensus.schedule.Timer.CancelMode.ASYNC;

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

    synchronized void start( ThrowingConsumer<Clock, Exception> electionAction, ThrowingConsumer<Clock, Exception> heartbeatAction )
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

    private TimeoutHandler renewing( ThrowingConsumer<Clock, Exception> action )
    {
        return timeout ->
        {
            try
            {
                action.accept( clock );
            }
            catch ( Exception e )
            {
                log.error( "Failed to process timeout.", e );
            }
            timeout.reset();
        };
    }
}
