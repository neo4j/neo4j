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
package org.neo4j.causalclustering.core.consensus;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class LeaderAvailabilityTimers
{
    private final long electionTimeout;
    private final long heartbeatInterval;
    private final Clock clock;
    private final RenewableTimeoutService renewableTimeoutService;
    private final Log log;

    private volatile long lastElectionRenewalMillis;

    private RenewableTimeoutService.RenewableTimeout heartbeatTimer;
    private RenewableTimeoutService.RenewableTimeout electionTimer;

    LeaderAvailabilityTimers( Duration electionTimeout, Duration heartbeatInterval, Clock clock, RenewableTimeoutService renewableTimeoutService,
            LogProvider logProvider )
    {
        this.electionTimeout = electionTimeout.toMillis();
        this.heartbeatInterval = heartbeatInterval.toMillis();
        this.clock = clock;
        this.renewableTimeoutService = renewableTimeoutService;
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
        this.electionTimer = renewableTimeoutService.create( RaftMachine.Timeouts.ELECTION, getElectionTimeout(), randomTimeoutRange(),
                renewing( electionAction ) );
        this.heartbeatTimer = renewableTimeoutService.create( RaftMachine.Timeouts.HEARTBEAT, getHeartbeatInterval(), 0,
                renewing( heartbeatAction ) );
        lastElectionRenewalMillis = clock.millis();
    }

    synchronized void stop()
    {
        if ( electionTimer != null )
        {
            electionTimer.cancel();
        }
        if ( heartbeatTimer != null )
        {
            heartbeatTimer.cancel();
        }

    }

    synchronized void renewElection()
    {
        lastElectionRenewalMillis = clock.millis();
        if ( electionTimer != null )
        {
            electionTimer.renew();
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

    long getHeartbeatInterval()
    {
        return heartbeatInterval;
    }

    private long randomTimeoutRange()
    {
        return getElectionTimeout();
    }

    private RenewableTimeoutService.TimeoutHandler renewing( ThrowingAction<Exception> action )
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
            timeout.renew();
        };
    }
}
