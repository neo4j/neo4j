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

class ElectionTiming
{
    private final long electionTimeout;
    private final long heartbeatInterval;
    private final Clock clock;

    private volatile long lastElectionRenewalMillis;

    private RenewableTimeoutService.RenewableTimeout heartbeatTimer;
    private RenewableTimeoutService.RenewableTimeout electionTimer;

    ElectionTiming( Duration electionTimeout, Duration heartbeatInterval, Clock clock )
    {
        this.electionTimeout = electionTimeout.toMillis();
        this.heartbeatInterval = heartbeatInterval.toMillis();
        this.clock = clock;
        if ( this.electionTimeout < this.heartbeatInterval )
        {
            throw new IllegalArgumentException( String.format(
                            "Election timeout %s should not be shorter than heartbeat interval %s", this.electionTimeout, this.heartbeatInterval
            ) );
        }
    }

    synchronized void start( RenewableTimeoutService.RenewableTimeout electionTimer, RenewableTimeoutService.RenewableTimeout heartbeatTimer )
    {
        this.electionTimer = electionTimer;
        this.heartbeatTimer = heartbeatTimer;
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
}
