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
package org.neo4j.coreedge.messaging.address;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.Log;

public class UnknownAddressMonitor
{
    private final Log log;
    private final Clock clock;
    private final long initialTimeoutMs;
    private Map<MemberId,PeriodicLogger> loggers = new ConcurrentHashMap<>();

    public UnknownAddressMonitor( Log log, Clock clock, long initialTimeoutMs )
    {
        this.log = log;
        this.clock = clock;
        this.initialTimeoutMs = initialTimeoutMs;
    }

    public long logAttemptToSendToMemberWithNoKnownAddress( MemberId to )
    {
        PeriodicLogger logger = loggers.get( to );
        if ( logger == null )
        {
            logger = new PeriodicLogger( clock, log );
            loggers.put( to, logger );
        }
        return logger.attemptLog( to );
    }

    private static class PeriodicLogger
    {
        private final Clock clock;
        private final Log log;
        private long numberOfAttemps;
        private final Penalty penalty = new Penalty();

        PeriodicLogger( Clock clock, Log log )
        {
            this.clock = clock;
            this.log = log;
        }

        long attemptLog( MemberId to )
        {
            numberOfAttemps++;

            if ( clock.millis() > penalty.blockedUntil() )
            {
                penalty.cancel();
            }

            if ( shouldLog() )
            {
                log.info( "No address found for member %s, probably because the member has been shut down; " +
                        "dropped %d message(s) over last %d milliseconds", to, numberOfAttemps, clock.millis() -
                        penalty.blockedUntil() );

                numberOfAttemps = 0;
            }

            penalty.increase();

            return penalty.blockedUntil();
        }

        private boolean shouldLog()
        {
            return clock.millis() >= penalty.blockedUntil();
        }
    }

    private static class Penalty
    {
        private static final long MAX_PENALTY = 60 * 1000;
        private long currentPenalty = 0;

        void increase()
        {
            if ( currentPenalty == 0 )
            {
                currentPenalty = 10_000L;
            }
            else
            {
                currentPenalty = currentPenalty * 2;
                if ( currentPenalty > MAX_PENALTY )
                {
                    currentPenalty = MAX_PENALTY;
                }
            }
        }

        long blockedUntil()
        {
            return currentPenalty;
        }

        public void cancel()
        {
            currentPenalty = 0;
        }
    }
}
