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
package org.neo4j.causalclustering.messaging.address;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.util.CappedLogger;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class UnknownAddressMonitor
{
    private final Log log;
    private final Clock clock;
    private final long timeLimitMs;
    private Map<MemberId,CappedLogger> loggers = new ConcurrentHashMap<>();

    public UnknownAddressMonitor( Log log, Clock clock, long timeLimitMs )
    {
        this.log = log;
        this.clock = clock;
        this.timeLimitMs = timeLimitMs;
    }

    public void logAttemptToSendToMemberWithNoKnownAddress( MemberId to )
    {
        CappedLogger cappedLogger = loggers.get( to );
        if ( cappedLogger == null )
        {
            cappedLogger = new CappedLogger( log );
            cappedLogger.setTimeLimit( timeLimitMs, MILLISECONDS, clock );
            loggers.put( to, cappedLogger );
        }
        cappedLogger.info(String.format("No address found for %s, probably because the member has been shut down.", to)  );
    }
}
