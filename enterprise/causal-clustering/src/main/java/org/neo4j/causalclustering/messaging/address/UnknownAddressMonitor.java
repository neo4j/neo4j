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
