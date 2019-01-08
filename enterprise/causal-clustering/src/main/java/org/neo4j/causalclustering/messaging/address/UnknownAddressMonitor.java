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
