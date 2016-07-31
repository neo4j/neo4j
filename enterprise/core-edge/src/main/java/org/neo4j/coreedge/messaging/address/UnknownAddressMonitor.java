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
import java.util.HashMap;

import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.Log;

public class UnknownAddressMonitor
{
    private final Log log;
    private final Clock clock;
    private final long logThreshold;

    private HashMap<MemberId, Long> throttle = new HashMap<>(  );

    public UnknownAddressMonitor( Log log, Clock clock, long logThresholdMillis )
    {
        this.log = log;
        this.clock = clock;
        this.logThreshold = logThresholdMillis;
    }

    public void logAttemptToSendToMemberWithNoKnownAddress( MemberId to )
    {
        long currentTime = clock.millis();
        Long lastLogged = throttle.get( to );
        if ( lastLogged == null || (currentTime - lastLogged) > logThreshold )
        {
            log.info( "No address found for member %s, probably because the member has been shut down; " +
                    "dropping message.", to );
            throttle.put( to, currentTime );
        }
    }
}
