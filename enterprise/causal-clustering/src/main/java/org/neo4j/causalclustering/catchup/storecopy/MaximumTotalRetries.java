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
package org.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;

public class MaximumTotalRetries implements TerminationCondition
{
    private final int maxRetries;
    private final long allowedInBetweenTimeMillis;
    private final Clock clock;
    private int tries;
    private long previousCheck;

    MaximumTotalRetries( int maxRetries, long allowedInBetweenTimeMillis )
    {
        this( maxRetries, allowedInBetweenTimeMillis, Clock.systemUTC() );
    }

    MaximumTotalRetries( int maxRetries, long allowedInBetweenTimeMillis, Clock clock )
    {
        this.clock = clock;
        this.maxRetries = maxRetries;
        this.allowedInBetweenTimeMillis = allowedInBetweenTimeMillis;
        this.previousCheck = 0;
    }

    @Override
    public void assertContinue() throws StoreCopyFailedException
    {
        long currentTime = clock.millis();
        if ( timeHasExpired( previousCheck, currentTime ) )
        {
            tries++;
            previousCheck = currentTime;
        }
        if ( tries >= maxRetries )
        {
            throw new StoreCopyFailedException( "Maximum allowed retries exceeded: " + maxRetries );
        }
    }

    private boolean timeHasExpired( long previousCheck, long currentTime )
    {
        return (currentTime - previousCheck) > allowedInBetweenTimeMillis;
    }
}
