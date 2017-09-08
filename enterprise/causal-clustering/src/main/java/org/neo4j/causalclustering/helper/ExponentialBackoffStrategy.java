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
package org.neo4j.causalclustering.helper;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Exponential backoff strategy helper class. Exponent is always 2.
 */
public class ExponentialBackoffStrategy implements TimeoutStrategy
{
    private final long initialBackoffTimeMillis;
    private final long upperBoundBackoffTimeMillis;

    public ExponentialBackoffStrategy( long initialBackoffTime, long upperBoundBackoffTime, TimeUnit timeUnit )
    {
        assert initialBackoffTime <= upperBoundBackoffTime;

        this.initialBackoffTimeMillis = timeUnit.toMillis( initialBackoffTime );
        this.upperBoundBackoffTimeMillis = timeUnit.toMillis( upperBoundBackoffTime );
    }

    public ExponentialBackoffStrategy( Duration initialBackoffTime, Duration upperBoundBackoffTime )
    {
        this( initialBackoffTime.toMillis(), upperBoundBackoffTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public Timeout newTimeout()
    {
        return new Timeout()
        {
            private long backoffTimeMillis = initialBackoffTimeMillis;

            @Override
            public long getMillis()
            {
                return backoffTimeMillis;
            }

            @Override
            public void increment()
            {
                backoffTimeMillis = Math.min( backoffTimeMillis * 2, upperBoundBackoffTimeMillis );
            }
        };
    }
}
