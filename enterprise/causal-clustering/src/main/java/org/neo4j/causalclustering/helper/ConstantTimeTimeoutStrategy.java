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
package org.neo4j.causalclustering.helper;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ConstantTimeTimeoutStrategy implements TimeoutStrategy
{
    private final Timeout constantTimeout;

    public ConstantTimeTimeoutStrategy( long backoffTime, TimeUnit timeUnit )
    {
        long backoffTimeMillis = timeUnit.toMillis( backoffTime );

        constantTimeout = new Timeout()
        {
            @Override
            public long getMillis()
            {
                return backoffTimeMillis;
            }

            @Override
            public void increment()
            {
            }
        };
    }

    public ConstantTimeTimeoutStrategy( Duration backoffTime )
    {
        this( backoffTime.toMillis(), TimeUnit.MILLISECONDS );
    }

    @Override
    public Timeout newTimeout()
    {
        return constantTimeout;
    }
}
