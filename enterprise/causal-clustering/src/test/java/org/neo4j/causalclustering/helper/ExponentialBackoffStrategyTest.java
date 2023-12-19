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

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class ExponentialBackoffStrategyTest
{
    private static final int NUMBER_OF_ACCESSES = 5;

    @Test
    public void shouldDoubleEachTime()
    {
        // given
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy( 1, 1 << NUMBER_OF_ACCESSES, MILLISECONDS );
        TimeoutStrategy.Timeout timeout = strategy.newTimeout();

        // when
        for ( int i = 0; i < NUMBER_OF_ACCESSES; i++ )
        {
            timeout.increment();
        }

        // then
        assertEquals( 1 << NUMBER_OF_ACCESSES, timeout.getMillis() );
    }

    @Test
    public void shouldProvidePreviousTimeout()
    {
        // given
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy( 1, 1 << NUMBER_OF_ACCESSES, MILLISECONDS );
        TimeoutStrategy.Timeout timeout = strategy.newTimeout();

        // when
        for ( int i = 0; i < NUMBER_OF_ACCESSES; i++ )
        {
            timeout.increment();
        }

        // then
        assertEquals( 1 << NUMBER_OF_ACCESSES, timeout.getMillis() );
    }

    @Test
    public void shouldRespectUpperBound()
    {
        // given
        long upperBound = (1 << NUMBER_OF_ACCESSES) - 5;
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy( 1, upperBound, MILLISECONDS );
        TimeoutStrategy.Timeout timeout = strategy.newTimeout();

        // when
        for ( int i = 0; i < NUMBER_OF_ACCESSES; i++ )
        {
            timeout.increment();
        }

        assertEquals( upperBound, timeout.getMillis() );

        // additional increments
        timeout.increment();
        timeout.increment();
        timeout.increment();

        // then
        assertEquals( upperBound, timeout.getMillis() );
    }
}
