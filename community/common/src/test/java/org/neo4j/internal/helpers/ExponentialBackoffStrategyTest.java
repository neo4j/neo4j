/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.helpers;


import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExponentialBackoffStrategyTest
{
    private static final int NUMBER_OF_ACCESSES = 5;

    @Test
    void shouldDoubleEachTime()
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
    void shouldProvidePreviousTimeout()
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
    void shouldRespectUpperBound()
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
