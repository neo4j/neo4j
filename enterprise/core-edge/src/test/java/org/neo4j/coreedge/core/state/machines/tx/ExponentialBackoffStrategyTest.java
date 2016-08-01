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
package org.neo4j.coreedge.core.state.machines.tx;

import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.junit.Assert.assertEquals;

public class ExponentialBackoffStrategyTest
{
    private static final int NUMBER_OF_ACCESSES = 5;

    @Test
    public void shouldDoubleEachTime() throws Exception
    {
        // given
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy( 1, MILLISECONDS );
        RetryStrategy.Timeout timeout = strategy.newTimeout();

        // when
        for ( int i = 0; i < NUMBER_OF_ACCESSES; i++ )
        {
            timeout.increment();
        }

        // then
        assertEquals( 2 << NUMBER_OF_ACCESSES - 1, timeout.getMillis() );
    }

    @Test
    public void shouldProvidePreviousTimeout() throws Exception
    {
        // given
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy( 1, MILLISECONDS );
        RetryStrategy.Timeout timeout = strategy.newTimeout();

        // when
        for ( int i = 0; i <= NUMBER_OF_ACCESSES; i++ )
        {
            timeout.increment();
        }

        // then
        assertEquals( 2 << NUMBER_OF_ACCESSES, timeout.getMillis() );
    }
}
