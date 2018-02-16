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
package org.neo4j.causalclustering.discovery;

import org.junit.Test;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiRetryStrategyTest
{
    private static final Predicate<Integer> ALWAYS_VALID = i -> true;
    private static final Predicate<Integer> NEVER_VALID = i -> false;
    private static final Predicate<Integer> VALID_ON_SECOND_TIME = new Predicate<Integer>()
    {
        private boolean nextSuccessful;
        @Override
        public boolean test( Integer integer )
        {
            if ( !nextSuccessful )
            {
                nextSuccessful = true;
                return false;
            }
            return true;
        }
    };

    @Test
    public void successOnRetryCausesNoDelay()
    {
        // given
        CountingSleeper countingSleeper = new CountingSleeper();
        int retries = 10;
        MultiRetryStrategy<Integer,Integer> subject = new MultiRetryStrategy<>( 0, retries, NullLogProvider.getInstance(), countingSleeper );

        // when
        Integer result = subject.apply( 3, Function.identity(), ALWAYS_VALID );

        // then
        assertEquals( 0, countingSleeper.invocationCount() );
        assertEquals( "Function identity should be used to retrieve the expected value", 3, result.intValue() );
    }

    @Test
    public void numberOfIterationsDoesNotExceedMaximum()
    {
        // given
        CountingSleeper countingSleeper = new CountingSleeper();
        int retries = 5;
        MultiRetryStrategy<Integer,Integer> subject = new MultiRetryStrategy<>( 0, retries, NullLogProvider.getInstance(), countingSleeper );

        // when
        subject.apply( 3, Function.identity(), NEVER_VALID );

        // then
        assertEquals( retries, countingSleeper.invocationCount() );
    }

    @Test
    public void successfulRetriesBreakTheRetryLoop()
    {
        CountingSleeper countingSleeper = new CountingSleeper();
        int retries = 5;
        MultiRetryStrategy<Integer,Integer> subject = new MultiRetryStrategy<>( 0, retries, NullLogProvider.getInstance(), countingSleeper );

        // when
        subject.apply( 3, Function.identity(), VALID_ON_SECOND_TIME );

        // then
        assertEquals( 1, countingSleeper.invocationCount() );
    }

    private class CountingSleeper implements LongConsumer
    {
        private int counter;

        @Override
        public void accept( long l )
        {
            counter++;
        }

        public int invocationCount()
        {
            return counter;
        }
    }
}
