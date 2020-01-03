/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.memory;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import org.neo4j.test.Race;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ThreadSafePeakMemoryAllocationTrackerTest
{
    @Test
    void shouldRegisterConcurrentAllocationsAndDeallocations() throws Throwable
    {
        // given
        ThreadSafePeakMemoryAllocationTracker tracker = new ThreadSafePeakMemoryAllocationTracker( GlobalMemoryTracker.INSTANCE );
        Race race = new Race();
        race.addContestants( 10, () ->
        {
            for ( int i = 1; i < 100; i++ )
            {
                tracker.allocated( i );
                assertThat( tracker.usedDirectMemory(), greaterThan( 0L ) );
            }
            for ( int i = 1; i < 100; i++ )
            {
                assertThat( tracker.usedDirectMemory(), greaterThan( 0L ) );
                tracker.deallocated( i );
            }
        }, 1 );

        // when
        race.go();

        // then
        assertEquals( 0, tracker.usedDirectMemory() );
    }

    @Test
    void shouldRegisterPeakMemoryUsage() throws Throwable
    {
        // given
        ThreadSafePeakMemoryAllocationTracker tracker = new ThreadSafePeakMemoryAllocationTracker( GlobalMemoryTracker.INSTANCE );
        int threads = 200;
        long[] allocations = new long[threads];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long sum = 0;
        for ( int i = 0; i < allocations.length; i++ )
        {
            allocations[i] = random.nextInt( 1, 10_000 );
            sum += allocations[i];
        }

        // when
        Race race = new Race();
        for ( int i = 0; i < threads; i++ )
        {
            int id = i;
            race.addContestant( () -> tracker.allocated( allocations[id] ) );
        }
        race.go();
        long peakAfterAllocation = tracker.peakMemoryUsage();
        LongStream.of( allocations ).forEach( tracker::deallocated );
        long peakAfterDeallocation = tracker.peakMemoryUsage();
        LongStream.of( allocations ).forEach( tracker::allocated );
        tracker.allocated( 10 ); // <-- 10 more than previous peak
        long peakAfterHigherReallocation = tracker.peakMemoryUsage();
        LongStream.of( allocations ).forEach( tracker::deallocated );
        tracker.deallocated( 10 );
        long peakAfterFinalDeallocation = tracker.peakMemoryUsage();

        // then
        assertEquals( sum, peakAfterAllocation );
        assertEquals( sum, peakAfterDeallocation );
        assertEquals( sum + 10, peakAfterHigherReallocation );
        assertEquals( sum + 10, peakAfterFinalDeallocation );
    }
}
