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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;

import org.neo4j.test.Race;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.RepeatRule.Repeat;

import static org.junit.Assert.assertEquals;

import static java.lang.Math.max;

public class HighestIdTest
{
    @Rule
    public final RepeatRule repeater = new RepeatRule();

    @Repeat( times = 100 )
    @Test
    public void shouldKeepHighest() throws Throwable
    {
        // GIVEN
        Race race = new Race();
        HighestId highestId = new HighestId();
        int threads = Runtime.getRuntime().availableProcessors();
        CountDownLatch latch = new CountDownLatch( threads );
        AtomicLongArray highestIds = new AtomicLongArray( threads );
        for ( int c = 0; c < threads; c++ )
        {
            int cc = c;
            race.addContestant( new Runnable()
            {
                boolean run;
                ThreadLocalRandom random = ThreadLocalRandom.current();

                @Override
                public void run()
                {
                    if ( run )
                    {
                        return;
                    }

                    long highest = 0;
                    for ( int i = 0; i < 10; i++ )
                    {
                        long nextLong = random.nextLong( 100 );
                        highestId.offer( nextLong );
                        highest = max( highest, nextLong );
                    }
                    highestIds.set( cc, highest );
                    latch.countDown();
                    run = true;
                }
            } );
        }
        race.withEndCondition( () -> latch.getCount() == 0 );

        // WHEN
        race.go();

        long highest = 0;
        for ( int i = 0; i < threads; i++ )
        {
            highest = max( highest, highestIds.get( i ) );
        }
        assertEquals( highest, highestId.get() );
    }
}
