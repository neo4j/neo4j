/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency.checking.full;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.Race;

import static org.junit.Assert.assertEquals;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RecordCheckWorkerTest
{
    @Test
    public void shouldDoInitialProcessingInOrder() throws Throwable
    {
        // GIVEN
        final Race race = new Race();
        final AtomicInteger coordination = new AtomicInteger( -1 );
        final AtomicInteger expected = new AtomicInteger();
        final int threads = 30;
        final RecordCheckWorker<Integer>[] workers = new RecordCheckWorker[threads];
        for ( int i = 0; i < threads; i++ )
        {
            final int id = i;
            ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>( 10 );
            queue.offer( i );
            race.addContestant( workers[i] = new RecordCheckWorker<Integer>( i, coordination, queue )
            {
                private boolean initialized;

                @Override
                protected void process( Integer record )
                {
                    if ( !initialized )
                    {
                        // THEN
                        assertEquals( id, expected.getAndAdd( 1 ) );
                        initialized = true;
                    }
                }
            } );
        }
        race.addContestant( new Runnable()
        {
            @Override
            public void run()
            {
                long end = currentTimeMillis() + SECONDS.toMillis( 10 );
                while ( currentTimeMillis() < end && expected.get() < threads );
                assertEquals( threads, expected.get() );
                for ( RecordCheckWorker<Integer> worker : workers )
                {
                    worker.done();
                }
            }
        } );

        // WHEN
        race.go();
    }
}
