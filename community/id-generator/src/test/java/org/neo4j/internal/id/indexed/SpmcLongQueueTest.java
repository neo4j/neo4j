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

package org.neo4j.internal.id.indexed;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.scheduler.DaemonThreadFactory;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.LongStream.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_ID;

@ExtendWith( RandomExtension.class )
class SpmcLongQueueTest
{
    @Inject
    private RandomRule random;

    @Test
    void fillAndDrain()
    {
        final SpmcLongQueue queue = new SpmcLongQueue( 4 );
        assertEquals( NO_ID, queue.takeOrDefault( NO_ID ) );
        for ( int i = 0; i < 4; i++ )
        {
            assertTrue( queue.offer( i ) );
        }
        assertFalse( queue.offer( 100 ) );
        for ( int i = 0; i < 4; i++ )
        {
            assertEquals( i, queue.takeOrDefault( NO_ID ) );
        }
        assertEquals( NO_ID, queue.takeOrDefault( NO_ID ) );
    }

    @Test
    void wrapAround()
    {
        final SpmcLongQueue queue = new SpmcLongQueue( 16 );
        for ( int chunk = 1; chunk < 16; chunk++ )
        {
            for ( int i = 0; i < 100; i++ )
            {
                for ( int j = 0; j < chunk; j++ )
                {
                    assertTrue( queue.offer( chunk * 1000 + i * 10 + j ) );
                }
                for ( int j = 0; j < chunk; j++ )
                {
                    assertEquals(chunk * 1000 + i * 10 + j, queue.takeOrDefault( NO_ID ) );
                }
            }
        }
        assertEquals( NO_ID, queue.takeOrDefault( NO_ID ) );
    }

    @Test
    void randomizedConcurrent() throws Exception
    {
        final int consumers = Runtime.getRuntime().availableProcessors() - 1;
        final int itemsPerConsumer = 10000;
        final SpmcLongQueue queue = new SpmcLongQueue( 1 << (32 - numberOfLeadingZeros( consumers * itemsPerConsumer )) );
        final long[] input = range( 0, consumers * itemsPerConsumer ).toArray();
        final long[][] outputs = new long[consumers][itemsPerConsumer];
        ArrayUtils.shuffle( input, random.random() );

        final Collection<Callable<Void>> workers = new ArrayList<>();
        workers.add( createProducer( queue, input ) );
        for ( int consumerId = 0; consumerId < consumers; consumerId++ )
        {
            workers.add( createConsumer( queue, outputs[consumerId] ) );
        }

        final ExecutorService executor = newCachedThreadPool( new DaemonThreadFactory() );
        try
        {
            final List<Future<Void>> futures = executor.invokeAll( workers );
            for ( final Future<Void> future : futures )
            {
                future.get();
            }

            final long[] actual = stream( outputs )
                    .flatMapToLong( LongStream::of )
                    .sorted()
                    .toArray();

            Arrays.sort(input);

            assertArrayEquals( input, actual );
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination( 10, SECONDS );
        }
    }

    @Test
    void shouldClearQueue()
    {
        // given
        ConcurrentLongQueue queue = new SpmcLongQueue( 16 );
        for ( int i = 0; i < 10; i++ )
        {
            queue.offer( random.nextLong( 1000 ) );
        }
        assertEquals( 10, queue.size() );
        assertNotEquals( -1, queue.takeOrDefault( -1 ) );

        // when
        queue.clear();

        // then
        assertEquals( 0, queue.size() );
        assertEquals( -1, queue.takeOrDefault( -1 ) );
    }

    private Callable<Void> createConsumer( SpmcLongQueue queue, long[] output )
    {
        return () ->
        {
            for ( int j = 0; j < output.length; j++ )
            {
                long value;
                while ( (value = queue.takeOrDefault( NO_ID )) == NO_ID )
                {
                    Thread.yield();
                }
                output[j] = value;
            }
            return null;
        };
    }

    private Callable<Void> createProducer( SpmcLongQueue queue, long[] input )
    {
        return () ->
        {
            for ( long value : input )
            {
                while ( !queue.offer( value ) )
                {
                    Thread.yield();
                }
            }
            return null;
        };
    }
}
