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
package org.neo4j.internal.batchimport.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class NodeLabelsCacheTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldCacheSmallSetOfLabelsPerNode()
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 5, 4 );
        NodeLabelsCache.Client client = cache.newClient();
        long nodeId = 0;

        // WHEN
        cache.put( nodeId, new long[] {1,2,3} );

        // THEN
        long[] readLabels = cache.get( client, nodeId );
        assertArrayEquals( new long[] {1,2,3}, shrunk( readLabels ) );
    }

    @Test
    void shouldHandleLargeAmountOfLabelsPerNode()
    {
        // GIVEN
        int highLabelId = 1000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, highLabelId );
        NodeLabelsCache.Client client = cache.newClient();
        long nodeId = 0;

        // WHEN
        long[] labels = randomLabels( 200, 1000 );
        cache.put( nodeId, labels );

        // THEN
        long[] readLabels = cache.get( client, nodeId );
        assertArrayEquals( labels, readLabels );
    }

    @Test
    void shouldHandleLabelsForManyNodes()
    {
        // GIVEN a really weird scenario where we have 5000 different labels
        int highLabelId = 1_000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, highLabelId );
        NodeLabelsCache.Client client = cache.newClient();
        int numberOfNodes = 100_000;
        long[][] expectedLabels = new long[numberOfNodes][];
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            long[] labels = randomLabels( random.nextInt( 30 ) + 1, highLabelId );
            expectedLabels[i] = labels;
            cache.put( i, labels );
        }

        // THEN
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            long[] labels = cache.get( client, i );
            assertArrayEquals( expectedLabels[i], shrunk( labels ), "For node " + i );
        }
    }

    @Test
    void shouldEndTargetArrayWithMinusOne()
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 10 );
        NodeLabelsCache.Client client = cache.newClient();
        cache.put( 10, new long[] { 5, 6, 7, 8 } );

        // WHEN
        long[] target = cache.get( client, 10 );
        assertEquals( 5, target[0] );
        assertEquals( 6, target[1] );
        assertEquals( 7, target[2] );
        assertEquals( 8, target[3] );

        // THEN
        assertEquals( -1, target[4] );
    }

    @Test
    void shouldReturnEmptyArrayForNodeWithNoLabelsAndNoLabelsWhatsoever()
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 0 );
        NodeLabelsCache.Client client = cache.newClient();

        // WHEN
        long[] target = cache.get( client, 0 );

        // THEN
        assertEquals( -1, target[0] );
    }

    @Test
    void shouldSupportConcurrentGet() throws Throwable
    {
        // GIVEN
        int highLabelId = 10;
        int numberOfNodes = 100;
        long[][] expectedLabels = new long[numberOfNodes][];
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, highLabelId );
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            cache.put( i, expectedLabels[i] = randomLabels( random.nextInt( 5 ), highLabelId ) );
        }

        // WHEN
        Race getRace = new Race();
        for ( int i = 0; i < 10; i++ )
        {
            getRace.addContestant( new LabelGetter( cache, expectedLabels, numberOfNodes ) );
        }

        // THEN expected labels should be had (asserted in LabelGetter), and no exceptions (propagated by go())
        getRace.go();
    }

    private static class LabelGetter implements Runnable
    {
        private final NodeLabelsCache cache;
        private final long[][] expectedLabels;
        private final NodeLabelsCache.Client client;
        private final int numberOfNodes;
        private long[] scratch;

        LabelGetter( NodeLabelsCache cache, long[][] expectedLabels, int numberOfNodes )
        {
            this.cache = cache;
            this.client = cache.newClient();
            this.expectedLabels = expectedLabels;
            this.numberOfNodes = numberOfNodes;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < 1_000; i++ )
            {
                int nodeId = ThreadLocalRandom.current().nextInt( numberOfNodes );
                scratch = cache.get( client, nodeId );
                assertCorrectLabels( nodeId, scratch );
            }
        }

        private void assertCorrectLabels( int nodeId, long[] gotten )
        {
            long[] expected = expectedLabels[nodeId];
            for ( int i = 0; i < expected.length; i++ )
            {
                assertEquals( expected[i], gotten[i] );
            }

            if ( gotten.length != expected.length )
            {
                // gotten is a "scratch" array, i.e. reused and not resized all the time, instead ended with -1 value.
                assertEquals( -1, gotten[expected.length] );
            }
        }
    }

    private long[] randomLabels( int count, int highId )
    {
        long[] result = new long[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = random.nextInt( highId );
        }
        return result;
    }

    private static long[] shrunk( long[] readLabels )
    {
        for ( int i = 0; i < readLabels.length; i++ )
        {
            if ( readLabels[i] == -1 )
            {
                return Arrays.copyOf( readLabels, i );
            }
        }
        return readLabels;
    }
}
