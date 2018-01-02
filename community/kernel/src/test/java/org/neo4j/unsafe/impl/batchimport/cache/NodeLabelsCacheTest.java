/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.test.Race;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeLabelsCacheTest
{
    @Test
    public void shouldCacheSmallSetOfLabelsPerNode() throws Exception
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, 5, CHUNK_SIZE );
        NodeLabelsCache.Client client = cache.newClient();
        long nodeId = 0;

        // WHEN
        cache.put( nodeId, new long[] {1,2,3} );

        // THEN
        int[] readLabels = new int[3];
        cache.get( client, nodeId, readLabels );
        assertArrayEquals( new int[] {1,2,3}, readLabels );
    }

    @Test
    public void shouldHandleLargeAmountOfLabelsPerNode() throws Exception
    {
        // GIVEN
        int highLabelId = 1000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId, CHUNK_SIZE );
        NodeLabelsCache.Client client = cache.newClient();
        long nodeId = 0;

        // WHEN
        int[] labels = randomLabels( 200, 1000 );
        cache.put( nodeId, asLongArray( labels ) );

        // THEN
        int[] readLabels = new int[labels.length];
        cache.get( client, nodeId, readLabels );
        assertArrayEquals( labels, readLabels );
    }

    @Test
    public void shouldHandleLabelsForManyNodes() throws Exception
    {
        // GIVEN a really weird scenario where we have 5000 different labels
        int highLabelId = 1_000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId, 1_000_000 );
        NodeLabelsCache.Client client = cache.newClient();
        int numberOfNodes = 100_000;
        int[][] expectedLabels = new int[numberOfNodes][];
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            int[] labels = randomLabels( random.nextInt( 30 )+1, highLabelId );
            expectedLabels[i] = labels;
            cache.put( i, asLongArray( labels ) );
        }

        // THEN
        int[] forceCreationOfNewIntArray = new int[0];
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            int[] labels = cache.get( client, i, forceCreationOfNewIntArray );
            assertArrayEquals( "For node " + i, expectedLabels[i], labels );
        }
    }

    @Test
    public void shouldEndTargetArrayWithMinusOne() throws Exception
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, 10 );
        NodeLabelsCache.Client client = cache.newClient();
        cache.put( 10, new long[] { 5, 6, 7, 8 } );

        // WHEN
        int[] target = new int[20];
        assertTrue( target == cache.get( client, 10, target ) );
        assertEquals( 5, target[0] );
        assertEquals( 6, target[1] );
        assertEquals( 7, target[2] );
        assertEquals( 8, target[3] );

        // THEN
        assertEquals( -1, target[4] );
    }

    @Test
    public void shouldReturnEmptyArrayForNodeWithNoLabelsAndNoLabelsWhatsoever() throws Exception
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, 0 );
        NodeLabelsCache.Client client = cache.newClient();

        // WHEN
        int[] target = new int[3];
        cache.get( client, 0, target );

        // THEN
        assertEquals( -1, target[0] );
    }

    @Test
    public void shouldSupportConcurrentGet() throws Throwable
    {
        // GIVEN
        int highLabelId = 10, numberOfNodes = 100;
        int[][] expectedLabels = new int[numberOfNodes][];
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId );
        for ( int i = 0; i < numberOfNodes; i++ )
        {
            cache.put( i, asLongArray( expectedLabels[i] = randomLabels( random.nextInt( 5 ), highLabelId ) ) );
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
        private final int[][] expectedLabels;
        private final NodeLabelsCache.Client client;
        private final int numberOfNodes;
        private int[] scratch = new int[10];

        public LabelGetter( NodeLabelsCache cache, int[][] expectedLabels, int numberOfNodes )
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
                scratch = cache.get( client, nodeId, scratch );
                assertCorrectLabels( nodeId, scratch );
            }
        }

        private void assertCorrectLabels( int nodeId, int[] gotten )
        {
            int[] expected = expectedLabels[nodeId];
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

    private long[] asLongArray( int[] labels )
    {
        long[] result = new long[labels.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            result[i] = labels[i];
        }
        return result;
    }

    private int[] randomLabels( int count, int highId )
    {
        int[] result = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = random.nextInt( highId );
        }
        return result;
    }

    private static final int CHUNK_SIZE = 100;
    private final Random random = new Random( 1234 );
}
