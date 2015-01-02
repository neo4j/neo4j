/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Random;

import org.junit.Test;
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
        long nodeId = 0;

        // WHEN
        cache.put( nodeId, new long[] {1,2,3} );

        // THEN
        int[] readLabels = new int[3];
        cache.get( nodeId, readLabels );
        assertArrayEquals( new int[] {1,2,3}, readLabels );
    }

    @Test
    public void shouldHandleLargeAmountOfLabelsPerNode() throws Exception
    {
        // GIVEN
        int highLabelId = 1000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId, CHUNK_SIZE );
        long nodeId = 0;

        // WHEN
        int[] labels = randomLabels( 200, 1000 );
        cache.put( nodeId, asLongArray( labels ) );

        // THEN
        int[] readLabels = new int[labels.length];
        cache.get( nodeId, readLabels );
        assertArrayEquals( labels, readLabels );
    }

    @Test
    public void shouldHandleLabelsForManyNodes() throws Exception
    {
        // GIVEN a really weird scenario where we have 5000 different labels
        int highLabelId = 1_000;
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, highLabelId, 1_000_000 );
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
            int[] labels = cache.get( i, forceCreationOfNewIntArray );
            assertArrayEquals( "For node " + i, expectedLabels[i], labels );
        }
    }

    @Test
    public void shouldEndTargetArrayWithMinusOne() throws Exception
    {
        // GIVEN
        NodeLabelsCache cache = new NodeLabelsCache( NumberArrayFactory.AUTO, 10 );
        cache.put( 10, new long[] { 5, 6, 7, 8 } );

        // WHEN
        int[] target = new int[20];
        assertTrue( target == cache.get( 10, target ) );
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

        // WHEN
        int[] target = new int[3];
        cache.get( 0, target );

        // THEN
        assertEquals( -1, target[0] );
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
