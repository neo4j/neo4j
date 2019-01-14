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
package org.neo4j.consistency.checking.full;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class GapFreeAllEntriesLabelScanReaderTest
{
    private static final int EMPTY_RANGE = 0;
    private static final int NON_EMPTY_RANGE = 0b10101; // 0, 2, 4
    private static final int RANGE_SIZE = 10;
    private static final long[] LABEL_IDS = new long[] {1};

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldFillGapInBeginning()
    {
        // given
        int[] ranges = array( EMPTY_RANGE, EMPTY_RANGE, NON_EMPTY_RANGE );
        GapFreeAllEntriesLabelScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<NodeLabelRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    public void shouldFillGapInEnd()
    {
        // given
        int[] ranges = array( NON_EMPTY_RANGE, EMPTY_RANGE, EMPTY_RANGE );
        GapFreeAllEntriesLabelScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<NodeLabelRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    public void shouldFillGapInMiddle()
    {
        // given
        int[] ranges = array( EMPTY_RANGE, NON_EMPTY_RANGE, EMPTY_RANGE );
        GapFreeAllEntriesLabelScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<NodeLabelRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    public void shouldFillRandomGaps()
    {
        // given
        int numberOfRanges = random.intBetween( 50, 100 );
        int[] ranges = new int[numberOfRanges];
        for ( int rangeId = 0; rangeId < numberOfRanges; rangeId++ )
        {
            ranges[rangeId] = random.nextInt( 1 << RANGE_SIZE );
        }
        GapFreeAllEntriesLabelScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<NodeLabelRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    private void assertRanges( Iterator<NodeLabelRange> iterator, int[] expectedRanges )
    {
        for ( int expectedRangeId = 0; expectedRangeId < expectedRanges.length; expectedRangeId++ )
        {
            assertTrue( iterator.hasNext() );
            NodeLabelRange actualRange = iterator.next();
            assertEquals( expectedRangeId, actualRange.id() );
            int expectedRange = expectedRanges[expectedRangeId];
            long baseNodeId = expectedRangeId * RANGE_SIZE;
            for ( int i = 0; i < RANGE_SIZE; i++ )
            {
                long nodeId = baseNodeId + i;
                long[] expectedLabelIds = (expectedRange & (1 << i)) == 0 ? EMPTY_LONG_ARRAY : LABEL_IDS;
                assertArrayEquals( expectedLabelIds, actualRange.labels( nodeId ) );
                assertEquals( nodeId, actualRange.nodes()[i] );
            }
        }
        assertFalse( iterator.hasNext() );
    }

    private GapFreeAllEntriesLabelScanReader newGapFreeAllEntriesLabelScanReader( int... ranges )
    {
        return new GapFreeAllEntriesLabelScanReader( ranges( RANGE_SIZE, ranges ), RANGE_SIZE * ranges.length );
    }

    private static AllEntriesLabelScanReader ranges( int rangeSize, int... ranges )
    {
        List<NodeLabelRange> rangeList = new ArrayList<>();
        for ( int rangeId = 0; rangeId < ranges.length; rangeId++ )
        {
            rangeList.add( new NodeLabelRange( rangeId, labelsPerNode( ranges[rangeId] ) ) );
        }

        return new AllEntriesLabelScanReader()
        {
            @Override
            public void close()
            {   // Nothing to close
            }

            @Override
            public Iterator<NodeLabelRange> iterator()
            {
                return rangeList.iterator();
            }

            @Override
            public long maxCount()
            {
                return ranges.length * rangeSize;
            }

            @Override
            public int rangeSize()
            {
                return RANGE_SIZE;
            }
        };
    }

    private static long[][] labelsPerNode( int relativeNodeIds )
    {
        long[][] result = new long[RANGE_SIZE][];
        for ( int i = 0; i < RANGE_SIZE; i++ )
        {
            if ( (relativeNodeIds & (1 << i)) != 0 )
            {
                result[i] = LABEL_IDS;
            }
        }
        return result;
    }

    private static int[] array( int... relativeNodeIds )
    {
        return relativeNodeIds;
    }
}
