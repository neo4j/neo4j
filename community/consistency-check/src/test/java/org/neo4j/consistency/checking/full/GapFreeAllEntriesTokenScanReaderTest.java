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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.internal.index.label.AllEntriesTokenScanReader;
import org.neo4j.internal.index.label.EntityTokenRange;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.common.EntityType.NODE;

@ExtendWith( RandomExtension.class )
class GapFreeAllEntriesTokenScanReaderTest
{
    private static final int EMPTY_RANGE = 0;
    private static final int NON_EMPTY_RANGE = 0b10101; // 0, 2, 4
    private static final int RANGE_SIZE = 10;
    private static final long[] LABEL_IDS = new long[] {1};

    @Inject
    private RandomRule random;

    @Test
    void openCloseSeparateCursorForAllEntriesTokenReader() throws Exception
    {
        int[] ranges = array( EMPTY_RANGE, EMPTY_RANGE, NON_EMPTY_RANGE );
        var cacheTracer = mock( PageCacheTracer.class );
        var cursorTracer = mock( PageCursorTracer.class );
        when( cacheTracer.createPageCursorTracer( any( String.class ) ) ).thenReturn( cursorTracer );

        try ( var reader = newGapFreeAllEntriesLabelScanReader( cacheTracer, ranges ) )
        {
            assertRanges( reader.iterator(), ranges );
        }

        verify( cacheTracer ).createPageCursorTracer( any( String.class ) );
        verify( cursorTracer ).close();
    }

    @Test
    void shouldFillGapInBeginning()
    {
        // given
        int[] ranges = array( EMPTY_RANGE, EMPTY_RANGE, NON_EMPTY_RANGE );
        GapFreeAllEntriesTokenScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<EntityTokenRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    void shouldFillGapInEnd()
    {
        // given
        int[] ranges = array( NON_EMPTY_RANGE, EMPTY_RANGE, EMPTY_RANGE );
        GapFreeAllEntriesTokenScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<EntityTokenRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    void shouldFillGapInMiddle()
    {
        // given
        int[] ranges = array( EMPTY_RANGE, NON_EMPTY_RANGE, EMPTY_RANGE );
        GapFreeAllEntriesTokenScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<EntityTokenRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    @Test
    void shouldFillRandomGaps()
    {
        // given
        int numberOfRanges = random.intBetween( 50, 100 );
        int[] ranges = new int[numberOfRanges];
        for ( int rangeId = 0; rangeId < numberOfRanges; rangeId++ )
        {
            ranges[rangeId] = random.nextInt( 1 << RANGE_SIZE );
        }
        GapFreeAllEntriesTokenScanReader reader = newGapFreeAllEntriesLabelScanReader( ranges );

        // when
        Iterator<EntityTokenRange> iterator = reader.iterator();

        // then
        assertRanges( iterator, ranges );
    }

    private static void assertRanges( Iterator<EntityTokenRange> iterator, int[] expectedRanges )
    {
        for ( int expectedRangeId = 0; expectedRangeId < expectedRanges.length; expectedRangeId++ )
        {
            assertTrue( iterator.hasNext() );
            EntityTokenRange actualRange = iterator.next();
            assertEquals( expectedRangeId, actualRange.id() );
            int expectedRange = expectedRanges[expectedRangeId];
            long baseNodeId = expectedRangeId * RANGE_SIZE;
            for ( int i = 0; i < RANGE_SIZE; i++ )
            {
                long nodeId = baseNodeId + i;
                long[] expectedLabelIds = (expectedRange & (1 << i)) == 0 ? EMPTY_LONG_ARRAY : LABEL_IDS;
                assertArrayEquals( expectedLabelIds, actualRange.tokens( nodeId ) );
                assertEquals( nodeId, actualRange.entities()[i] );
            }
        }
        assertFalse( iterator.hasNext() );
    }

    private static GapFreeAllEntriesTokenScanReader newGapFreeAllEntriesLabelScanReader( int... ranges )
    {
        return newGapFreeAllEntriesLabelScanReader( PageCacheTracer.NULL, ranges );
    }

    private static GapFreeAllEntriesTokenScanReader newGapFreeAllEntriesLabelScanReader( PageCacheTracer cacheTracer, int... ranges )
    {
        var labelScanStore = prepareLabelScanStore( ranges );
        return new GapFreeAllEntriesTokenScanReader( labelScanStore, RANGE_SIZE * ranges.length, cacheTracer );
    }

    private static LabelScanStore prepareLabelScanStore( int[] ranges )
    {
        var labelScanStore = mock( LabelScanStore.class );
        when( labelScanStore.allEntityTokenRanges( any( PageCursorTracer.class ) ) ).thenReturn( ranges( NODE, RANGE_SIZE, ranges ) );
        return labelScanStore;
    }

    private static AllEntriesTokenScanReader ranges( EntityType entityType, int rangeSize, int... ranges )
    {
        List<EntityTokenRange> rangeList = new ArrayList<>();
        for ( int rangeId = 0; rangeId < ranges.length; rangeId++ )
        {
            rangeList.add( new EntityTokenRange( rangeId, labelsPerNode( ranges[rangeId] ), entityType ) );
        }

        return new AllEntriesTokenScanReader()
        {
            @Override
            public void close()
            {   // Nothing to close
            }

            @Override
            public Iterator<EntityTokenRange> iterator()
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
