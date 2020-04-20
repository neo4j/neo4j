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
package org.neo4j.internal.index.label;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.IntFunction;

import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Long.max;
import static java.lang.Math.toIntExact;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.asArray;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.helpers.collection.Iterables.reverse;
import static org.neo4j.internal.index.label.TokenScanValue.RANGE_SIZE;

@ExtendWith( RandomExtension.class )
class NativeAllEntriesTokenScanReaderTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldSeeNonOverlappingRanges() throws Exception
    {
        int rangeSize = 4;
        // new ranges at: 0, 4, 8, 12 ...
        shouldIterateCorrectlyOver(
                labels( 0, rangeSize, 0, 1, 2, 3 ),
                labels( 1, rangeSize, 4, 6 ),
                labels( 2, rangeSize, 12 ),
                labels( 3, rangeSize, 17, 18 ) );
    }

    @Test
    void shouldSeeOverlappingRanges() throws Exception
    {
        int rangeSize = 4;
        // new ranges at: 0, 4, 8, 12 ...
        shouldIterateCorrectlyOver(
                labels( 0, rangeSize, 0, 1, 3, 55 ),
                labels( 3, rangeSize, 1, 2, 5, 6, 43 ),
                labels( 5, rangeSize, 8, 9, 15, 42 ),
                labels( 6, rangeSize, 4, 8, 12 ) );
    }

    @Test
    void shouldSeeRangesFromRandomData() throws Exception
    {
        List<Labels> labels = randomData( random );

        shouldIterateCorrectlyOver( labels.toArray( new Labels[0] ) );
    }

    private static void shouldIterateCorrectlyOver( Labels... data ) throws Exception
    {
        // GIVEN
        try ( AllEntriesTokenScanReader reader = new NativeAllEntriesTokenScanReader( store( data ), highestLabelId( data ), NODE ) )
        {
            // WHEN/THEN
            assertRanges( reader, data );
        }
    }

    static List<Labels> randomData( RandomRule random )
    {
        List<Labels> labels = new ArrayList<>();
        int labelCount = random.intBetween( 30, 100 );
        int labelId = 0;
        for ( int i = 0; i < labelCount; i++ )
        {
            labelId += random.intBetween( 1, 20 );
            int nodeCount = random.intBetween( 20, 100 );
            long[] nodeIds = new long[nodeCount];
            long nodeId = 0;
            for ( int j = 0; j < nodeCount; j++ )
            {
                nodeId += random.intBetween( 1, 100 );
                nodeIds[j] = nodeId;
            }
            labels.add( labels( labelId, nodeIds ) );
        }
        return labels;
    }

    private static int highestLabelId( Labels[] data )
    {
        int highest = 0;
        for ( Labels labels : data )
        {
            highest = Integer.max( highest, labels.labelId );
        }
        return highest;
    }

    private static void assertRanges( AllEntriesTokenScanReader reader, Labels[] data )
    {
        Iterator<EntityTokenRange> iterator = reader.iterator();
        long highestRangeId = highestRangeId( data );
        for ( long rangeId = 0; rangeId <= highestRangeId; rangeId++ )
        {
            SortedMap<Long/*nodeId*/,List<Long>/*labelIds*/> expected = rangeOf( data, rangeId );
            if ( expected != null )
            {
                assertTrue( iterator.hasNext(), "Was expecting range " + expected );
                EntityTokenRange range = iterator.next();

                assertEquals( rangeId, range.id() );
                for ( Map.Entry<Long,List<Long>> expectedEntry : expected.entrySet() )
                {
                    long[] labels = range.tokens( expectedEntry.getKey() );
                    assertArrayEquals( asArray( expectedEntry.getValue().iterator() ), labels );
                }
            }
            // else there was nothing in this range
        }
        assertFalse( iterator.hasNext() );
    }

    private static SortedMap<Long,List<Long>> rangeOf( Labels[] data, long rangeId )
    {
        SortedMap<Long,List<Long>> result = new TreeMap<>();
        for ( Labels label : data )
        {
            for ( Pair<TokenScanKey,TokenScanValue> entry : label.entries )
            {
                if ( entry.first().idRange == rangeId )
                {
                    long baseNodeId = entry.first().idRange * RANGE_SIZE;
                    long bits = entry.other().bits;
                    while ( bits != 0 )
                    {
                        long nodeId = baseNodeId + Long.numberOfTrailingZeros( bits );
                        result.computeIfAbsent( nodeId, id -> new ArrayList<>() ).add( (long) label.labelId );
                        bits &= bits - 1;
                    }
                }
            }
        }
        return result.isEmpty() ? null : result;
    }

    private static long highestRangeId( Labels[] data )
    {
        long highest = 0;
        for ( Labels labels : data )
        {
            Pair<TokenScanKey,TokenScanValue> highestEntry = labels.entries.get( labels.entries.size() - 1 );
            highest = max( highest, highestEntry.first().idRange );
        }
        return highest;
    }

    private static IntFunction<Seeker<TokenScanKey,TokenScanValue>> store( Labels... labels )
    {
        final MutableIntObjectMap<Labels> labelsMap = new IntObjectHashMap<>( labels.length );
        for ( Labels item : labels )
        {
            labelsMap.put( item.labelId, item );
        }

        return labelId ->
        {
            Labels item = labelsMap.get( labelId );
            return item != null ? item.cursor() : EMPTY_CURSOR;
        };
    }

    static Labels labels( int labelId, long... nodeIds )
    {
        List<Pair<TokenScanKey,TokenScanValue>> entries = new ArrayList<>();
        long currentRange = 0;
        TokenScanValue value = new TokenScanValue();
        for ( long nodeId : nodeIds )
        {
            long range = nodeId / RANGE_SIZE;
            if ( range != currentRange )
            {
                if ( value.bits != 0 )
                {
                    entries.add( Pair.of( new TokenScanKey().set( labelId, currentRange ), value ) );
                    value = new TokenScanValue();
                }
            }
            value.set( toIntExact( nodeId % RANGE_SIZE ) );
            currentRange = range;
        }

        if ( value.bits != 0 )
        {
            entries.add( Pair.of( new TokenScanKey().set( labelId, currentRange ), value ) );
        }

        return new Labels( labelId, entries, nodeIds );
    }

    static class Labels
    {
        private final int labelId;
        private final List<Pair<TokenScanKey,TokenScanValue>> entries;
        private final long[] nodeIds;

        Labels( int labelId, List<Pair<TokenScanKey,TokenScanValue>> entries, long... nodeIds )
        {
            this.labelId = labelId;
            this.entries = entries;
            this.nodeIds = nodeIds;
        }

        Seeker<TokenScanKey,TokenScanValue> cursor()
        {
            return new LabelsSeeker<>( entries );
        }

        Seeker<TokenScanKey,TokenScanValue> descendingCursor()
        {
            return new LabelsSeeker<>( reverse( entries ) );
        }

        public long[] getNodeIds()
        {
            return nodeIds;
        }
    }

    static final class LabelsSeeker<TokenScanKey, TokenScanValue> implements Seeker<TokenScanKey,TokenScanValue>
    {
        int cursor = -1;
        private final List<Pair<TokenScanKey,TokenScanValue>> entries;

        LabelsSeeker( List<Pair<TokenScanKey,TokenScanValue>> entries )
        {
            this.entries = entries;
        }

        @Override
        public TokenScanKey key()
        {
            return entries.get( cursor ).first();
        }

        @Override
        public TokenScanValue value()
        {
            return entries.get( cursor ).other();
        }

        @Override
        public boolean next()
        {
            if ( cursor + 1 >= entries.size() )
            {
                return false;
            }
            cursor++;
            return true;
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    }

    static final Seeker<TokenScanKey,TokenScanValue> EMPTY_CURSOR = new Seeker<>()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public TokenScanKey key()
        {
            throw new IllegalStateException();
        }

        @Override
        public TokenScanValue value()
        {
            throw new IllegalStateException();
        }
    };
}
