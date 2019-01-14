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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.IntFunction;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Long.max;
import static java.lang.Math.toIntExact;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.impl.index.labelscan.LabelScanValue.RANGE_SIZE;

public class NativeAllEntriesLabelScanReaderTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldSeeNonOverlappingRanges() throws Exception
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
    public void shouldSeeOverlappingRanges() throws Exception
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
    public void shouldSeeRangesFromRandomData() throws Exception
    {
        List<Labels> labels = randomData();

        shouldIterateCorrectlyOver( labels.toArray( new Labels[labels.size()] ) );
    }

    private void shouldIterateCorrectlyOver( Labels... data ) throws Exception
    {
        // GIVEN
        try ( AllEntriesLabelScanReader reader = new NativeAllEntriesLabelScanReader(
                store( data ), highestLabelId( data ) ) )
        {
            // WHEN/THEN
            assertRanges( reader, data );
        }
    }

    private List<Labels> randomData()
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

    private static void assertRanges( AllEntriesLabelScanReader reader, Labels[] data )
    {
        Iterator<NodeLabelRange> iterator = reader.iterator();
        long highestRangeId = highestRangeId( data );
        for ( long rangeId = 0; rangeId <= highestRangeId; rangeId++ )
        {
            SortedMap<Long/*nodeId*/,List<Long>/*labelIds*/> expected = rangeOf( data, rangeId );
            if ( expected != null )
            {
                assertTrue( "Was expecting range " + expected, iterator.hasNext() );
                NodeLabelRange range = iterator.next();

                assertEquals( rangeId, range.id() );
                for ( Map.Entry<Long,List<Long>> expectedEntry : expected.entrySet() )
                {
                    long[] labels = range.labels( expectedEntry.getKey() );
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
            for ( Pair<LabelScanKey,LabelScanValue> entry : label.entries )
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
            Pair<LabelScanKey,LabelScanValue> highestEntry = labels.entries.get( labels.entries.size() - 1 );
            highest = max( highest, highestEntry.first().idRange );
        }
        return highest;
    }

    private static IntFunction<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> store( Labels... labels )
    {
        PrimitiveIntObjectMap<Labels> labelsMap = Primitive.intObjectMap( labels.length );
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

    private static Labels labels( int labelId, long... nodeIds )
    {
        List<Pair<LabelScanKey,LabelScanValue>> entries = new ArrayList<>();
        long currentRange = 0;
        LabelScanValue value = new LabelScanValue();
        for ( long nodeId : nodeIds )
        {
            long range = nodeId / RANGE_SIZE;
            if ( range != currentRange )
            {
                if ( value.bits != 0 )
                {
                    entries.add( Pair.of( new LabelScanKey().set( labelId, currentRange ), value ) );
                    value = new LabelScanValue();
                }
            }
            value.set( toIntExact( nodeId % RANGE_SIZE ) );
            currentRange = range;
        }

        if ( value.bits != 0 )
        {
            entries.add( Pair.of( new LabelScanKey().set( labelId, currentRange ), value ) );
        }

        return new Labels( labelId, entries );
    }

    private static class Labels
    {
        private final int labelId;
        private final List<Pair<LabelScanKey,LabelScanValue>> entries;

        Labels( int labelId, List<Pair<LabelScanKey,LabelScanValue>> entries )
        {
            this.labelId = labelId;
            this.entries = entries;
        }

        RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor()
        {
            return new RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>()
            {
                int cursor = -1;

                @Override
                public Hit<LabelScanKey,LabelScanValue> get()
                {
                    assert cursor >= 0;
                    Pair<LabelScanKey,LabelScanValue> entry = entries.get( cursor );
                    return new MutableHit<>( entry.first(), entry.other() );
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
            };
        }
    }

    private static final RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> EMPTY_CURSOR =
            new RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>()
    {
        @Override
        public Hit<LabelScanKey,LabelScanValue> get()
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    };
}
