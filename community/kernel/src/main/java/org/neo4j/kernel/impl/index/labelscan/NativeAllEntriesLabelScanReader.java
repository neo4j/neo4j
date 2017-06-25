/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;

import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.fill;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;
import static org.neo4j.kernel.impl.index.labelscan.LabelScanValue.RANGE_SIZE;

/**
 * {@link AllEntriesLabelScanReader} for {@link NativeLabelScanStore}.
 * <p>
 * {@link NativeLabelScanStore} uses {@link GBPTree} for storage and it doesn't have means of aggregating
 * results, so the approach this implementation is taking is to create one (lazy) seek cursor per label id
 * and coordinate those simultaneously over the scan. Each {@link NodeLabelRange} returned is a view
 * over all cursors at that same range, giving an aggregation of all labels in that node id range.
 */
class NativeAllEntriesLabelScanReader implements AllEntriesLabelScanReader
{
    private final IntFunction<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> seekProvider;
    private final List<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> cursors = new ArrayList<>();
    private final int highestLabelId;

    NativeAllEntriesLabelScanReader( IntFunction<RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException>> seekProvider,
            int highestLabelId )
    {
        this.seekProvider = seekProvider;
        this.highestLabelId = highestLabelId;
    }

    @Override
    public long maxCount()
    {
        return -1;
    }

    @Override
    public Iterator<NodeLabelRange> iterator()
    {
        try
        {
            long lowestRange = Long.MAX_VALUE;
            closeCursors();
            for ( int labelId = 0; labelId <= highestLabelId; labelId++ )
            {
                RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = seekProvider.apply( labelId );

                // Bootstrap the cursor, which also provides a great opportunity to exclude if empty
                if ( cursor.next() )
                {
                    lowestRange = min( lowestRange, cursor.get().key().idRange );
                    cursors.add( cursor );
                }
            }
            return new NodeLabelRangeIterator( lowestRange );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void closeCursors() throws IOException
    {
        for ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor : cursors )
        {
            cursor.close();
        }
        cursors.clear();
    }

    @Override
    public void close() throws Exception
    {
        closeCursors();
    }

    /**
     * The main iterator over {@link NodeLabelRange ranges}, aggregating all the cursors as it goes.
     */
    private class NodeLabelRangeIterator extends PrefetchingIterator<NodeLabelRange>
    {
        private long currentRange;

        // nodeId (relative to lowestRange) --> labelId[]
        @SuppressWarnings( "unchecked" )
        private final List<Long>[] labelsForEachNode = new List[RANGE_SIZE];

        NodeLabelRangeIterator( long lowestRange )
        {
            this.currentRange = lowestRange;
        }

        @Override
        protected NodeLabelRange fetchNextOrNull()
        {
            if ( currentRange == Long.MAX_VALUE )
            {
                return null;
            }

            fill( labelsForEachNode, null );
            long nextLowestRange = Long.MAX_VALUE;
            int slots = 0;
            try
            {
                // One "rangeSize" range at a time
                for ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor : cursors )
                {
                    long idRange = cursor.get().key().idRange;
                    if ( idRange < currentRange )
                    {
                        // This should only happen if the cursor has been exhausted and the iterator have moved on
                        // from the range it returned as its last hit.
                        assert !cursor.next();
                    }
                    else if ( idRange == currentRange )
                    {
                        slots = readRange( slots, cursor );

                        // Advance cursor and look ahead to the next range
                        if ( cursor.next() )
                        {
                            nextLowestRange = min( nextLowestRange, cursor.get().key().idRange );
                        }
                    }
                    else
                    {
                        // Excluded from this range
                        nextLowestRange = min( nextLowestRange, cursor.get().key().idRange );
                    }
                }

                NativeNodeLabelRange range = new NativeNodeLabelRange( currentRange, convertState(), slots );
                currentRange = nextLowestRange;

                return range;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        private long[][] convertState()
        {
            long[][] labelIdsByNodeIndex = new long[RANGE_SIZE][];
            for ( int i = 0; i < RANGE_SIZE; i++ )
            {
                List<Long> labelIdList = labelsForEachNode[i];
                if ( labelIdList != null )
                {
                    labelIdsByNodeIndex[i] = asArray( labelIdList.iterator() );
                }
            }
            return labelIdsByNodeIndex;
        }

        private int readRange( int slots, RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor )
        {
            long bits = cursor.get().value().bits;
            while ( bits != 0 )
            {
                int relativeNodeId = Long.numberOfTrailingZeros( bits );
                long labelId = cursor.get().key().labelId;
                if ( labelsForEachNode[relativeNodeId] == null )
                {
                    labelsForEachNode[relativeNodeId] = new ArrayList<>();
                    slots++;
                }
                labelsForEachNode[relativeNodeId].add( labelId );
                bits &= bits - 1;
            }
            return slots;
        }
    }

    private static class NativeNodeLabelRange implements NodeLabelRange
    {
        private final long idRange;
        private final long[] nodes;
        private final long[][] labels;

        NativeNodeLabelRange( long idRange, long[][] labels, int slots )
        {
            this.idRange = idRange;
            this.labels = labels;
            long baseNodeId = idRange * RANGE_SIZE;

            this.nodes = new long[slots];
            int nodeIndex = 0;
            for ( int i = 0; i < RANGE_SIZE; i++ )
            {
                if ( labels[i] != null )
                {
                    nodes[nodeIndex++] = baseNodeId + i;
                }
            }
        }

        @Override
        public int id()
        {
            return (int) idRange; // TODO this is a weird thing, id and this conversion
        }

        @Override
        public long[] nodes()
        {
            return nodes;
        }

        @Override
        public long[] labels( long nodeId )
        {
            long firstNodeId = idRange * RANGE_SIZE;
            int index = toIntExact( nodeId - firstNodeId );
            assert index >= 0 && index < RANGE_SIZE : "nodeId:" + nodeId + ", idRange:" + idRange;
            return labels[index];
        }

        @Override
        public String toString()
        {
            long lowRange = idRange * RANGE_SIZE;
            long highRange = (idRange + 1) * RANGE_SIZE;
            String rangeString = lowRange + "-" + highRange;
            StringBuilder result = new StringBuilder( "NodeLabelRange[idRange=" ).append( rangeString );
            result.append( "; {" );
            for ( int i = 0; i < nodes.length; i++ )
            {
                if ( i != 0 )
                {
                    result.append( ", " );
                }
                result.append( "Node[" ).append( nodes[i] ).append( "]: Labels[" );
                String sep = "";
                if ( labels[i] != null )
                {
                    for ( long labelId : labels[i] )
                    {
                        result.append( sep ).append( labelId );
                        sep = ", ";
                    }
                }
                result.append( "]" );
            }
            return result.append( "}]" ).toString();
        }
    }
}
