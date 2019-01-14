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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

import org.neo4j.collection.primitive.PrimitiveLongList;
import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;

import static java.lang.Long.min;
import static java.util.Arrays.fill;
import static org.neo4j.kernel.api.labelscan.NodeLabelRange.convertState;
import static org.neo4j.kernel.api.labelscan.NodeLabelRange.readBitmap;
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
        return UNKNOWN_MAX_COUNT;
    }

    @Override
    public int rangeSize()
    {
        return RANGE_SIZE;
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
        private final PrimitiveLongList[] labelsForEachNode = new PrimitiveLongList[RANGE_SIZE];

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
                        long bits = cursor.get().value().bits;
                        long labelId = cursor.get().key().labelId;
                        readBitmap( bits, labelId, labelsForEachNode );

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

                NodeLabelRange range = new NodeLabelRange( currentRange, convertState( labelsForEachNode ) );
                currentRange = nextLowestRange;

                return range;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
