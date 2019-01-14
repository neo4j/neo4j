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

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;

/**
 * Inserts empty {@link NodeLabelRange} for those ranges missing from the source iterator.
 * High node id is known up front such that ranges are returned up to that point.
 */
class GapFreeAllEntriesLabelScanReader implements AllEntriesLabelScanReader
{
    private final AllEntriesLabelScanReader nodeLabelRanges;
    private final long highId;

    GapFreeAllEntriesLabelScanReader( AllEntriesLabelScanReader nodeLabelRanges, long highId )
    {
        this.nodeLabelRanges = nodeLabelRanges;
        this.highId = highId;
    }

    @Override
    public long maxCount()
    {
        return nodeLabelRanges.maxCount();
    }

    @Override
    public void close() throws Exception
    {
        nodeLabelRanges.close();
    }

    @Override
    public int rangeSize()
    {
        return nodeLabelRanges.rangeSize();
    }

    @Override
    public Iterator<NodeLabelRange> iterator()
    {
        return new GapFillingIterator( nodeLabelRanges.iterator(), (highId - 1) / nodeLabelRanges.rangeSize(),
                nodeLabelRanges.rangeSize() );
    }

    private static class GapFillingIterator extends PrefetchingIterator<NodeLabelRange>
    {
        private final long highestRangeId;
        private final Iterator<NodeLabelRange> source;
        private final long[][] emptyRangeData;

        private NodeLabelRange nextFromSource;
        private long currentRangeId = -1;

        GapFillingIterator( Iterator<NodeLabelRange> nodeLableRangeIterator, long highestRangeId, int rangeSize )
        {
            this.highestRangeId = highestRangeId;
            this.source = nodeLableRangeIterator;
            this.emptyRangeData = new long[rangeSize][];
        }

        @Override
        protected NodeLabelRange fetchNextOrNull()
        {
            while ( true )
            {
                // These conditions only come into play after we've gotten the first range from the source
                if ( nextFromSource != null )
                {
                    if ( currentRangeId + 1 == nextFromSource.id() )
                    {
                        // Next to return is the one from source
                        currentRangeId++;
                        return nextFromSource;
                    }

                    if ( currentRangeId < nextFromSource.id() )
                    {
                        // Source range iterator has a gap we need to fill
                        return new NodeLabelRange( ++currentRangeId, emptyRangeData );
                    }
                }

                if ( source.hasNext() )
                {
                    // The source iterator has more ranges, grab the next one
                    nextFromSource = source.next();
                    // continue in the outer loop
                }
                else if ( currentRangeId < highestRangeId )
                {
                    nextFromSource = new NodeLabelRange( highestRangeId, emptyRangeData );
                    // continue in the outer loop
                }
                else
                {
                    // End has been reached
                    return null;
                }
            }
        }
    }
}
