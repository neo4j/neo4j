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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;

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
    public Iterator<NodeLabelRange> iterator()
    {
        return new GapFillingIterator( nodeLabelRanges.iterator(), highId );
    }

    private static class GapFillingIterator extends PrefetchingIterator<NodeLabelRange>
    {
        private static final int BATCH_SIZE = 1_000;
        private static final long[] EMPTY_LONG_ARRAY = new long[0];
        private final long highId;
        private Iterator<NodeLabelRange> source;
        private NodeLabelRange nextFromSource;
        private long[] sourceNodeIds;
        private int sourceIndex;
        private int currentId;
        private boolean first;

        GapFillingIterator( Iterator<NodeLabelRange> iterator, long highId )
        {
            this.highId = highId;
            this.source = iterator;
            this.first = true;
        }

        @Override
        protected NodeLabelRange fetchNextOrNull()
        {
            long baseId = currentId;
            int batchSize = BATCH_SIZE;
            long[] nodes = new long[batchSize];
            long[][] labels = new long[batchSize][];

            int cursor = 0;
            for ( ; cursor < batchSize; cursor++, currentId++ )
            {
                if ( first || (sourceNodeIds != null && sourceIndex >= sourceNodeIds.length) )
                {
                    first = false;
                    if ( source.hasNext() )
                    {
                        nextFromSource = source.next();
                        sourceNodeIds = nextFromSource.nodes();
                        sourceIndex = 0;
                    }
                    else
                    {
                        nextFromSource = null;
                        sourceNodeIds = null;
                    }
                }

                if ( currentId >= highId && sourceNodeIds == null )
                {
                    break;
                }

                nodes[cursor] = currentId;
                if ( sourceNodeIds != null && sourceNodeIds[sourceIndex] == currentId )
                {
                    labels[cursor] = nextFromSource.labels( currentId );
                    sourceIndex++;
                }
                else
                {
                    labels[cursor] = EMPTY_LONG_ARRAY;
                }
            }
            return cursor > 0 ? new SimpleNodeLabelRange( baseId, nodes, labels ) : null;
        }

        private static class SimpleNodeLabelRange extends NodeLabelRange
        {
            private final long baseId;
            private final long[] nodes;
            private final long[][] labels;

            SimpleNodeLabelRange( long baseId, long[] nodes, long[][] labels )
            {
                this.baseId = baseId;
                this.nodes = nodes;
                this.labels = labels;
            }

            @Override
            public int id()
            {
                return (int) baseId;
            }

            @Override
            public long[] nodes()
            {
                return nodes;
            }

            @Override
            public long[] labels( long nodeId )
            {
                return labels[(int) (nodeId - baseId)];
            }

            @Override
            public String toString()
            {
                String rangeString = baseId + "-" + (baseId + nodes.length);
                String prefix = "NodeLabelRange[idRange=" + rangeString;
                return toString( prefix, nodes, labels );
            }
        }
    }
}
