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
package org.neo4j.kernel.impl.api.scan;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.direct.NodeLabelRange;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static java.util.Arrays.binarySearch;
import static java.util.Collections.singletonList;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class InMemoryLabelScanStore implements LabelScanStore
{
    // LabelId --> Set<NodeId>
    private final Map<Long, Set<Long>> data = new HashMap<>();

    private Set<Long> nodeSetForRemoving( long labelId )
    {
        Set<Long> nodes = data.get( labelId );
        return nodes != null ? nodes : Collections.<Long>emptySet();
    }

    private Set<Long> nodeSetForAdding( long labelId )
    {
        Set<Long> nodes = data.get( labelId );
        if ( nodes == null )
        {
            nodes = new HashSet<>();
            data.put( labelId, nodes );
        }
        return nodes;
    }

    @Override
    public LabelScanReader newReader()
    {
        return new LabelScanReader()
        {
            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                Set<Long> nodes = data.get( (long) labelId );
                if ( null == nodes )
                {
                    return PrimitiveLongCollections.emptyIterator();
                }

                final Iterator<Long> nodesIterator = nodes.iterator();
                return new PrimitiveLongIterator()
                {
                    @Override
                    public long next()
                    {
                        return nodesIterator.next();
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return nodesIterator.hasNext();
                    }
                };
            }

            @Override
            public void close()
            {   // Nothing to close
            }

            @Override
            public Iterator<Long> labelsForNode( long nodeId )
            {
                List<Long> nodes = new ArrayList<>();
                for ( Map.Entry<Long, Set<Long>> entry : data.entrySet() )
                {
                    if ( entry.getValue().contains( nodeId ) )
                    {
                        nodes.add( entry.getKey() );
                    }
                }
                return nodes.iterator();
            }
        };
    }

    @Override
    public AllEntriesLabelScanReader newAllEntriesReader()
    {
        final Map<Long, Set<Long>> nodesToLabels = new HashMap<>();
        for ( Map.Entry<Long, Set<Long>> labelToNodes : data.entrySet() )
        {
            for ( Long nodeId : labelToNodes.getValue() )
            {
                if ( ! nodesToLabels.containsKey( nodeId ))
                {
                    nodesToLabels.put( nodeId, new HashSet<Long>(  ) );
                }
                nodesToLabels.get( nodeId ).add( labelToNodes.getKey() );
            }
        }

        return new AllEntriesLabelScanReader()
        {
            @Override
            public long maxCount()
            {
                return 0;
            }

            @Override
            public void close()
            {
            }

            @Override
            public Iterator<NodeLabelRange> iterator()
            {
                NodeLabelRange range = new NodeLabelRange()
                {
                    @Override
                    public int id()
                    {
                        return 0;
                    }

                    @Override
                    public long[] nodes()
                    {
                        return toLongArray( nodesToLabels.keySet() );
                    }

                    @Override
                    public long[] labels( long nodeId )
                    {
                        return toLongArray( nodesToLabels.get( nodeId ) );
                    }
                };
                return singletonList( range ).iterator();
            }

            private long[] toLongArray( Set<Long> longs )
            {
                long[] array = new long[longs.size()];
                int position = 0;
                for ( Long entry : longs )
                {
                    array[position++] = entry;
                }
                return array;
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles()
    {
        return emptyIterator();
    }

    @Override
    public void init()
    {   // Nothing to init
    }

    @Override
    public void start()
    {   // Nothing to start
    }

    @Override
    public void stop()
    {   // Nothing to stop
    }

    @Override
    public void shutdown()
    {   // Nothing to shutdown
    }

    @Override
    public LabelScanWriter newWriter()
    {
        return new LabelScanWriter()
        {
            @Override
            public void write( NodeLabelUpdate update ) throws IOException
            {
                // Split up into added/removed from before/after
                long[] added = new long[update.getLabelsAfter().length]; // pessimistic length
                long[] removed = new long[update.getLabelsBefore().length]; // pessimistic length

                int addedIndex = 0, removedIndex = 0;
                for ( long labelAfter : update.getLabelsAfter() )
                {
                    if ( binarySearch( update.getLabelsBefore(), labelAfter ) < 0 )
                    {
                        added[addedIndex++] = labelAfter;
                    }
                }

                for ( long labelBefore : update.getLabelsBefore() )
                {
                    if ( binarySearch( update.getLabelsAfter(), labelBefore ) < 0 )
                    {
                        removed[removedIndex++] = labelBefore;
                    }
                }

                // Update the internal map with those changes
                for ( int i = 0; i < addedIndex; i++ )
                {
                    nodeSetForAdding( added[i] ).add( update.getNodeId() );
                }
                for ( int i = 0; i < removedIndex; i++ )
                {
                    nodeSetForRemoving( removed[i] ).remove( update.getNodeId() );
                }
            }

            @Override
            public void close() throws IOException
            {
            }
        };
    }

    @Override
    public void force()
    {   // Nothing to force
    }
}
