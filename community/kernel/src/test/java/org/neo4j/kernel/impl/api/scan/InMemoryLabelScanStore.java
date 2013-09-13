/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.scan.LabelScanReader;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static java.util.Arrays.binarySearch;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class InMemoryLabelScanStore implements LabelScanStore
{
    private final Map<Long, Set<Long>> data = new HashMap<>();

    @Override
    public void updateAndCommit( Iterator<NodeLabelUpdate> updates )
    {
        while ( updates.hasNext() )
        {
            NodeLabelUpdate update = updates.next();
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
    }

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
    public void recover( Iterator<NodeLabelUpdate> updates )
    {
        updateAndCommit( updates );
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
                assert nodes != null;

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
    public void force()
    {   // Nothing to force
    }
}
