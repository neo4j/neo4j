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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;

/**
 * Take multiple {@link BlockEntryCursor} that each by themselves provide block entries in sorted order and lazily merge join, providing a view over all
 * entries from given cursors in sorted order.
 * Merging is done by keeping the cursors in a priority queue with a comparator that compare {@link BlockEntryCursor#key()} (current key on cursor).
 * Instances handed out from {@link #key()} and {@link #value()} are reused, consumer is responsible for creating copy if there is a need to cache results.
 */
public class MergingBlockEntryReader<KEY,VALUE> implements BlockEntryCursor<KEY,VALUE>
{
    private final PriorityQueue<BlockEntryCursor<KEY,VALUE>> sortedReaders;
    private final List<BlockEntryCursor<KEY,VALUE>> readersToClose = new ArrayList<>();
    private BlockEntryCursor<KEY,VALUE> lastReturned;

    MergingBlockEntryReader( Layout<KEY,VALUE> layout )
    {
        this.sortedReaders = new PriorityQueue<>( ( o1, o2 ) -> layout.compare( o1.key(), o2.key() ) );
    }

    void addSource( BlockEntryCursor<KEY,VALUE> source ) throws IOException
    {
        readersToClose.add( source );
        if ( source.next() )
        {
            sortedReaders.add( source );
        }
    }

    @Override
    public boolean next() throws IOException
    {
        if ( lastReturned != null )
        {
            if ( lastReturned.next() )
            {
                sortedReaders.add( lastReturned );
            }
        }

        if ( sortedReaders.isEmpty() )
        {
            return false;
        }
        lastReturned = sortedReaders.poll();
        return true;
    }

    @Override
    public KEY key()
    {
        return lastReturned.key();
    }

    @Override
    public VALUE value()
    {
        return lastReturned.value();
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( readersToClose );
    }
}
