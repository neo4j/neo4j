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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.function.Predicate;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public class IndexSamplingJobQueue<T>
{
    private final Queue<T> queue = new ArrayDeque<>();
    private final Predicate<? super T> enqueueablePredicate;

    public IndexSamplingJobQueue( Predicate<? super T> enqueueablePredicate )
    {
        this.enqueueablePredicate = enqueueablePredicate;
    }

    public synchronized void add( boolean force, T item )
    {
        if ( shouldEnqueue( force, item ) )
        {
            queue.add( item );
        }
    }

    public synchronized void addAll( boolean force, Iterator<T> items )
    {
        while ( items.hasNext() )
        {
            add( force, items.next() );
        }
    }

    private boolean shouldEnqueue( boolean force, T item )
    {

        // Add index if not in queue
        if ( queue.contains( item ) )
        {
            return false;
        }

        // and either adding all
        if ( force )
        {
            return true;
        }

        // or otherwise only if seen enough updates (as determined by updatePredicate)
        return enqueueablePredicate.test( item );
    }

    public synchronized T poll()
    {
        return queue.poll();
    }

    public synchronized Iterable<T> pollAll()
    {
        Collection<T> items = new ArrayList<>( queue.size() );
        while ( true )
        {
            T item = queue.poll();
            if ( item == null )
            {
                return items;
            }
            items.add( item );
        }
    }
}
