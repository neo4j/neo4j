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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

class ListBasedIndex extends InMemoryIndexImplementation
{
    private final List<Entry> data = new ArrayList<>();

    @Override
    void clear()
    {
        data.clear();
    }

    @Override
    PrimitiveLongIterator doLookup( Object propertyValue )
    {
        return IteratorUtil.toPrimitiveLongIterator( find( data.iterator(), propertyValue ) );
    }

    @Override
    void doAdd( Object propertyValue, long nodeId, boolean applyIdempotently )
    {
        if ( applyIdempotently && find( data.iterator(), propertyValue ).hasNext() )
        {
            return;
        }
        data.add( new Entry( propertyValue, nodeId ) );
    }

    @Override
    void doRemove( Object propertyValue, long nodeId )
    {
        for ( Iterator<Entry> iterator = data.iterator(); iterator.hasNext(); )
        {
            if ( iterator.next().entryEquals( propertyValue, nodeId ) )
            {
                iterator.remove();
                return;
            }
        }
    }

    @Override
    void remove( long nodeId )
    {
        for ( Iterator<Entry> iterator = data.iterator(); iterator.hasNext(); )
        {
            if ( iterator.next().nodeId == nodeId )
            {
                iterator.remove();
            }
        }
    }

    @Override
    public long maxCount()
    {
        return data.size();
    }

    @Override
    public Iterator<Long> iterator()
    {
        final Iterator<Entry> iterator = data.iterator();

        return new PrefetchingIterator<Long>()
        {
            @Override
            protected Long fetchNextOrNull()
            {
                if ( ! iterator.hasNext() )
                {
                    return null;
                }
                return iterator.next().nodeId;
            }
        };
    }

    private static class Entry
    {
        private final Object propertyValue;
        private final long nodeId;

        Entry( Object propertyValue, long nodeId )
        {
            this.propertyValue = propertyValue;
            this.nodeId = nodeId;
        }

        boolean entryEquals( Object propertyValue, long nodeId )
        {
            return this.nodeId == nodeId && this.propertyValue.equals( propertyValue );
        }

        boolean valueEquals( Object propertyValue )
        {
            return this.propertyValue.equals( propertyValue );
        }
    }

    private static Iterator<Long> find( final Iterator<Entry> source, final Object propertyValue )
    {
        return new PrefetchingIterator<Long>()
        {
            @Override
            protected Long fetchNextOrNull()
            {
                while ( source.hasNext() )
                {
                    Entry entry = source.next();
                    if ( entry.valueEquals( propertyValue ) )
                    {
                        return entry.nodeId;
                    }
                }
                return null;
            }
        };
    }

    @Override
    InMemoryIndexImplementation snapshot()
    {
        ListBasedIndex snapshot = new ListBasedIndex();
        snapshot.data.addAll( data );
        return snapshot;
    }
}
