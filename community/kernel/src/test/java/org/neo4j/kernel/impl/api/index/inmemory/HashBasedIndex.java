/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

class HashBasedIndex extends InMemoryIndexImplementation
{
    private final Map<Object, Set<Long>> data = new HashMap<>();

    @Override
    public String toString()
    {
        return data.toString();
    }

    @Override
    void clear()
    {
        data.clear();
    }

    @Override
    PrimitiveLongIterator doLookup( Object propertyValue )
    {
        Set<Long> nodes = data.get( propertyValue );
        return nodes == null ? emptyPrimitiveLongIterator() : toPrimitiveLongIterator( nodes.iterator() );
    }

    @Override
    void doAdd( Object propertyValue, long nodeId, boolean applyIdempotently )
    {
        Set<Long> nodes = data.get( propertyValue );
        if ( nodes == null )
        {
            data.put( propertyValue, nodes = new HashSet<>() );
        }
        // In this implementation we don't care about idempotency.
        nodes.add( nodeId );
    }

    @Override
    void doRemove( Object propertyValue, long nodeId )
    {
        Set<Long> nodes = data.get( propertyValue );
        if ( nodes != null )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    void remove( long nodeId )
    {
        for ( Set<Long> nodes : data.values() )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    void iterateAll( IndexEntryIterator iterator ) throws Exception
    {
        for ( Map.Entry<Object, Set<Long>> entry : data.entrySet() )
        {
            iterator.visitEntry( entry.getKey(), entry.getValue() );
        }
    }

    @Override
    public long maxCount()
    {
        return ids().size();
    }

    @Override
    public Iterator<Long> iterator()
    {
        return ids().iterator();
    }

    private Collection<Long> ids()
    {
        Set<Long> allIds = new HashSet<>();
        for ( Set<Long> someIds : data.values() )
        {
            allIds.addAll( someIds );
        }
        return allIds;
    }

    @Override
    InMemoryIndexImplementation snapshot()
    {
        HashBasedIndex snapshot = new HashBasedIndex();
        for ( Map.Entry<Object, Set<Long>> entry : data.entrySet() )
        {
            snapshot.data.put( entry.getKey(), new HashSet<>( entry.getValue() ) );
        }
        return snapshot;
    }

    @Override
    public boolean hasIndexed( long nodeId, Object propertyValue )
    {
        Set<Long> canditates = data.get( propertyValue );
        return canditates != null && canditates.contains( nodeId );
    }
}
