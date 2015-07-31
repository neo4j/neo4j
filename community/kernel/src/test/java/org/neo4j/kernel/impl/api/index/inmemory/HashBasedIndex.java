/*
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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.toPrimitiveIterator;
import static org.neo4j.register.Register.DoubleLong;

class HashBasedIndex extends InMemoryIndexImplementation
{
    private Map<Object, Set<Long>> data;

    public Map<Object,Set<Long>> data()
    {
        if ( data == null )
        {
            throw new IllegalStateException( "Index has not been created, or has been dropped." );
        }
        return data;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + data;
    }

    @Override
    void initialize()
    {
        data = new HashMap<>();
    }

    @Override
    void drop()
    {
        data = null;
    }

    @Override
    PrimitiveLongIterator doLookup( Object propertyValue )
    {
        Set<Long> nodes = data().get( propertyValue );
        return nodes == null ? PrimitiveLongCollections.emptyIterator() : toPrimitiveIterator( nodes.iterator() );
    }

    @Override
    boolean doAdd( Object propertyValue, long nodeId, boolean applyIdempotently )
    {
        Set<Long> nodes = data().get( propertyValue );
        if ( nodes == null )
        {
            data().put( propertyValue, nodes = new HashSet<>() );
        }
        // In this implementation we don't care about idempotency.
        return nodes.add( nodeId );
    }

    @Override
    void doRemove( Object propertyValue, long nodeId )
    {
        Set<Long> nodes = data().get( propertyValue );
        if ( nodes != null )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    void remove( long nodeId )
    {
        for ( Set<Long> nodes : data().values() )
        {
            nodes.remove( nodeId );
        }
    }

    @Override
    void iterateAll( IndexEntryIterator iterator ) throws Exception
    {
        for ( Map.Entry<Object, Set<Long>> entry : data().entrySet() )
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
        for ( Set<Long> someIds : data().values() )
        {
            allIds.addAll( someIds );
        }
        return allIds;
    }

    @Override
    InMemoryIndexImplementation snapshot()
    {
        HashBasedIndex snapshot = new HashBasedIndex();
        snapshot.initialize();
        for ( Map.Entry<Object, Set<Long>> entry : data().entrySet() )
        {
            snapshot.data().put( entry.getKey(), new HashSet<>( entry.getValue() ) );
        }
        return snapshot;
    }

    @Override
    public int getIndexedCount( long nodeId, Object propertyValue )
    {
        Set<Long> candidates = data().get( propertyValue );
        return candidates != null && candidates.contains( nodeId ) ? 1 : 0;
    }

    @Override
    public Set<Class> valueTypesInIndex()
    {
        if ( data == null )
        {
            return Collections.emptySet();
        }
        Set<Class> result = new HashSet<>();
        for ( Object value : data.keySet() )
        {
            if ( value instanceof Number )
            {
                result.add( Number.class );
            }
            else if ( value instanceof String )
            {
                result.add( String.class );
            }
            else if ( value instanceof Boolean )
            {
                result.add( Boolean.class );
            }
            else if ( value instanceof ArrayKey )
            {
                result.add( Array.class );
            }
        }
        return result;
    }

    @Override
    public long sampleIndex( final DoubleLong.Out result ) throws IndexNotFoundKernelException
    {
        if ( data == null )
        {
            throw new IndexNotFoundKernelException( "Index dropped while sampling." );
        }
        final long[] uniqueAndSize = {0, 0};
        try
        {
            iterateAll( new IndexEntryIterator()
            {
                @Override
                public void visitEntry( Object value, Set<Long> nodeIds )
                {
                    int ids = nodeIds.size();
                    if ( ids > 0 )
                    {
                        uniqueAndSize[0] += 1;
                        uniqueAndSize[1] += ids;
                    }
                }
            });
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }

        result.write( uniqueAndSize[0], uniqueAndSize[1] );
        return uniqueAndSize[1];
    }
}
