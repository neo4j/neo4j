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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

class NonUniqueInMemoryIndex extends InMemoryIndex
{
    private final Map<Object, Set<Long>> indexData = new HashMap<>();

    @Override
    IndexPopulator getPopulator()
    {
        return new InMemoryIndex.Populator()
        {
        };
    }

    @Override
    IndexAccessor getOnlineAccessor()
    {
        return new InMemoryIndex.OnlineAccessor()
        {
            @Override
            public IndexReader newReader()
            {
                return new NonUniqueInMemoryIndexReader( indexData );
            }
        };
    }

    @Override
    public void add( long nodeId, Object propertyValue )
    {
        Set<Long> nodes = getLongs( propertyValue );
        nodes.add( nodeId );
    }

    @Override
    void remove( long nodeId, Object propertyValue )
    {
        Set<Long> nodes = getLongs( propertyValue );
        nodes.remove( nodeId );
    }

    @Override
    void clear()
    {
        indexData.clear();
    }

    private Set<Long> getLongs( Object propertyValue )
    {
        Object key = encode( propertyValue );
        Set<Long> nodes = indexData.get( key );
        if ( nodes == null )
        {
            nodes = new HashSet<>();
            indexData.put( key, nodes );
        }
        return nodes;
    }

    private static class ArrayKey
    {
        private final String arrayValue;

        private ArrayKey( String arrayValue )
        {
            this.arrayValue = arrayValue;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            ArrayKey other = (ArrayKey) o;

            return other.arrayValue.equals( this.arrayValue );
        }

        @Override
        public int hashCode()
        {
            return arrayValue != null ? arrayValue.hashCode() : 0;
        }
    }

    private static Object encode( Object propertyValue )
    {
        if ( propertyValue instanceof Number )
        {
            return ((Number) propertyValue).doubleValue();
        }

        if ( propertyValue instanceof Character )
        {
            return propertyValue.toString();
        }

        if ( propertyValue.getClass().isArray() )
        {
            return new ArrayKey( ArrayEncoder.encode( propertyValue ) );
        }

        return propertyValue;
    }

    static class NonUniqueInMemoryIndexReader implements IndexReader
    {
        private final HashMap<Object, Set<Long>> indexData;

        NonUniqueInMemoryIndexReader( Map<Object, Set<Long>> indexData )
        {
            this.indexData = new HashMap<>( indexData );
        }

        @Override
        public PrimitiveLongIterator lookup( Object value )
        {
            Set<Long> result = indexData.get( encode( value ) );
            return result != null ? toPrimitiveLongIterator( result.iterator() )  : emptyPrimitiveLongIterator();
        }

        @Override
        public void close()
        {
        }
    }
}
