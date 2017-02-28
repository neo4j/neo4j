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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.storageengine.api.schema.IndexReader;

abstract class InMemoryIndexImplementation implements IndexReader, BoundedIterable<Long>
{
    abstract void initialize();

    abstract void drop();

    public final PrimitiveLongIterator seek( Object... values )
    {
        return doIndexSeek( encode( values ) );
    }

    final boolean add( long nodeId, boolean applyIdempotently, Object... propertyValues )
    {
        return doAdd( nodeId, applyIdempotently, encode( propertyValues ) );
    }

    final void remove( long nodeId, Object... propertyValues )
    {
        doRemove( nodeId, encode( propertyValues ) );
    }

    @Override
    public final long countIndexedNodes( long nodeId, Object... propertyValues )
    {
        return doCountIndexedNodes( nodeId, encode( propertyValues ) );
    }

    protected abstract long doCountIndexedNodes( long nodeId, Object... encode );

    abstract PrimitiveLongIterator doIndexSeek( Object... propertyValue );

    abstract boolean doAdd( long nodeId, boolean applyIdempotently, Object... propertyValue );

    abstract void doRemove( long nodeId, Object... propertyValue );

    abstract void remove( long nodeId );

    abstract void iterateAll( IndexEntryIterator iterator ) throws Exception;

    @Override
    public void close()
    {
    }

    private static Object[] encode( Object... propertyValues )
    {
        for ( int i = 0; i < propertyValues.length; i++ )
        {

            if ( propertyValues[i] instanceof Number )
            {
                propertyValues[i] = ((Number) propertyValues[i]).doubleValue();
            }

            if ( propertyValues[i] instanceof Character )
            {
                propertyValues[i] = propertyValues[i].toString();
            }

            if ( propertyValues[i].getClass().isArray() )
            {
                propertyValues[i] = new ArrayKey( ArrayEncoder.encode( propertyValues[i] ) );
            }
        }

        return propertyValues;
    }

    static class ArrayKey
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

    abstract InMemoryIndexImplementation snapshot();

    protected interface IndexEntryIterator
    {
        void visitEntry( Object key, Set<Long> nodeId ) throws Exception;
    }
}
