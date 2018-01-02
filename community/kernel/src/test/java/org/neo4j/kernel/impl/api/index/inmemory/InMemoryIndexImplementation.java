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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.kernel.api.index.IndexReader;

abstract class InMemoryIndexImplementation implements IndexReader, BoundedIterable<Long>
{
    abstract void initialize();

    abstract void drop();

    @Override
    public final PrimitiveLongIterator seek( Object value )
    {
        return doIndexSeek( encode( value ) );
    }

    final boolean add( long nodeId, Object propertyValue, boolean applyIdempotently )
    {
        return doAdd( encode( propertyValue ), nodeId, applyIdempotently );
    }

    final void remove( long nodeId, Object propertyValue )
    {
        doRemove( encode( propertyValue ), nodeId );
    }

    abstract PrimitiveLongIterator doIndexSeek( Object propertyValue );

    abstract boolean doAdd( Object propertyValue, long nodeId, boolean applyIdempotently );

    abstract void doRemove( Object propertyValue, long nodeId );

    abstract void remove( long nodeId );

    abstract void iterateAll( IndexEntryIterator iterator ) throws Exception;

    @Override
    public void close()
    {
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
