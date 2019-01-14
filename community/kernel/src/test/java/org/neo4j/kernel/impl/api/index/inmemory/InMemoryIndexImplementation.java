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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.index.ArrayEncoder;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.AbstractIndexReader;
import org.neo4j.values.storable.Value;

abstract class InMemoryIndexImplementation extends AbstractIndexReader implements BoundedIterable<Long>
{
    InMemoryIndexImplementation( SchemaIndexDescriptor descriptor )
    {
        super( descriptor );
    }

    abstract void initialize();

    abstract void drop();

    public final PrimitiveLongResourceIterator seek( Value... values )
    {
        return doIndexSeek( encode( values ) );
    }

    final boolean add( long nodeId, boolean applyIdempotently, Value... propertyValues )
    {
        return doAdd( nodeId, applyIdempotently, encode( propertyValues ) );
    }

    final void remove( long nodeId, Value... propertyValues )
    {
        doRemove( nodeId, encode( propertyValues ) );
    }

    @Override
    public final long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        return doCountIndexedNodes( nodeId, encode( propertyValues ) );
    }

    protected abstract long doCountIndexedNodes( long nodeId, Object... encode );

    abstract PrimitiveLongResourceIterator doIndexSeek( Object... propertyValue );

    abstract boolean doAdd( long nodeId, boolean applyIdempotently, Object... propertyValue );

    abstract void doRemove( long nodeId, Object... propertyValue );

    abstract void remove( long nodeId );

    abstract void iterateAll( IndexEntryIterator iterator ) throws Exception;

    @Override
    public void close()
    {
    }

    private static Object[] encode( Value[] propertyValues )
    {
        Object[] encoded = new Object[propertyValues.length];
        for ( int i = 0; i < propertyValues.length; i++ )
        {
            encoded[i] = encode( propertyValues[i] );
        }

        return encoded;
    }

    private static Object encode( Value value )
    {
        Object asObject = value.asObject();
        if ( asObject instanceof Number )
        {
            asObject = ((Number) asObject).doubleValue();
        }
        else if ( asObject instanceof Character )
        {
            asObject = asObject.toString();
        }
        else if ( asObject.getClass().isArray() )
        {
            asObject = new ArrayKey( ArrayEncoder.encode( value ) );
        }
        return asObject;
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

    abstract boolean hasSameContentsAs( InMemoryIndexImplementation other );
}
