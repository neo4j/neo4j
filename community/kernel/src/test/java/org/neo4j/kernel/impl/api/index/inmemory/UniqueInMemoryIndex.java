/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.updater.UniquePropertyIndexUpdater;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

class UniqueInMemoryIndex extends InMemoryIndex
{
    private final SchemaDescriptor schema;

    UniqueInMemoryIndex( SchemaIndexDescriptor descriptor )
    {
        super( descriptor );
        this.schema = descriptor.schema();
    }

    @Override
    protected IndexUpdater newUpdater( final IndexUpdateMode mode, final boolean populating )
    {
        return new UniquePropertyIndexUpdater()
        {
            @Override
            protected void flushUpdates( Iterable<IndexEntryUpdate<?>> updates )
            {
                for ( IndexEntryUpdate<?> update : updates )
                {
                    switch ( update.updateMode() )
                    {
                        case CHANGED:
                            UniqueInMemoryIndex.this.remove( update.getEntityId(), update.beforeValues() );
                            break;
                        case REMOVED:
                            UniqueInMemoryIndex.this.remove( update.getEntityId(), update.values() );
                            break;
                        default:
                            break;
                    }
                }
                for ( IndexEntryUpdate<?> update : updates )
                {
                    switch ( update.updateMode() )
                    {
                        case ADDED:
                            add( update.getEntityId(), update.values(), IndexUpdateMode.ONLINE == mode );
                            break;
                        case CHANGED:
                            add( update.getEntityId(), update.values(), IndexUpdateMode.ONLINE == mode );
                            break;
                        default:
                            break;
                    }
                }
            }
        };
    }

    @Override
    public void verifyDeferredConstraints( final PropertyAccessor accessor )
            throws IndexEntryConflictException
    {
        try
        {
            if ( schema.getPropertyIds().length == 1 )
            {
                indexData.iterateAll( new SinglePropertyValidator( accessor ) );
            }
            else
            {
                indexData.iterateAll( new MultiPropertyValidator( accessor ) );
            }
        }
        catch ( IndexEntryConflictException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private class SinglePropertyValidator implements InMemoryIndexImplementation.IndexEntryIterator
    {
        private final PropertyAccessor accessor;

        SinglePropertyValidator( PropertyAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public void visitEntry( Object key, Set<Long> nodeIds ) throws Exception
        {
            Map<Object,Long> entries = new HashMap<>();
            for ( long nodeId : nodeIds )
            {
                final Value value = accessor.getPropertyValue( nodeId, schema.getPropertyId() );
                if ( entries.containsKey( value ) )
                {
                    long existingNodeId = entries.get( value );
                    throw new IndexEntryConflictException( existingNodeId, nodeId, ValueTuple.of( value ) );
                }
                entries.put( value, nodeId );
            }
        }
    }

    private class MultiPropertyValidator implements InMemoryIndexImplementation.IndexEntryIterator
    {
        private final PropertyAccessor accessor;

        MultiPropertyValidator( PropertyAccessor accessor )
        {
            this.accessor = accessor;
        }

        @Override
        public void visitEntry( Object key, Set<Long> nodeIds ) throws Exception
        {
            Map<ValueTuple,Long> entries = new HashMap<>();
            for ( long nodeId : nodeIds )
            {
                final ValueTuple values = getValues( nodeId );
                if ( entries.containsKey( values ) )
                {
                    long existingNodeId = entries.get( values );
                    throw new IndexEntryConflictException( existingNodeId, nodeId, values );
                }
                entries.put( values, nodeId );
            }
        }

        ValueTuple getValues( long nodeId ) throws EntityNotFoundException
        {
            int[] propertyIds = schema.getPropertyIds();
            Value[] values = new Value[propertyIds.length];
            for ( int i = 0; i < values.length; i++ )
            {
                values[i] = accessor.getPropertyValue( nodeId, propertyIds[i] );
            }
            return ValueTuple.of( values );
        }
    }
}
