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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.updater.UniquePropertyIndexUpdater;

class UniqueInMemoryIndex extends InMemoryIndex
{
    private final LabelSchemaDescriptor schema;

    private final PrimitiveLongVisitor<RuntimeException> removeFromIndex = new PrimitiveLongVisitor<RuntimeException>()
    {
        @Override
        public boolean visited( long nodeId )
        {
            UniqueInMemoryIndex.this.remove( nodeId );
            return false;
        }
    };

    UniqueInMemoryIndex( LabelSchemaDescriptor schema )
    {
        this.schema = schema;
    }

    @Override
    protected IndexUpdater newUpdater( final IndexUpdateMode mode, final boolean populating )
    {
        return new UniquePropertyIndexUpdater()
        {
            @Override
            protected void flushUpdates( Iterable<IndexEntryUpdate> updates )
                    throws IOException, IndexEntryConflictException
            {
                for ( IndexEntryUpdate update : updates )
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
                for ( IndexEntryUpdate update : updates )
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

            @Override
            public void remove( PrimitiveLongSet nodeIds )
            {
                nodeIds.visitKeys( removeFromIndex );
            }
        };
    }

    @Override
    public void verifyDeferredConstraints( final PropertyAccessor accessor )
            throws IndexEntryConflictException, IOException
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
                final Object value = accessor.getProperty( nodeId, schema.getPropertyId() ).value();
                if ( entries.containsKey( value ) )
                {
                    long existingNodeId = entries.get( value );
                    throw new IndexEntryConflictException( existingNodeId, nodeId, OrderedPropertyValues.ofUndefined( value ) );
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
            Map<OrderedPropertyValues,Long> entries = new HashMap<>();
            for ( long nodeId : nodeIds )
            {
                final OrderedPropertyValues values = getValues( nodeId );
                if ( entries.containsKey( values ) )
                {
                    long existingNodeId = entries.get( values );
                    throw new IndexEntryConflictException( existingNodeId, nodeId, values );
                }
                entries.put( values, nodeId );
            }
        }

        OrderedPropertyValues getValues( long nodeId ) throws PropertyNotFoundException, EntityNotFoundException
        {
            int[] propertyIds = schema.getPropertyIds();
            Object[] values = new Object[propertyIds.length];
            for ( int i = 0; i < values.length; i++ )
            {
                values[i] = accessor.getProperty( nodeId, propertyIds[i] ).value();
            }
            return OrderedPropertyValues.ofUndefined( values );
        }
    }
}
