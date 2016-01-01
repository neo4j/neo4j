/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UniquePropertyIndexUpdater;

class UniqueInMemoryIndex extends InMemoryIndex implements UniquePropertyIndexUpdater.Lookup
{
    private final int propertyKeyId;

    public UniqueInMemoryIndex( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    protected IndexUpdater newUpdater( final IndexUpdateMode mode, final boolean populating )
    {
        return new UniquePropertyIndexUpdater( this )
        {
            @Override
            protected void flushUpdates( Iterable<NodePropertyUpdate> updates )
                    throws IOException, IndexEntryConflictException
            {
                for ( NodePropertyUpdate update : updates )
                {
                    switch ( update.getUpdateMode() )
                    {
                        case CHANGED:
                        case REMOVED:
                            UniqueInMemoryIndex.this.remove( update.getNodeId(), update.getValueBefore() );
                    }
                }
                for ( NodePropertyUpdate update : updates )
                {
                    switch ( update.getUpdateMode() )
                    {
                        case ADDED:
                        case CHANGED:
                            add( update.getNodeId(), update.getValueAfter(), IndexUpdateMode.ONLINE == mode );
                    }
                }
            }

            @Override
            public Reservation validate( Iterable<NodePropertyUpdate> updates ) throws IOException
            {
                return Reservation.EMPTY;
            }

            @Override
            public void remove( Iterable<Long> nodeIds )
            {
                for ( long nodeId : nodeIds )
                {
                    UniqueInMemoryIndex.this.remove( nodeId );
                }
            }
        };
    }

    @Override
    public Long currentlyIndexedNode( Object value ) throws IOException
    {
        PrimitiveLongIterator nodes = lookup( value );
        return nodes.hasNext() ? nodes.next() : null;
    }

    @Override
    public void verifyDeferredConstraints( final PropertyAccessor accessor ) throws Exception
    {
        indexData.iterateAll( new InMemoryIndexImplementation.IndexEntryIterator()
        {
            @Override
            public void visitEntry( Object key, Set<Long> nodeIds ) throws Exception
            {
                List<Entry> entries = new ArrayList<>();
                for (long nodeId : nodeIds )
                {
                    Property property = accessor.getProperty( nodeId, propertyKeyId );
                    Object value = property.value();
                    for ( Entry current : entries )
                    {
                        if (current.property.valueEquals( value ))
                        {
                            throw new PreexistingIndexEntryConflictException( value, current.nodeId, nodeId );
                        }
                    }
                    entries.add( new Entry( nodeId, property) );
                }
            }
        } );
    }

    private static class Entry {
        long nodeId;
        Property property;

        private Entry( long nodeId, Property property )
        {
            this.nodeId = nodeId;
            this.property = property;
        }
    }
}
