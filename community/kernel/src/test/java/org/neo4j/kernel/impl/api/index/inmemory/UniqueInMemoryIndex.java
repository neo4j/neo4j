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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UniquePropertyIndexUpdater;

class UniqueInMemoryIndex extends InMemoryIndex
{
    private final int propertyKeyId;

    private final PrimitiveLongVisitor<RuntimeException> removeFromIndex = new PrimitiveLongVisitor<RuntimeException>()
    {
        @Override
        public boolean visited( long nodeId )
        {
            UniqueInMemoryIndex.this.remove( nodeId );
            return false;
        }
    };

    public UniqueInMemoryIndex( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    protected IndexUpdater newUpdater( final IndexUpdateMode mode, final boolean populating )
    {
        return new UniquePropertyIndexUpdater()
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
            indexData.iterateAll( new InMemoryIndexImplementation.IndexEntryIterator()
            {
                @Override
                public void visitEntry( Object key, Set<Long> nodeIds ) throws Exception
                {
                    Map<Object,Long> entries = new HashMap<>();
                    for ( long nodeId : nodeIds )
                    {
                        final Object value = accessor.getProperty( nodeId, propertyKeyId ).value();
                        if ( entries.containsKey( value ) )
                        {
                            long existingNodeId = entries.get( value );
                            throw new PreexistingIndexEntryConflictException( value, existingNodeId, nodeId );
                        }
                        entries.put( value, nodeId );
                    }
                }
            } );
        }
        catch ( PreexistingIndexEntryConflictException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
