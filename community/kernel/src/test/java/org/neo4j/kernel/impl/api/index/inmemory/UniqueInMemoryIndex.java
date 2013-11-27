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

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.UniquePropertyIndexUpdater;

class UniqueInMemoryIndex extends InMemoryIndex implements UniquePropertyIndexUpdater.Lookup
{
    @Override
    protected void add( long nodeId, Object propertyValue, boolean applyIdempotently )
            throws IndexEntryConflictException, IOException
    {
        PrimitiveLongIterator nodes = lookup( propertyValue );
        if ( nodes.hasNext() )
        {
            throw new PreexistingIndexEntryConflictException( propertyValue, nodes.next(), nodeId );
        }
        super.add( nodeId, propertyValue, applyIdempotently );
    }

    @Override
    protected IndexUpdater newUpdater( final IndexUpdateMode mode, final boolean populating )
    {
        return new UniquePropertyIndexUpdater( UniqueInMemoryIndex.this )
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
}
