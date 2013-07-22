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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.PropertyUpdateUniquenessValidator;

class UniqueInMemoryIndex extends InMemoryIndex implements PropertyUpdateUniquenessValidator.Lookup
{
    private final Map<Object, Long> indexData = new HashMap<Object, Long>();

    @Override
    IndexPopulator getPopulator()
    {
        return new InMemoryIndex.Populator()
        {
            @Override
            public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
            {
                Long previous = indexData.get( propertyValue );
                if ( previous != null )
                {
                    throw new PreexistingIndexEntryConflictException( propertyValue, previous, nodeId );
                }

                super.add( nodeId, propertyValue );
            }
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
                return new UniqueInMemoryIndexReader( indexData );
            }

            @Override
            public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException,
                    IndexEntryConflictException
            {
                PropertyUpdateUniquenessValidator.validateUniqueness( updates, UniqueInMemoryIndex.this );

                super.updateAndCommit( updates );
            }
        };
    }

    @Override
    protected void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException
    {
        indexData.put( propertyValue, nodeId );
    }

    @Override
    protected void remove( long nodeId, Object propertyValue )
    {
        long curNodeId = indexData.get( propertyValue );
        if ( nodeId == curNodeId )
        {
            indexData.remove( propertyValue );
        }
    }


    @Override
    protected void clear()
    {
        indexData.clear();
    }

    @Override
    public Long currentlyIndexedNode( Object value ) throws IOException
    {
        return indexData.get( value );
    }
}
