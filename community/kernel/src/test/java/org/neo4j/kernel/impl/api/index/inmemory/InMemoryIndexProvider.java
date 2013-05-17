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

import java.util.Map;

import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class InMemoryIndexProvider extends SchemaIndexProvider
{
    private final Map<Long, InMemoryIndex> indexes = new CopyOnWriteHashMap<Long, InMemoryIndex>();

    public InMemoryIndexProvider()
    {
        super( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, 0 );
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        InMemoryIndex index = indexes.get( indexId );
        return index != null ? index.getState() : InternalIndexState.POPULATING;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexConfiguration config )
    {
        InMemoryIndex index = config.isUnique() ? new UniqueInMemoryIndex() : new NonUniqueInMemoryIndex();
        indexes.put( indexId, index );
        return index.getPopulator();
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config )
    {
        InMemoryIndex index = indexes.get( indexId );
        if ( index == null || index.getState() != InternalIndexState.ONLINE )
        {
            throw new IllegalStateException( "Index " + indexId + " not online yet" );
        }
        if ( config.isUnique() && !(index instanceof UniqueInMemoryIndex) )
        {
            throw new IllegalStateException( String.format( "The index [%s] was not created as a unique index.",
                    indexId ) );
        }
        return index.getOnlineAccessor();
    }
}
