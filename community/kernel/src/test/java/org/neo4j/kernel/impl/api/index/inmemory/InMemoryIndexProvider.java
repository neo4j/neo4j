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

import java.util.Map;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

public class InMemoryIndexProvider extends IndexProvider
{
    private final Map<Long, InMemoryIndex> indexes;

    public InMemoryIndexProvider()
    {
        this( 0 );
    }

    public InMemoryIndexProvider( int prio )
    {
        this( prio, new CopyOnWriteHashMap<>() );
    }

    private InMemoryIndexProvider( int prio, Map<Long, InMemoryIndex> indexes )
    {
        super( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, prio, IndexDirectoryStructure.NONE );
        this.indexes = indexes;
    }

    @Override
    public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
    {
        InMemoryIndex index = indexes.get( descriptor.getId() );
        return index != null ? index.getState() : InternalIndexState.POPULATING;
    }

    @Override
    public IndexCapability getCapability()
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    @Override
    public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        InMemoryIndex index = descriptor.type() == UNIQUE
                ? new UniqueInMemoryIndex( descriptor ) : new InMemoryIndex( descriptor );
        indexes.put( descriptor.getId(), index );
        return index.getPopulator();
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        InMemoryIndex index = indexes.get( descriptor.getId() );
        if ( index == null || index.getState() != InternalIndexState.ONLINE )
        {
            throw new IllegalStateException( "Index " + descriptor.getId() + " not online yet" );
        }
        if ( descriptor.type() == UNIQUE && !(index instanceof UniqueInMemoryIndex) )
        {
            throw new IllegalStateException(
                    String.format( "The index [%s] was not created as a unique index.", descriptor.getId() )
            );
        }
        return index.getOnlineAccessor();
    }

    @Override
    public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
    {
        String failure = indexes.get( descriptor.getId() ).failure;
        if ( failure == null )
        {
            throw new IllegalStateException();
        }
        return failure;
    }

    public InMemoryIndexProvider snapshot()
    {
        Map<Long, InMemoryIndex> copy = new CopyOnWriteHashMap<>();
        for ( Map.Entry<Long, InMemoryIndex> entry : indexes.entrySet() )
        {
            copy.put( entry.getKey(), entry.getValue().snapshot() );
        }
        return new InMemoryIndexProvider( priority, copy );
    }

    public boolean dataEquals( InMemoryIndexProvider other )
    {
        for ( Map.Entry<Long,InMemoryIndex> entry : indexes.entrySet() )
        {
            InMemoryIndex otherIndex = other.indexes.get( entry.getKey() );
            if ( otherIndex == null || !entry.getValue().hasSameContentsAs( otherIndex ) )
            {
                return false;
            }
        }
        return true;
    }
}
