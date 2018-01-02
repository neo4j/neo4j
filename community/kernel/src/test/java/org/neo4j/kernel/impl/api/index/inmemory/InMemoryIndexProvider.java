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

import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class InMemoryIndexProvider extends SchemaIndexProvider
{
    private final Map<Long, InMemoryIndex> indexes;

    public InMemoryIndexProvider()
    {
        this( 0 );
    }

    public InMemoryIndexProvider( int prio )
    {
        this( prio, new CopyOnWriteHashMap<Long, InMemoryIndex>() );
    }

    private InMemoryIndexProvider( int prio, Map<Long, InMemoryIndex> indexes )
    {
        super( InMemoryIndexProviderFactory.PROVIDER_DESCRIPTOR, prio );
        this.indexes = indexes;
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        InMemoryIndex index = indexes.get( indexId );
        return index != null ? index.getState() : InternalIndexState.POPULATING;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexConfiguration config,
                                        IndexSamplingConfig samplingConfig )
    {
        InMemoryIndex index = config.isUnique()
                ? new UniqueInMemoryIndex( descriptor.getPropertyKeyId() ) : new InMemoryIndex();
        indexes.put( indexId, index );
        return index.getPopulator();
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration indexConfig,
                                            IndexSamplingConfig samplingConfig )
    {
        InMemoryIndex index = indexes.get( indexId );
        if ( index == null || index.getState() != InternalIndexState.ONLINE )
        {
            throw new IllegalStateException( "Index " + indexId + " not online yet" );
        }
        if ( indexConfig.isUnique() && !(index instanceof UniqueInMemoryIndex) )
        {
            throw new IllegalStateException(
                    String.format( "The index [%s] was not created as a unique index.", indexId )
            );
        }
        return index.getOnlineAccessor();
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        String failure = indexes.get( indexId ).failure;
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
}
