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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;

public class UpdateCapturingIndexProvider extends IndexProvider
{
    private final IndexProvider actual;
    private final Map<Long,UpdateCapturingIndexAccessor> indexes = new ConcurrentHashMap<>();
    private final Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates;

    public UpdateCapturingIndexProvider( IndexProvider actual, Map<Long,Collection<IndexEntryUpdate<?>>> initialUpdates )
    {
        super( actual );
        this.actual = actual;
        this.initialUpdates = initialUpdates;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return actual.getPopulator( indexId, descriptor, samplingConfig );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
            throws IOException
    {
        IndexAccessor actualAccessor = actual.getOnlineAccessor( indexId, descriptor, samplingConfig );
        return indexes.computeIfAbsent( indexId, id -> new UpdateCapturingIndexAccessor( actualAccessor, initialUpdates.get( id ) ) );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        return actual.getPopulationFailure( indexId, descriptor );
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        return actual.getInitialState( indexId, descriptor );
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return actual.getCapability( schemaIndexDescriptor );
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return actual.storeMigrationParticipant( fs, pageCache );
    }

    public Map<Long,Collection<IndexEntryUpdate<?>>> snapshot()
    {
        Map<Long,Collection<IndexEntryUpdate<?>>> result = new HashMap<>();
        indexes.forEach( ( indexId, index ) -> result.put( indexId, index.snapshot() ) );
        return result;
    }
}
