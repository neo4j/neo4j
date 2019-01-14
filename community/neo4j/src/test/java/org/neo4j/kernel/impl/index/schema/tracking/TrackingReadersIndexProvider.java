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
package org.neo4j.kernel.impl.index.schema.tracking;

import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;

public class TrackingReadersIndexProvider extends IndexProvider
{
    private final IndexProvider indexProvider;

    public TrackingReadersIndexProvider( IndexProvider copySource )
    {
        super( copySource );
        this.indexProvider = copySource;
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new TrackingReadersIndexAccessor( indexProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ) );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        return indexProvider.getPopulationFailure( indexId, descriptor );
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        return indexProvider.getInitialState( indexId, descriptor );
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return indexProvider.getCapability( schemaIndexDescriptor );
    }

    @Override
    public Descriptor getProviderDescriptor()
    {
        return indexProvider.getProviderDescriptor();
    }

    @Override
    public int compareTo( IndexProvider o )
    {
        return indexProvider.compareTo( o );
    }

    @Override
    public boolean equals( Object o )
    {
        return indexProvider.equals( o );
    }

    @Override
    public int hashCode()
    {
        return indexProvider.hashCode();
    }

    @Override
    public IndexDirectoryStructure directoryStructure()
    {
        return indexProvider.directoryStructure();
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return indexProvider.storeMigrationParticipant( fs, pageCache );
    }

    @Override
    public void init() throws Throwable
    {
        indexProvider.init();
    }

    @Override
    public void start() throws Throwable
    {
        indexProvider.start();
    }

    @Override
    public void stop() throws Throwable
    {
        indexProvider.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        indexProvider.shutdown();
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        return indexProvider.getPopulator( indexId, descriptor, samplingConfig );
    }
}
