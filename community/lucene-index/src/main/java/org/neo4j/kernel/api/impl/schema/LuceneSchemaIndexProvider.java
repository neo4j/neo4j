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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.populator.NonUniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.impl.schema.populator.UniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;

import static org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

public class LuceneSchemaIndexProvider extends AbstractLuceneIndexProvider<SchemaIndexDescriptor>
{
    static final int PRIORITY = 1;

    private final Monitor monitor;

    LuceneSchemaIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory, IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitor monitor, Config config, OperationalMode operationalMode )
    {
        super( PROVIDER_DESCRIPTOR, PRIORITY, directoryStructureFactory, config, operationalMode, fileSystem, directoryFactory );
        this.monitor = monitor;
    }

    static IndexDirectoryStructure.Factory defaultDirectoryStructure( File storeDir )
    {
        return IndexDirectoryStructure.directoriesByProviderKey( storeDir );
    }

    @Override
    public IndexDescriptor indexDescriptorFor( SchemaDescriptor schema, IndexDescriptor.Type type, String name, String metadata )
    {
        if ( type == GENERAL )
        {
            return SchemaIndexDescriptorFactory.forLabelBySchema( schema );
        }
        else if ( type == UNIQUE )
        {
            return SchemaIndexDescriptorFactory.uniqueForLabelBySchema( schema );
        }
        throw new UnsupportedOperationException( String.format( "This provider does not support indexes of type %s", type ) );    }

    @Override
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public boolean compatible( IndexDescriptor indexDescriptor )
    {
        return indexDescriptor instanceof SchemaIndexDescriptor;
    }

    @Override
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( indexId );
        String failure = indexStorage.getStoredIndexFailure();
        if ( failure != null )
        {
            return InternalIndexState.FAILED;
        }
        try
        {
            return indexIsOnline( indexStorage, descriptor ) ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
        }
        catch ( IOException e )
        {
            monitor.failedToOpenIndex( indexId, descriptor, "Requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                                        .withFileSystem( fileSystem )
                                        .withOperationalMode( operationalMode )
                                        .withSamplingConfig( samplingConfig )
                                        .withIndexStorage( getIndexStorage( indexId ) )
                                        .withWriterConfig( IndexWriterConfigs::population )
                                        .build();
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        if ( descriptor.type() == UNIQUE )
        {
            return new UniqueLuceneIndexPopulator( luceneIndex, descriptor );
        }
        else
        {
            return new NonUniqueLuceneIndexPopulator( luceneIndex, samplingConfig );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                                            .withOperationalMode( operationalMode )
                                            .withSamplingConfig( samplingConfig )
                                            .withIndexStorage( getIndexStorage( indexId ) )
                                            .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, descriptor );
    }
}
