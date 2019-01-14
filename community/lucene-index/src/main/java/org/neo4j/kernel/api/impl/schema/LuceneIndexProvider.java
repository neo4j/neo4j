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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.populator.NonUniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.impl.schema.populator.UniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;

import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.UNIQUE;

public class LuceneIndexProvider extends IndexProvider
{
    static final int PRIORITY = 1;

    private final IndexStorageFactory indexStorageFactory;
    private final Config config;
    private final OperationalMode operationalMode;
    private final FileSystemAbstraction fileSystem;
    private final Monitor monitor;

    public LuceneIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
                                IndexDirectoryStructure.Factory directoryStructureFactory, Monitor monitor, Config config,
                                OperationalMode operationalMode )
    {
        super( LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR, PRIORITY, directoryStructureFactory );
        this.monitor = monitor;
        this.indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.operationalMode = operationalMode;
    }

    public static IndexDirectoryStructure.Factory defaultDirectoryStructure( File storeDir )
    {
        return IndexDirectoryStructure.directoriesByProviderKey( storeDir );
    }

    /**
     * Visible <b>only</b> for testing.
     */
    protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
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
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create( descriptor, config )
                                            .withOperationalMode( operationalMode )
                                            .withSamplingConfig( samplingConfig )
                                            .withIndexStorage( getIndexStorage( indexId ) )
                                            .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex, descriptor );
    }

    @Override
    public void shutdown()
    {   // Nothing to shut down
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
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache )
    {
        return new SchemaIndexMigrator( fs, this );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        String failure = getIndexStorage( indexId ).getStoredIndexFailure();
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + indexId + " isn't failed" );
        }
        return failure;
    }

    private PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage, SchemaIndexDescriptor descriptor ) throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create( descriptor, config ).withIndexStorage( indexStorage ).build() )
        {
            if ( index.exists() )
            {
                index.open();
                return index.isOnline();
            }
            return false;
        }
    }
}
