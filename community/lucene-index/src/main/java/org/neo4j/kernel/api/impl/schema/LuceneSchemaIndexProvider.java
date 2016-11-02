/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.populator.NonUniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.impl.schema.populator.UniqueLuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    private final IndexStorageFactory indexStorageFactory;
    private final Log log;
    private Config config;
    private OperationalMode operationalMode;
    private FileSystemAbstraction fileSystem;

    public LuceneSchemaIndexProvider( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory,
                                      File storeDir, LogProvider logging, Config config,
                                      OperationalMode operationalMode )
    {
        super( LuceneSchemaIndexProviderFactory.PROVIDER_DESCRIPTOR, 1 );
        File schemaIndexStoreFolder = getSchemaIndexStoreDirectory( storeDir );
        this.indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory, schemaIndexStoreFolder );
        this.fileSystem = fileSystem;
        this.config = config;
        this.operationalMode = operationalMode;
        this.log = logging.getLog( getClass() );
    }

    /**
     * Visible <b>only</b> for testing.
     */
    protected IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem,
                                                            DirectoryFactory directoryFactory,
                                                            File schemaIndexStoreFolder )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, schemaIndexStoreFolder );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor,
            IndexConfiguration indexConfiguration, IndexSamplingConfig samplingConfig )
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create()
                                        .withFileSystem( fileSystem )
                                        .withIndexConfig( indexConfiguration )
                                        .withConfig( config )
                                        .withOperationalMode( operationalMode )
                                        .withSamplingConfig( samplingConfig )
                                        .withIndexStorage( getIndexStorage( indexId ) )
                                        .withWriterConfig( IndexWriterConfigs::population )
                                        .build();
        if ( luceneIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        if ( indexConfiguration.isUnique() )
        {
            return new UniqueLuceneIndexPopulator( luceneIndex, descriptor );
        }
        else
        {
            return new NonUniqueLuceneIndexPopulator( luceneIndex, samplingConfig );
        }
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration indexConfiguration,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        SchemaIndex luceneIndex = LuceneSchemaIndexBuilder.create()
                                            .withIndexConfig( indexConfiguration )
                                            .withConfig( config )
                                            .withOperationalMode( operationalMode )
                                            .withSamplingConfig( samplingConfig )
                                            .withIndexStorage( getIndexStorage( indexId ) )
                                            .build();
        luceneIndex.open();
        return new LuceneIndexAccessor( luceneIndex );
    }

    @Override
    public void shutdown() throws Throwable
    {   // Nothing to shut down
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( indexId );
        String failure = indexStorage.getStoredIndexFailure();
        if ( failure != null )
        {
            return InternalIndexState.FAILED;
        }
        try
        {
            return indexIsOnline( indexStorage ) ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
        }
        catch ( IOException e )
        {
            log.error( "Failed to open index:" + indexId + ", requesting re-population.", e );
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache,
            LabelScanStoreProvider labelScanStoreProvider )
    {
        return new SchemaIndexMigrator( fs, this, labelScanStoreProvider );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
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
        return indexStorageFactory.indexStorageOf( indexId, config.get( GraphDatabaseSettings.archive_failed_index ) );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage ) throws IOException
    {
        try ( SchemaIndex index = LuceneSchemaIndexBuilder.create().withIndexStorage( indexStorage ).build() )
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
