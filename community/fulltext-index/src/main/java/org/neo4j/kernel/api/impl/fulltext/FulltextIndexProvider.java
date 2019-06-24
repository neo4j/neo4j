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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory;
import org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.createAnalyzer;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.createPropertyNames;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.isEventuallyConsistent;

class FulltextIndexProvider extends IndexProvider implements FulltextAdapter
{
    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final OperationalMode operationalMode;
    private final String defaultAnalyzerName;
    private final boolean defaultEventuallyConsistentSetting;
    private final Log log;
    private final IndexUpdateSink indexUpdateSink;
    private final IndexStorageFactory indexStorageFactory;

    FulltextIndexProvider( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory,
            FileSystemAbstraction fileSystem, Config config, TokenHolders tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode,
            JobScheduler scheduler, Log log )
    {
        super( descriptor, directoryStructureFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;
        this.log = log;

        defaultAnalyzerName = config.get( FulltextSettings.fulltext_default_analyzer );
        defaultEventuallyConsistentSetting = config.get( FulltextSettings.eventually_consistent );
        indexUpdateSink = new IndexUpdateSink( scheduler, config.get( FulltextSettings.eventually_consistent_index_update_queue_max_length ) );
        indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
    }

    private IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage, IndexDescriptor descriptor ) throws IOException
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

    private PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId );
    }

    @Override
    public void shutdown() throws Exception
    {
        // Closing the index storage factory also closes all Lucene Directory instances.
        // This has to be done at shutdown, which happens after all of the index accessors have been closed, and thus committed any pent up changes.
        indexStorageFactory.close();
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        SchemaDescriptor schema = index.schema();
        IndexConfig indexConfig = schema.getIndexConfig();
        indexConfig = addMissingDefaultIndexConfig( indexConfig );
        schema = schema.withIndexConfig( indexConfig );
        index = index.withSchemaDescriptor( schema );
        if ( index.getCapability().equals( IndexCapability.NO_CAPABILITY ) )
        {
            index = index.withIndexCapability( getCapability( index ) );
        }
        return index;
    }

    private IndexCapability getCapability( IndexDescriptor descriptor )
    {
        return new FulltextIndexCapability( isEventuallyConsistent( descriptor.schema() ) );
    }

    @Override
    public String getPopulationFailure( IndexDescriptor descriptor ) throws IllegalStateException
    {
        String failure = getIndexStorage( descriptor.getId() ).getStoredIndexFailure();
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + descriptor.getId() + " isn't failed" );
        }
        return failure;
    }

    @Override
    public InternalIndexState getInitialState( IndexDescriptor descriptor )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
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
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        NonTransactionalTokenNameLookup tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );
        Analyzer analyzer = createAnalyzer( descriptor, tokenNameLookup );
        String[] propertyNames = createPropertyNames( descriptor, tokenNameLookup );
        DatabaseIndex<FulltextIndexReader> fulltextIndex = FulltextIndexBuilder
                .create( descriptor, config, tokenHolders.propertyKeyTokens(), analyzer, propertyNames )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( true )
                .build();
        if ( fulltextIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        log.debug( "Creating populator for fulltext schema index: %s", descriptor );
        return new FulltextIndexPopulator( descriptor, fulltextIndex, propertyNames );
    }

    @Override
    public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        NonTransactionalTokenNameLookup tokenNameLookup = new NonTransactionalTokenNameLookup( tokenHolders );
        Analyzer analyzer = createAnalyzer( descriptor, tokenNameLookup );
        String[] propertyNames = createPropertyNames( descriptor, tokenNameLookup );
        FulltextIndexBuilder fulltextIndexBuilder = FulltextIndexBuilder
                .create( descriptor, config, tokenHolders.propertyKeyTokens(), analyzer, propertyNames )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( false );
        if ( isEventuallyConsistent( descriptor.schema() ) )
        {
            fulltextIndexBuilder = fulltextIndexBuilder.withIndexUpdateSink( indexUpdateSink );
        }
        DatabaseIndex<FulltextIndexReader> fulltextIndex = fulltextIndexBuilder.build();
        fulltextIndex.open();

        FulltextIndexAccessor accessor = new FulltextIndexAccessor( indexUpdateSink, fulltextIndex, descriptor, propertyNames );
        log.debug( "Created online accessor for fulltext schema index %s: %s", descriptor, accessor );
        return accessor;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache, StorageEngineFactory storageEngineFactory )
    {
        return new SchemaIndexMigrator( fs, this, storageEngineFactory );
    }

    @Override
    public void validatePrototype( IndexPrototype prototype )
    {
        if ( prototype.getIndexType() != IndexType.FULLTEXT )
        {
            throw new IllegalArgumentException( "The '" + getProviderDescriptor().name() + "' only supports FULLTEXT index types: " + prototype );
        }
    }

    @Override
    public SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, IndexConfig indexConfig, String... properties )
    {
        if ( entityTokens.length == 0 )
        {
            throw new BadSchemaException(
                    "At least one " + ( type == EntityType.NODE ? "label" : "relationship type" ) + " must be specified when creating a fulltext index." );
        }
        if ( properties.length == 0 )
        {
            throw new BadSchemaException( "At least one property name must be specified when creating a fulltext index." );
        }
        if ( Arrays.asList( properties ).contains( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID ) )
        {
            throw new BadSchemaException( "Unable to index the property, the name is reserved for internal use " +
                    LuceneFulltextDocumentStructure.FIELD_ENTITY_ID );
        }
        int[] entityTokenIds = new int[entityTokens.length];
        try
        {
            if ( type == EntityType.NODE )
            {
                tokenHolders.labelTokens().getOrCreateIds( entityTokens, entityTokenIds );
            }
            else
            {
                tokenHolders.relationshipTypeTokens().getOrCreateIds( entityTokens, entityTokenIds );
            }
            int[] propertyIds = new int[properties.length];
            tokenHolders.propertyKeyTokens().getOrCreateIds( properties, propertyIds );

            indexConfig = addMissingDefaultIndexConfig( indexConfig );
            return SchemaDescriptor.fulltext( type, indexConfig, entityTokenIds, propertyIds );
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( "Error creating token", e );
        }
    }

    private IndexConfig addMissingDefaultIndexConfig( IndexConfig indexConfig )
    {
        indexConfig = indexConfig.withIfAbsent( FulltextIndexSettingsKeys.ANALYZER, Values.stringValue( defaultAnalyzerName ) );
        indexConfig = indexConfig.withIfAbsent( FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT, Values.booleanValue( defaultEventuallyConsistentSetting ) );
        return indexConfig;
    }

    @Override
    public void awaitRefresh()
    {
        indexUpdateSink.awaitUpdateApplication();
    }

    @Override
    public Stream<AnalyzerProvider> listAvailableAnalyzers()
    {
        return Services.loadAll( AnalyzerProvider.class ).stream();
    }
}
