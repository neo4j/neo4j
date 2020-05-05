/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelTransaction;
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
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionState;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static org.neo4j.kernel.api.exceptions.Status.General.InvalidArguments;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.readOrInitialiseDescriptor;

class FulltextIndexProvider extends IndexProvider implements FulltextAdapter, AuxiliaryTransactionStateProvider
{
    private static final String TX_STATE_PROVIDER_KEY = "FULLTEXT SCHEMA INDEX TRANSACTION STATE";

    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final OperationalMode operationalMode;
    private final String defaultAnalyzerName;
    private final String defaultEventuallyConsistentSetting;
    private final AuxiliaryTransactionStateManager auxiliaryTransactionStateManager;
    private final Log log;
    private final IndexUpdateSink indexUpdateSink;
    private final ConcurrentMap<StoreIndexDescriptor,FulltextIndexAccessor> openOnlineAccessors;
    private final IndexStorageFactory indexStorageFactory;

    FulltextIndexProvider( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory,
            FileSystemAbstraction fileSystem, Config config, TokenHolders tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode,
            JobScheduler scheduler, AuxiliaryTransactionStateManager auxiliaryTransactionStateManager, Log log )
    {
        super( descriptor, directoryStructureFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;
        this.auxiliaryTransactionStateManager = auxiliaryTransactionStateManager;
        this.log = log;

        defaultAnalyzerName = config.get( FulltextConfig.fulltext_default_analyzer );
        defaultEventuallyConsistentSetting = Boolean.toString( config.get( FulltextConfig.eventually_consistent ) );
        indexUpdateSink = new IndexUpdateSink( scheduler, config.get( FulltextConfig.eventually_consistent_index_update_queue_max_length ) );
        openOnlineAccessors = new ConcurrentHashMap<>();
        indexStorageFactory = buildIndexStorageFactory( fileSystem, directoryFactory );
    }

    private IndexStorageFactory buildIndexStorageFactory( FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory )
    {
        return new IndexStorageFactory( directoryFactory, fileSystem, directoryStructure() );
    }

    private boolean indexIsOnline( PartitionedIndexStorage indexStorage, StoreIndexDescriptor descriptor ) throws IOException
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
    public void start() throws Throwable
    {
        super.start();
        auxiliaryTransactionStateManager.registerProvider( this );
    }

    @Override
    public void stop() throws Throwable
    {
        auxiliaryTransactionStateManager.unregisterProvider( this );
        indexStorageFactory.close();
    }

    @Override
    public IndexCapability getCapability( StoreIndexDescriptor descriptor )
    {
        FulltextIndexDescriptor fulltextIndexDescriptor;
        if ( descriptor instanceof FulltextIndexDescriptor )
        {
            // We got our own index descriptor type, so we can ask it directly.
            fulltextIndexDescriptor = (FulltextIndexDescriptor) descriptor;
            return new FulltextIndexCapability( fulltextIndexDescriptor.isEventuallyConsistent() );
        }
        SchemaDescriptor schema = descriptor.schema();
        if ( schema instanceof FulltextSchemaDescriptor )
        {
            // The fulltext schema descriptor is readily available with our settings.
            // This could be the situation where the index creation is about to be committed.
            // In that case, the schema descriptor is our own legit type, but the StoreIndexDescriptor is generic.
            FulltextSchemaDescriptor fulltextSchemaDescriptor = (FulltextSchemaDescriptor) schema;
            return new FulltextIndexCapability( fulltextSchemaDescriptor.isEventuallyConsistent() );
        }
        // The schema descriptor is probably a generic multi-token descriptor.
        // This happens if it was loaded from the schema store instead of created by our provider.
        // This would be the case when the IndexingService is starting up, and if so, we probably have an online accessor that we can ask instead.
        FulltextIndexAccessor accessor = getOpenOnlineAccessor( descriptor );
        if ( accessor != null )
        {
            fulltextIndexDescriptor = accessor.getDescriptor();
            return new FulltextIndexCapability( fulltextIndexDescriptor.isEventuallyConsistent() );
        }
        // All of the above has failed, so we need to load the settings in from the storage directory of the index.
        // This situation happens during recovery.
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        fulltextIndexDescriptor =
                readOrInitialiseDescriptor( descriptor, defaultAnalyzerName, tokenHolders.propertyKeyTokens(), indexStorage.getIndexFolder(), fileSystem );
        return new FulltextIndexCapability( fulltextIndexDescriptor.isEventuallyConsistent() );
    }

    @Override
    public IndexDescriptor bless( IndexDescriptor index ) throws MisconfiguredIndexException
    {
        if ( !(index.schema() instanceof FulltextSchemaDescriptor) )
        {
            // The fulltext index provider only support fulltext indexes.
            throw new MisconfiguredIndexException( InvalidArguments, "The index provider '" + getProviderDescriptor() + "' only supports fulltext index " +
                    "descriptors. Make sure that fulltext indexes are created using the relevant fulltext index procedures." );
        }
        return super.bless( index );
    }

    @Override
    public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
    {
        String failure = getIndexStorage( descriptor.getId() ).getStoredIndexFailure();
        if ( failure == null )
        {
            throw new IllegalStateException( "Index " + descriptor.getId() + " isn't failed" );
        }
        return failure;
    }

    @Override
    public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
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
    public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        FulltextIndexDescriptor fulltextIndexDescriptor = readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerName, tokenHolders.propertyKeyTokens(), indexStorage.getIndexFolder(), fileSystem );
        DatabaseIndex<FulltextIndexReader> fulltextIndex = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config, tokenHolders.propertyKeyTokens() )
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
        return new FulltextIndexPopulator( fulltextIndexDescriptor, fulltextIndex,
                () -> FulltextIndexSettings.saveFulltextIndexSettings( fulltextIndexDescriptor, indexStorage.getIndexFolder(), fileSystem ) );
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );

        FulltextIndexDescriptor fulltextIndexDescriptor = readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerName, tokenHolders.propertyKeyTokens(), indexStorage.getIndexFolder(), fileSystem );
        FulltextIndexBuilder fulltextIndexBuilder = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config, tokenHolders.propertyKeyTokens() )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( false );
        if ( fulltextIndexDescriptor.isEventuallyConsistent() )
        {
            fulltextIndexBuilder = fulltextIndexBuilder.withIndexUpdateSink( indexUpdateSink );
        }
        DatabaseFulltextIndex fulltextIndex = fulltextIndexBuilder.build();
        fulltextIndex.open();

        Runnable onClose = () -> openOnlineAccessors.remove( descriptor );
        FulltextIndexAccessor accessor = new FulltextIndexAccessor( indexUpdateSink, fulltextIndex, fulltextIndexDescriptor, onClose );
        openOnlineAccessors.put( descriptor, accessor );
        log.debug( "Created online accessor for fulltext schema index %s: %s", descriptor, accessor );
        return accessor;
    }

    FulltextIndexAccessor getOpenOnlineAccessor( StoreIndexDescriptor descriptor )
    {
        return openOnlineAccessors.get( descriptor );
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( final FileSystemAbstraction fs, PageCache pageCache )
    {
        return new SchemaIndexMigrator( fs, this );
    }

    @Override
    public SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, Properties indexConfiguration, String... properties )
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
        if ( type == EntityType.NODE )
        {
            tokenHolders.labelTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        else
        {
            tokenHolders.relationshipTypeTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        int[] propertyIds = Arrays.stream( properties ).mapToInt( tokenHolders.propertyKeyTokens()::getOrCreateId ).toArray();

        SchemaDescriptor schema = SchemaDescriptorFactory.multiToken( entityTokenIds, type, propertyIds );
        indexConfiguration.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_ANALYZER, defaultAnalyzerName );
        indexConfiguration.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT, defaultEventuallyConsistentSetting );
        String analyzerName = indexConfiguration.getProperty( FulltextIndexSettings.INDEX_CONFIG_ANALYZER );
        try
        {
            FulltextIndexSettings.createAnalyzer( analyzerName );
        }
        catch ( RuntimeException e )
        {
            throw new IllegalArgumentException( "No such analyzer: " + analyzerName, e );
        }
        return new FulltextSchemaDescriptor( schema, indexConfiguration );
    }

    @Override
    public ScoreEntityIterator query( KernelTransaction ktx, String indexName, String queryString ) throws IndexNotFoundKernelException, ParseException
    {
        KernelTransactionImplementation kti = (KernelTransactionImplementation) ktx;
        AllStoreHolder allStoreHolder = (AllStoreHolder) kti.dataRead();
        IndexReference indexReference = kti.schemaRead().indexGetForName( indexName );
        FulltextIndexReader fulltextIndexReader;
        if ( kti.hasTxStateWithChanges() && !isEventuallyConsistent( indexReference ) )
        {
            FulltextAuxiliaryTransactionState auxiliaryTxState = (FulltextAuxiliaryTransactionState) allStoreHolder.auxiliaryTxState( TX_STATE_PROVIDER_KEY );
            fulltextIndexReader = auxiliaryTxState.indexReader( indexReference, kti );
        }
        else
        {
            IndexReader indexReader = allStoreHolder.indexReader( indexReference, false );
            fulltextIndexReader = (FulltextIndexReader) indexReader;
        }
        return fulltextIndexReader.query( queryString );
    }

    private boolean isEventuallyConsistent( IndexReference indexReference )
    {
        if ( indexReference instanceof CapableIndexDescriptor )
        {
            CapableIndexDescriptor index = (CapableIndexDescriptor) indexReference;
            return index.isEventuallyConsistent();
        }
        return ((FulltextSchemaDescriptor) indexReference.schema()).isEventuallyConsistent();
    }

    @Override
    public void awaitRefresh()
    {
        indexUpdateSink.awaitUpdateApplication();
    }

    @Override
    public Stream<AnalyzerProvider> listAvailableAnalyzers()
    {
        Iterable<AnalyzerProvider> providers = AnalyzerProvider.load( AnalyzerProvider.class );
        return StreamSupport.stream( providers.spliterator(), false );
    }

    @Override
    public Object getIdentityKey()
    {
        return TX_STATE_PROVIDER_KEY;
    }

    @Override
    public AuxiliaryTransactionState createNewAuxiliaryTransactionState()
    {
        return new FulltextAuxiliaryTransactionState( this, log );
    }
}
