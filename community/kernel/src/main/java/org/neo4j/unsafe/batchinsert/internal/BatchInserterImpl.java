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
package org.neo4j.unsafe.batchinsert.internal;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.DatabaseKernelExtensions;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensionFailureStrategies;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.schema.BaseNodeConstraintCreator;
import org.neo4j.kernel.impl.coreapi.schema.IndexCreatorImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;
import org.neo4j.kernel.impl.coreapi.schema.NodeKeyConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyCreator;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyDeleter;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyTraverser;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RelationshipCreator;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RelationshipGroupGetter;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.locker.GlobalStoreLocker;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchRelationship;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.neo4j.collection.PrimitiveLongCollections.map;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.kernel.impl.api.index.IndexingService.NO_MONITOR;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;
import static org.neo4j.util.Preconditions.checkState;

public class BatchInserterImpl implements BatchInserter, IndexConfigStoreProvider
{
    private final LifeSupport life;
    private final NeoStores neoStores;
    private final IndexConfigStore indexStore;
    private final DatabaseLayout databaseLayout;
    private final TokenHolders tokenHolders;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IndexProviderMap indexProviderMap;
    private final Log msgLog;
    private final SchemaCache schemaCache;
    private final Config config;
    private final BatchInserterImpl.BatchSchemaActions actions;
    private final StoreLocker storeLocker;
    private final PageCache pageCache;
    private final RecordStorageReader storageReader;
    private final StoreLogService logService;
    private final FileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private final JobScheduler jobScheduler;
    private boolean labelsTouched;
    private boolean isShutdown;

    private final LongFunction<Label> labelIdToLabelFunction = new LongFunction<Label>()
    {
        @Override
        public Label apply( long from )
        {
            try
            {
                return label( tokenHolders.labelTokens().getTokenById( safeCastLongToInt( from ) ).name() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        }
    };

    private final FlushStrategy flushStrategy;
    // Helper structure for setNodeProperty
    private final RelationshipCreator relationshipCreator;
    private final DirectRecordAccessSet recordAccess;
    private final PropertyTraverser propertyTraverser;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeletor;

    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final RelationshipTypeTokenStore relationshipTypeTokenStore;
    private final PropertyKeyTokenStore propertyKeyTokenStore;
    private final PropertyStore propertyStore;
    private final SchemaStore schemaStore;
    private final NeoStoreIndexStoreView storeIndexStoreView;

    private final LabelTokenStore labelTokenStore;
    private final Locks.Client noopLockClient = new NoOpClient();
    private final long maxNodeId;

    public BatchInserterImpl( final File databaseDirectory, final FileSystemAbstraction fileSystem,
                       Map<String, String> stringParams, Iterable<KernelExtensionFactory<?>> kernelExtensions ) throws IOException
    {
        rejectAutoUpgrade( stringParams );
        Map<String, String> params = getDefaultParams();
        params.putAll( stringParams );
        this.config = Config.defaults( params );
        this.fileSystem = fileSystem;

        life = new LifeSupport();
        this.databaseLayout = DatabaseLayout.of( databaseDirectory );
        this.jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        life.add( jobScheduler );

        storeLocker = tryLockStore( fileSystem );
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL, NullLog.getInstance(),
                EmptyVersionContextSupplier.EMPTY, jobScheduler );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();
        life.add( new PageCacheLifecycle( pageCache ) );

        config.augment( logs_directory, databaseDirectory.getCanonicalPath() );
        File internalLog = config.get( store_internal_log_path );

        logService = life.add( StoreLogService.withInternalLog( internalLog).build( fileSystem ) );
        msgLog = logService.getInternalLog( getClass() );

        boolean dump = config.get( GraphDatabaseSettings.dump_configuration );
        this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );

        LogProvider internalLogProvider = logService.getInternalLogProvider();
        RecordFormats recordFormats = RecordFormatSelector.selectForStoreOrConfig( config, databaseLayout, fileSystem,
                pageCache, internalLogProvider );
        StoreFactory sf = new StoreFactory( this.databaseLayout, config, idGeneratorFactory, pageCache, fileSystem,
                recordFormats, internalLogProvider, EmptyVersionContextSupplier.EMPTY );

        maxNodeId = recordFormats.node().getMaxId();

        if ( dump )
        {
            dumpConfiguration( params, System.out );
        }
        msgLog.info( Thread.currentThread() + " Starting BatchInserter(" + this + ")" );
        life.start();
        neoStores = sf.openAllNeoStores( true );
        neoStores.verifyStoreOk();
        this.pageCache = pageCache;

        nodeStore = neoStores.getNodeStore();
        relationshipStore = neoStores.getRelationshipStore();
        relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
        propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        propertyStore = neoStores.getPropertyStore();
        RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
        schemaStore = neoStores.getSchemaStore();
        labelTokenStore = neoStores.getLabelTokenStore();

        monitors = new Monitors();

        storeIndexStoreView = new NeoStoreIndexStoreView( NO_LOCK_SERVICE, neoStores );
        Dependencies deps = new Dependencies();
        Monitors monitors = new Monitors();
//<<<<<<< HEAD
        deps.satisfyDependencies( fileSystem, config, logService, storeIndexStoreView, pageCache, monitors, RecoveryCleanupWorkCollector.immediate() );

        DatabaseKernelExtensions extensions = life.add( new DatabaseKernelExtensions(
                new SimpleKernelContext( databaseDirectory, DatabaseInfo.TOOL, deps ),
                kernelExtensions, deps, KernelExtensionFailureStrategies.ignore() ) );

        indexProviderMap = life.add( new DefaultIndexProviderMap( extensions, config ) );

        TokenHolder propertyKeyTokenHolder = new DelegatingTokenHolder( this::createNewPropertyKeyId, TokenHolder.TYPE_PROPERTY_KEY );
        propertyKeyTokenHolder.setInitialTokens( propertyKeyTokenStore.getTokens() );
        TokenHolder relationshipTypeTokenHolder = new DelegatingTokenHolder( this::createNewRelationshipType, TokenHolder.TYPE_RELATIONSHIP_TYPE );
        relationshipTypeTokenHolder.setInitialTokens( relationshipTypeTokenStore.getTokens() );
        TokenHolder labelTokenHolder = new DelegatingTokenHolder( this::createNewLabelId, TokenHolder.TYPE_LABEL );
        labelTokenHolder.setInitialTokens( labelTokenStore.getTokens() );
        tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        indexStore = life.add( new IndexConfigStore( this.databaseLayout, fileSystem ) );
        schemaCache = new SchemaCache( loadConstraintSemantics(), schemaStore, indexProviderMap );

        actions = new BatchSchemaActions();

        // Record access
        recordAccess = new DirectRecordAccessSet( neoStores );
        relationshipCreator = new RelationshipCreator(
                new RelationshipGroupGetter( relationshipGroupStore ), relationshipGroupStore.getStoreHeaderInt() );
        propertyTraverser = new PropertyTraverser();
        propertyCreator = new PropertyCreator( propertyStore, propertyTraverser );
        propertyDeletor = new PropertyDeleter( propertyTraverser );

        flushStrategy = new BatchedFlushStrategy( recordAccess, config.get( GraphDatabaseSettings
                .batch_inserter_batch_size ) );
        storageReader = new RecordStorageReader( neoStores );
    }

    private StoreLocker tryLockStore( FileSystemAbstraction fileSystem )
    {
        StoreLocker storeLocker = new GlobalStoreLocker( fileSystem, this.databaseLayout.getStoreLayout() );
        try
        {
            storeLocker.checkLock();
        }
        catch ( Exception e )
        {
            try
            {
                storeLocker.close();
            }
            catch ( IOException ce )
            {
                e.addSuppressed( ce );
            }
            throw e;
        }
        return storeLocker;
    }

    private static Map<String, String> getDefaultParams()
    {
        Map<String, String> params = new HashMap<>();
        params.put( GraphDatabaseSettings.pagecache_memory.name(), "32m" );
        return params;
    }

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return primitiveHasProperty( getNodeRecord( node ).forChangingData(), propertyName );
    }

    @Override
    public boolean relationshipHasProperty( long relationship, String propertyName )
    {
        return primitiveHasProperty(
                recordAccess.getRelRecords().getOrLoad( relationship, null ).forReadingData(), propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName, Object propertyValue )
    {
        RecordProxy<NodeRecord,Void> nodeRecord = getNodeRecord( node );
        setPrimitiveProperty( nodeRecord, propertyName, propertyValue );

        flushStrategy.flush();
    }

    @Override
    public void setRelationshipProperty( long relationship, String propertyName, Object propertyValue )
    {
        RecordProxy<RelationshipRecord,Void> relationshipRecord = getRelationshipRecord( relationship );
        setPrimitiveProperty( relationshipRecord, propertyName, propertyValue );

        flushStrategy.flush();
    }

    @Override
    public void removeNodeProperty( long node, String propertyName )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        propertyDeletor.removePropertyIfExists( getNodeRecord( node ), propertyKey, recordAccess.getPropertyRecords() );
        flushStrategy.flush();
    }

    @Override
    public void removeRelationshipProperty( long relationship,
                                            String propertyName )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        propertyDeletor.removePropertyIfExists( getRelationshipRecord( relationship ), propertyKey,
                recordAccess.getPropertyRecords() );
        flushStrategy.flush();
    }

    @Override
    public IndexCreator createDeferredSchemaIndex( Label label )
    {
        return new IndexCreatorImpl( actions, label );
    }

    private void setPrimitiveProperty( RecordProxy<? extends PrimitiveRecord,Void> primitiveRecord,
            String propertyName, Object propertyValue )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords = recordAccess.getPropertyRecords();

        propertyCreator.primitiveSetProperty( primitiveRecord, propertyKey, ValueUtils.asValue( propertyValue ), propertyRecords );
    }

    private void validateIndexCanBeCreated( int labelId, int[] propertyKeyIds )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( labelId, propertyKeyIds,
                "Index for given {label;property} already exists" );
    }

    private void validateUniquenessConstraintCanBeCreated( int labelId, int[] propertyKeyIds )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( labelId, propertyKeyIds,
                "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}" );
    }

    private void validateNodeKeyConstraintCanBeCreated( int labelId, int[] propertyKeyIds )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( labelId, propertyKeyIds,
                "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}" );
    }

    private void verifyIndexOrUniquenessConstraintCanBeCreated( int labelId, int[] propertyKeyIds, String errorMessage )
    {
        LabelSchemaDescriptor schemaDescriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
        ConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.uniqueForSchema( schemaDescriptor );
        ConstraintDescriptor nodeKeyDescriptor = ConstraintDescriptorFactory.nodeKeyForSchema( schemaDescriptor );
        if ( schemaCache.hasIndex( schemaDescriptor ) ||
             schemaCache.hasConstraintRule( constraintDescriptor ) ||
             schemaCache.hasConstraintRule( nodeKeyDescriptor ) )
        {
            throw new ConstraintViolationException( errorMessage );
        }
    }

    private void validateNodePropertyExistenceConstraintCanBeCreated( int labelId, int[] propertyKeyIds )
    {
        ConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.existsForLabel( labelId, propertyKeyIds );

        if ( schemaCache.hasConstraintRule( constraintDescriptor ) )
        {
            throw new ConstraintViolationException(
                        "Node property existence constraint for given {label;property} already exists" );
        }
    }

    private void validateRelationshipConstraintCanBeCreated( int relTypeId, int propertyKeyId )
    {
        ConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.existsForLabel( relTypeId, propertyKeyId );

        if ( schemaCache.hasConstraintRule( constraintDescriptor ) )
        {
            throw new ConstraintViolationException(
                        "Relationship property existence constraint for given {type;property} already exists" );
        }
    }

    private IndexReference createIndex( int labelId, int[] propertyKeyIds, Optional<String> indexName )
    {
        LabelSchemaDescriptor schema = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
        IndexProvider provider = indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = provider.getProviderDescriptor();
        IndexDescriptor index = IndexDescriptorFactory.forSchema( schema, indexName, providerDescriptor );
        StoreIndexDescriptor schemaRule;
        try
        {
            schemaRule = provider.bless( index ).withId( schemaStore.nextId() );
        }
        catch ( MisconfiguredIndexException e )
        {
            throw new ConstraintViolationException(
                    "Unable to create index. The index configuration was refused by the '" + providerDescriptor + "' index provider.", e );
        }

        for ( DynamicRecord record : schemaStore.allocateFrom( schemaRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( schemaRule );
        labelsTouched = true;
        flushStrategy.forceFlush();
        return schemaRule;
    }

    private void repopulateAllIndexes( NativeLabelScanStore labelIndex )
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        LogProvider userLogProvider = logService.getUserLogProvider();
        IndexStoreView indexStoreView = new DynamicIndexStoreView( storeIndexStoreView, labelIndex, NO_LOCK_SERVICE, neoStores, logProvider );
        IndexingService indexingService = IndexingServiceFactory
                .createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView, new NonTransactionalTokenNameLookup( tokenHolders ),
                        emptyList(), logProvider, userLogProvider, NO_MONITOR, new DatabaseSchemaState( logProvider ), false );
        life.add( indexingService );
        try
        {
            StoreIndexDescriptor[] descriptors = getIndexesNeedingPopulation();
            indexingService.createIndexes( true /*verify constraints before flipping over*/, descriptors );
            for ( StoreIndexDescriptor descriptor : descriptors )
            {
                IndexProxy indexProxy = getIndexProxy( indexingService, descriptor );
                try
                {
                    indexProxy.awaitStoreScanCompleted( 0, TimeUnit.MILLISECONDS );
                }
                catch ( IndexPopulationFailedKernelException e )
                {
                    // In this scenario this is OK
                }
            }
            indexingService.forceAll( IOLimiter.UNLIMITED );
        }
        catch ( InterruptedException e )
        {
            // Someone wanted us to abort this. The indexes may not have been fully populated. This just means that they will be populated on next startup.
            Thread.currentThread().interrupt();
        }
    }

    private IndexProxy getIndexProxy( IndexingService indexingService, StoreIndexDescriptor descriptpr )
    {
        try
        {
            return indexingService.getIndexProxy( descriptpr.schema() );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Expected index by descriptor " + descriptpr + " to exist, but didn't", e );
        }
    }

    private void rebuildCounts()
    {
        CountsTracker counts = neoStores.getCounts();
        try
        {
            counts.start();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }

        CountsComputer.recomputeCounts( neoStores, pageCache, databaseLayout );
    }

    private StoreIndexDescriptor[] getIndexesNeedingPopulation()
    {
        List<StoreIndexDescriptor> indexesNeedingPopulation = new ArrayList<>();
        for ( StoreIndexDescriptor rule : schemaCache.indexDescriptors() )
        {
            IndexProvider provider = indexProviderMap.lookup( rule.providerDescriptor() );
            if ( provider.getInitialState( rule ) != InternalIndexState.FAILED )
            {
                indexesNeedingPopulation.add( rule );
            }
        }
        return indexesNeedingPopulation.toArray( new StoreIndexDescriptor[0] );
    }

    @Override
    public ConstraintCreator createDeferredConstraint( Label label )
    {
        return new BaseNodeConstraintCreator( new BatchSchemaActions(), label );
    }

    private void createUniqueIndexAndOwningConstraint( LabelSchemaDescriptor schema,
            IndexBackedConstraintDescriptor constraintDescriptor )
    {
        // TODO: Do not create duplicate index

        long indexId = schemaStore.nextId();
        long constraintRuleId = schemaStore.nextId();

        IndexProvider provider = indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = provider.getProviderDescriptor();
        IndexDescriptor index = IndexDescriptorFactory.uniqueForSchema( schema, providerDescriptor );
        StoreIndexDescriptor storeIndexDescriptor;
        try
        {
            storeIndexDescriptor = provider.bless( index ).withIds( indexId, constraintRuleId );
        }
        catch ( MisconfiguredIndexException e )
        {
            throw new ConstraintViolationException(
                    "Unable to create index. The index configuration was refused by the '" + providerDescriptor + "' index provider.", e );
        }

        ConstraintRule constraintRule = ConstraintRule.constraintRule( constraintRuleId, constraintDescriptor, indexId );

        for ( DynamicRecord record : schemaStore.allocateFrom( constraintRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( constraintRule );
        for ( DynamicRecord record : schemaStore.allocateFrom( storeIndexDescriptor ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( storeIndexDescriptor );
        labelsTouched = true;
        flushStrategy.forceFlush();
    }

    private void createUniquenessConstraintRule( LabelSchemaDescriptor descriptor )
    {
        createUniqueIndexAndOwningConstraint( descriptor, ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );
    }

    private void createNodeKeyConstraintRule( LabelSchemaDescriptor descriptor )
    {
        createUniqueIndexAndOwningConstraint( descriptor, ConstraintDescriptorFactory.nodeKeyForSchema( descriptor ) );
    }

    private void createNodePropertyExistenceConstraintRule( int labelId, int... propertyKeyIds )
    {
        SchemaRule rule = ConstraintRule.constraintRule( schemaStore.nextId(),
                ConstraintDescriptorFactory.existsForLabel( labelId, propertyKeyIds ) );

        for ( DynamicRecord record : schemaStore.allocateFrom( rule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( rule );
        labelsTouched = true;
        flushStrategy.forceFlush();
    }

    private void createRelTypePropertyExistenceConstraintRule( int relTypeId, int... propertyKeyIds )
    {
        SchemaRule rule = ConstraintRule.constraintRule( schemaStore.nextId(),
                ConstraintDescriptorFactory.existsForRelType( relTypeId, propertyKeyIds ) );

        for ( DynamicRecord record : schemaStore.allocateFrom( rule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( rule );
        flushStrategy.forceFlush();
    }

    private int getOrCreatePropertyKeyId( String name )
    {
        return tokenHolders.propertyKeyTokens().getOrCreateId( name );
    }

    private int getOrCreateRelationshipTypeId( String name )
    {
        return tokenHolders.relationshipTypeTokens().getOrCreateId( name );
    }

    private int getOrCreateLabelId( String name )
    {
        return tokenHolders.labelTokens().getOrCreateId( name );
    }

    private boolean primitiveHasProperty( PrimitiveRecord record, String propertyName )
    {
        int propertyKeyId = tokenHolders.propertyKeyTokens().getIdByName( propertyName );
        return propertyKeyId != NO_TOKEN && propertyTraverser.findPropertyRecordContaining( record, propertyKeyId,
                recordAccess.getPropertyRecords(), false ) != Record.NO_NEXT_PROPERTY.intValue();
    }

    private static void rejectAutoUpgrade( Map<String,String> params )
    {
        if ( parseBoolean( params.get( GraphDatabaseSettings.allow_upgrade.name() ) ) )
        {
            throw new IllegalArgumentException( "Batch inserter is not allowed to do upgrade of a store." );
        }
    }

    @Override
    public long createNode( Map<String, Object> properties, Label... labels )
    {
        return internalCreateNode( nodeStore.nextId(), properties, labels );
    }

    private long internalCreateNode( long nodeId, Map<String, Object> properties, Label... labels )
    {
        NodeRecord nodeRecord = recordAccess.getNodeRecords().create( nodeId, null ).forChangingData();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( propertyCreator.createPropertyChain( nodeRecord,
                propertiesIterator( properties ), recordAccess.getPropertyRecords() ) );

        if ( labels.length > 0 )
        {
            setNodeLabels( nodeRecord, labels );
        }

        flushStrategy.flush();
        return nodeId;
    }

    private Iterator<PropertyBlock> propertiesIterator( Map<String, Object> properties )
    {
        if ( properties == null || properties.isEmpty() )
        {
            return emptyIterator();
        }
        return new IteratorWrapper<PropertyBlock, Map.Entry<String,Object>>( properties.entrySet().iterator() )
        {
            @Override
            protected PropertyBlock underlyingObjectToObject( Entry<String, Object> property )
            {
                return propertyCreator.encodePropertyValue(
                        getOrCreatePropertyKeyId( property.getKey() ), ValueUtils.asValue( property.getValue() ) );
            }
        };
    }

    private void setNodeLabels( NodeRecord nodeRecord, Label... labels )
    {
        NodeLabels nodeLabels = parseLabelsField( nodeRecord );
        nodeLabels.put( getOrCreateLabelIds( labels ), nodeStore, nodeStore.getDynamicLabelStore() );
        labelsTouched = true;
    }

    private long[] getOrCreateLabelIds( Label[] labels )
    {
        long[] ids = new long[labels.length];
        int cursor = 0;
        for ( int i = 0; i < ids.length; i++ )
        {
            int labelId = getOrCreateLabelId( labels[i].name() );
            if ( !arrayContains( ids, cursor, labelId ) )
            {
                ids[cursor++] = labelId;
            }
        }
        if ( cursor < ids.length )
        {
            ids = Arrays.copyOf( ids, cursor );
        }
        return ids;
    }

    private static boolean arrayContains( long[] ids, int cursor, int labelId )
    {
        for ( int i = 0; i < cursor; i++ )
        {
            if ( ids[i] == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void createNode( long id, Map<String, Object> properties, Label... labels )
    {
        IdValidator.assertValidId( IdType.NODE, id, maxNodeId );
        if ( nodeStore.isInUse( id ) )
        {
            throw new IllegalArgumentException( "id=" + id + " already in use" );
        }
        long highId = nodeStore.getHighId();
        if ( highId <= id )
        {
            nodeStore.setHighestPossibleIdInUse( id );
        }
        internalCreateNode( id, properties, labels );
    }

    @Override
    public void setNodeLabels( long node, Label... labels )
    {
        NodeRecord record = getNodeRecord( node ).forChangingData();
        setNodeLabels( record, labels );
        flushStrategy.flush();
    }

    @Override
    public Iterable<Label> getNodeLabels( final long node )
    {
        return () ->
        {
            NodeRecord record = getNodeRecord( node ).forReadingData();
            long[] labels = parseLabelsField( record ).get( nodeStore );
            return map( labelIdToLabelFunction, PrimitiveLongCollections.iterator( labels ) );
        };
    }

    @Override
    public boolean nodeHasLabel( long node, Label label )
    {
        int labelId = tokenHolders.labelTokens().getIdByName( label.name() );
        return labelId != NO_TOKEN && nodeHasLabel( node, labelId );
    }

    private boolean nodeHasLabel( long node, int labelId )
    {
        NodeRecord record = getNodeRecord( node ).forReadingData();
        for ( long label : parseLabelsField( record ).get( nodeStore ) )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long createRelationship( long node1, long node2, RelationshipType type,
            Map<String, Object> properties )
    {
        long id = relationshipStore.nextId();
        int typeId = getOrCreateRelationshipTypeId( type.name() );
        relationshipCreator.relationshipCreate( id, typeId, node1, node2, recordAccess, noopLockClient );
        if ( properties != null && !properties.isEmpty() )
        {
            RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( id, null ).forChangingData();
            record.setNextProp( propertyCreator.createPropertyChain( record,
                    propertiesIterator( properties ), recordAccess.getPropertyRecords() ) );
        }
        flushStrategy.flush();
        return id;
    }

    @Override
    public void setNodeProperties( long node, Map<String, Object> properties )
    {
        NodeRecord record = getNodeRecord( node ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propertyDeletor.deletePropertyChain( record, recordAccess.getPropertyRecords() );
        }
        record.setNextProp( propertyCreator.createPropertyChain( record, propertiesIterator( properties ),
                recordAccess.getPropertyRecords() ) );
        flushStrategy.flush();
    }

    @Override
    public void setRelationshipProperties( long rel, Map<String, Object> properties )
    {
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( rel, null ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propertyDeletor.deletePropertyChain( record, recordAccess.getPropertyRecords() );
        }
        record.setNextProp( propertyCreator.createPropertyChain( record, propertiesIterator( properties ),
                recordAccess.getPropertyRecords() ) );
        flushStrategy.flush();
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        flushStrategy.forceFlush();
        return nodeStore.isInUse( nodeId );
    }

    @Override
    public Map<String,Object> getNodeProperties( long nodeId )
    {
        NodeRecord record = getNodeRecord( nodeId ).forReadingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        flushStrategy.forceFlush();
        return new BatchRelationshipIterable<Long>( storageReader, nodeId )
        {
            @Override
            protected Long nextFrom( long relId, int type, long startNode, long endNode )
            {
                return relId;
            }
        };
    }

    @Override
    public Iterable<BatchRelationship> getRelationships( long nodeId )
    {
        flushStrategy.forceFlush();
        return new BatchRelationshipIterable<BatchRelationship>( storageReader, nodeId )
        {
            @Override
            protected BatchRelationship nextFrom( long relId, int type, long startNode, long endNode )
            {
                return batchRelationshipOf( relId, type, startNode, endNode );
            }
        };
    }

    private BatchRelationship batchRelationshipOf( long id, int type, long startNode, long endNode )
    {
        try
        {
            return new BatchRelationship( id, startNode, endNode,
                    RelationshipType.withName( tokenHolders.relationshipTypeTokens().getTokenById( type ).name() ) );
        }
        catch ( TokenNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public BatchRelationship getRelationshipById( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId ).forReadingData();
        return batchRelationshipOf( relId, record.getType(), record.getFirstNode(), record.getSecondNode() );
    }

    @Override
    public Map<String,Object> getRelationshipProperties( long relId )
    {
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( relId, null ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public void shutdown()
    {
        if ( isShutdown )
        {
            throw new IllegalStateException( "Batch inserter already has shutdown" );
        }
        isShutdown = true;

        flushStrategy.forceFlush();

        rebuildCounts();

        try
        {
            NativeLabelScanStore labelIndex = buildLabelIndex();
            repopulateAllIndexes( labelIndex );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            neoStores.close();

            try
            {
                storeLocker.close();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Could not release store lock", e );
            }

            msgLog.info( Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")" );
            life.shutdown();
        }
    }

    private NativeLabelScanStore buildLabelIndex() throws IOException
    {
        NativeLabelScanStore labelIndex =
                new NativeLabelScanStore( pageCache, databaseLayout, fileSystem, new FullLabelStream( storeIndexStoreView ), false, monitors,
                        RecoveryCleanupWorkCollector.immediate() );
        if ( labelsTouched )
        {
            labelIndex.drop();
        }
        // Rebuild will happen as part of this call if it was dropped
        life.add( labelIndex );
        return labelIndex;
    }

    @Override
    public String toString()
    {
        return "EmbeddedBatchInserter[" + databaseLayout + "]";
    }

    private Map<String, Object> getPropertyChain( long nextProp )
    {
        final Map<String, Object> map = new HashMap<>();
        propertyTraverser.getPropertyChain( nextProp, recordAccess.getPropertyRecords(), propBlock ->
        {
            try
            {
                String key = tokenHolders.propertyKeyTokens().getTokenById( propBlock.getKeyIndexId() ).name();
                Value propertyValue = propBlock.newPropertyValue( propertyStore );
                map.put( key, propertyValue.asObject() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        } );
        return map;
    }

    private int createNewPropertyKeyId( String stringKey )
    {
        int keyId = (int) propertyKeyTokenStore.nextId();
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( keyId );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> keyRecords =
                propertyKeyTokenStore.allocateNameRecords( encodeString( stringKey ) );
        record.setNameId( (int) Iterables.first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        propertyKeyTokenStore.updateRecord( record );
        tokenHolders.propertyKeyTokens().addToken( new NamedToken( stringKey, keyId ) );
        return keyId;
    }

    private int createNewLabelId( String stringKey )
    {
        int keyId = (int) labelTokenStore.nextId();
        LabelTokenRecord record = new LabelTokenRecord( keyId );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> keyRecords =
                labelTokenStore.allocateNameRecords( encodeString( stringKey ) );
        record.setNameId( (int) Iterables.first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        labelTokenStore.updateRecord( record );
        tokenHolders.labelTokens().addToken( new NamedToken( stringKey, keyId ) );
        return keyId;
    }

    private int createNewRelationshipType( String name )
    {
        int id = (int) relationshipTypeTokenStore.nextId();
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> nameRecords = relationshipTypeTokenStore.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) Iterables.first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
        relationshipTypeTokenStore.updateRecord( record );
        tokenHolders.relationshipTypeTokens().addToken( new NamedToken( name, id ) );
        return id;
    }

    private RecordProxy<NodeRecord,Void> getNodeRecord( long id )
    {
        if ( id < 0 || id >= nodeStore.getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getNodeRecords().getOrLoad( id, null );
    }

    private RecordProxy<RelationshipRecord,Void> getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= relationshipStore.getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getRelRecords().getOrLoad( id, null );
    }

    @Override
    public String getStoreDir()
    {
        return databaseLayout.databaseDirectory().getPath();
    }

    @Override
    public IndexConfigStore getIndexStore()
    {
        return this.indexStore;
    }

    private static void dumpConfiguration( Map<String,String> config, PrintStream out )
    {
        for ( Entry<String,String> entry : config.entrySet() )
        {
            if ( entry.getValue() != null )
            {
                out.println( entry.getKey() + "=" + entry.getValue() );
            }
        }
    }

    @VisibleForTesting
    NeoStores getNeoStores()
    {
        return neoStores;
    }

    void forceFlushChanges()
    {
        flushStrategy.forceFlush();
    }

    private static ConstraintSemantics loadConstraintSemantics()
    {
        Iterable<ConstraintSemantics> semantics = Service.load( ConstraintSemantics.class );
        List<ConstraintSemantics> candidates = Iterables.asList( semantics );
        checkState( !candidates.isEmpty(), format( "At least one implementation of %s should be available.", ConstraintSemantics.class ) );

        return Collections.max( candidates, Comparator.comparingInt( ConstraintSemantics::getPriority ) );
    }

    private class BatchSchemaActions implements InternalSchemaActions
    {
        private int[] getOrCreatePropertyKeyIds( Iterable<String> properties )
        {
            return Iterables.stream( properties )
                    .mapToInt( BatchInserterImpl.this::getOrCreatePropertyKeyId )
                    .toArray();
        }

        private int[] getOrCreatePropertyKeyIds( String[] properties )
        {
            return Arrays.stream( properties )
                    .mapToInt( BatchInserterImpl.this::getOrCreatePropertyKeyId )
                    .toArray();
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, Optional<String> indexName, String... propertyKeys )
        {
            int labelId = getOrCreateLabelId( label.name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( propertyKeys );

            validateIndexCanBeCreated( labelId, propertyKeyIds );

            IndexReference indexReference = createIndex( labelId, propertyKeyIds, indexName );
            return new IndexDefinitionImpl( this, indexReference, new Label[]{label}, propertyKeys, false );
        }

        @Override
        public void dropIndexDefinitions( IndexDefinition indexDefinition )
        {
            throw unsupportedException();
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( IndexDefinition indexDefinition )
        {
            int labelId = getOrCreateLabelId( indexDefinition.getLabel().name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( indexDefinition.getPropertyKeys() );
            LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );

            validateUniquenessConstraintCanBeCreated( labelId, propertyKeyIds );
            createUniquenessConstraintRule( descriptor );
            return new UniquenessConstraintDefinition( this, indexDefinition );
        }

        @Override
        public ConstraintDefinition createNodeKeyConstraint( IndexDefinition indexDefinition )
        {
            int labelId = getOrCreateLabelId( indexDefinition.getLabel().name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( indexDefinition.getPropertyKeys() );
            LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );

            validateNodeKeyConstraintCanBeCreated( labelId, propertyKeyIds );
            createNodeKeyConstraintRule( descriptor );
            return new NodeKeyConstraintDefinition( this, indexDefinition );
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( Label label, String... propertyKeys )
        {
            int labelId = getOrCreateLabelId( label.name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( propertyKeys );

            validateNodePropertyExistenceConstraintCanBeCreated( labelId, propertyKeyIds );

            createNodePropertyExistenceConstraintRule( labelId, propertyKeyIds );
            return new NodePropertyExistenceConstraintDefinition( this, label, propertyKeys );
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( RelationshipType type, String propertyKey )
        {
            int relationshipTypeId = getOrCreateRelationshipTypeId( type.name() );
            int propertyKeyId = getOrCreatePropertyKeyId( propertyKey );

            validateRelationshipConstraintCanBeCreated( relationshipTypeId, propertyKeyId );

            createRelTypePropertyExistenceConstraintRule( relationshipTypeId, propertyKeyId );
            return new RelationshipPropertyExistenceConstraintDefinition( this, type, propertyKey );
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String[] properties )
        {
            throw unsupportedException();
        }

        @Override
        public void dropNodeKeyConstraint( Label label, String[] properties )
        {
            throw unsupportedException();
        }

        @Override
        public void dropNodePropertyExistenceConstraint( Label label, String[] properties )
        {
            throw unsupportedException();
        }

        @Override
        public void dropRelationshipPropertyExistenceConstraint( RelationshipType type, String propertyKey )
        {
            throw unsupportedException();
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            throw unsupportedException();
        }

        @Override
        public void assertInOpenTransaction()
        {
            // BatchInserterImpl always is expected to be running in one big single "transaction"
        }

        private UnsupportedOperationException unsupportedException()
        {
            return new UnsupportedOperationException( "Batch inserter doesn't support this" );
        }
    }

    interface FlushStrategy
    {
        void flush();

        void forceFlush();
    }

    static final class BatchedFlushStrategy implements FlushStrategy
    {
        private final DirectRecordAccessSet directRecordAccess;
        private final int batchSize;
        private int attempts;

        BatchedFlushStrategy( DirectRecordAccessSet directRecordAccess, int batchSize )
        {
            this.directRecordAccess = directRecordAccess;
            this.batchSize = batchSize;
        }

        @Override
        public void flush()
        {
            attempts++;
            if ( attempts >= batchSize )
            {
                forceFlush();
            }
        }

        @Override
        public void forceFlush()
        {
            directRecordAccess.commit();
            attempts = 0;
        }
    }
}
