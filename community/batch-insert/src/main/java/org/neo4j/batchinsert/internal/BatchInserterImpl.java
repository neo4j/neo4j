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
package org.neo4j.batchinsert.internal;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.stream.LongStream;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.IteratorWrapper;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;
import org.neo4j.internal.recordstorage.PropertyCreator;
import org.neo4j.internal.recordstorage.PropertyDeleter;
import org.neo4j.internal.recordstorage.PropertyTraverser;
import org.neo4j.internal.recordstorage.RecordAccess;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.RelationshipCreator;
import org.neo4j.internal.recordstorage.RelationshipGroupGetter;
import org.neo4j.internal.recordstorage.SchemaCache;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.scan.FullRelationshipTypeStream;
import org.neo4j.kernel.impl.coreapi.schema.BaseNodeConstraintCreator;
import org.neo4j.kernel.impl.coreapi.schema.IndexCreatorImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;
import org.neo4j.kernel.impl.coreapi.schema.NodeKeyConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
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
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.locker.DatabaseLocker;
import org.neo4j.kernel.internal.locker.Locker;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.internal.schema.IndexType.fromPublicApi;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForLabel;
import static org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory.existsForRelType;
import static org.neo4j.kernel.impl.api.index.IndexingService.NO_MONITOR;
import static org.neo4j.kernel.impl.constraints.ConstraintSemantics.getConstraintSemantics;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;

public class BatchInserterImpl implements BatchInserter
{
    private static final String BATCH_INSERTER_TAG = "batchInserter";
    private static final String CHECKPOINT_REASON = "Batch inserter checkpoint.";
    private final LifeSupport life;
    private final NeoStores neoStores;
    private final DatabaseLayout databaseLayout;
    private final TokenHolders tokenHolders;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IndexProviderMap indexProviderMap;
    private final Log msgLog;
    private final SchemaCache schemaCache;
    private final Config config;
    private final BatchInserterImpl.BatchSchemaActions actions;
    private final Locker locker;
    private final PageCache pageCache;
    private final RecordStorageReader storageReader;
    private final SimpleLogService logService;
    private final FileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private final JobScheduler jobScheduler;
    private final SchemaRuleAccess schemaRuleAccess;
    private final PageCacheTracer pageCacheTracer;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;
    private boolean labelsTouched;
    private boolean relationshipTypesTouched;
    private boolean isShutdown;

    private final LongFunction<Label> labelIdToLabelFunction;
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

    public BatchInserterImpl( final DatabaseLayout databaseLayout, final FileSystemAbstraction fileSystem,
                       Config fromConfig, Iterable<ExtensionFactory<?>> extensions, DatabaseTracers tracers ) throws IOException
    {
        rejectAutoUpgrade( fromConfig );
        Neo4jLayout layout = databaseLayout.getNeo4jLayout();
        this.config = Config.newBuilder()
                .setDefaults( getDefaultParams() )
                .set( neo4j_home, layout.homeDirectory() )
                .set( databases_root_path, layout.databasesDirectory() )
                .set( transaction_logs_root_path, layout.transactionLogsRootDirectory() )
                .set( logs_directory, Path.of( "" ) )
                .fromConfig( fromConfig )
                .build();
        this.fileSystem = fileSystem;
        pageCacheTracer = tracers.getPageCacheTracer();
        cursorTracer = pageCacheTracer.createPageCursorTracer( BATCH_INSERTER_TAG );
        memoryTracker = EmptyMemoryTracker.INSTANCE;
        life = new LifeSupport();
        this.databaseLayout = databaseLayout;
        this.jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        try
        {
            life.add( jobScheduler );

            locker = tryLockStore( fileSystem, databaseLayout );
            ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, pageCacheTracer, NullLog.getInstance(), EmptyVersionContextSupplier.EMPTY, jobScheduler, Clocks.nanoClock(),
                    new MemoryPools( config.get( memory_tracking ) ) );
            pageCache = pageCacheFactory.getOrCreatePageCache();
            life.add( new PageCacheLifecycle( pageCache ) );

            Neo4jLoggerContext ctx = LogConfig.createBuilder( fileSystem, config.get( store_internal_log_path ), Level.INFO ).build();

            logService = life.add( new SimpleLogService( ctx ) );
            msgLog = logService.getInternalLog( getClass() );

            boolean dump = config.get( GraphDatabaseInternalSettings.dump_configuration );
            this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), true );

            LogProvider internalLogProvider = logService.getInternalLogProvider();
            RecordFormats recordFormats = RecordFormatSelector.selectForStoreOrConfig( config, this.databaseLayout, fileSystem,
                pageCache, internalLogProvider, pageCacheTracer );
            StoreFactory sf = new StoreFactory( this.databaseLayout, config, idGeneratorFactory, pageCache, fileSystem,
                recordFormats, internalLogProvider, pageCacheTracer, immutable.empty() );

            maxNodeId = recordFormats.node().getMaxId();

            if ( dump )
            {
                dumpConfiguration( config );
            }
            msgLog.info( Thread.currentThread() + " Starting BatchInserter(" + this + ")" );
            life.start();
            neoStores = sf.openAllNeoStores( true );
            neoStores.verifyStoreOk();
            neoStores.start( cursorTracer );

            nodeStore = neoStores.getNodeStore();
            relationshipStore = neoStores.getRelationshipStore();
            relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
            propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
            propertyStore = neoStores.getPropertyStore();
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            schemaStore = neoStores.getSchemaStore();
            labelTokenStore = neoStores.getLabelTokenStore();

            TokenHolder propertyKeyTokenHolder = new DelegatingTokenHolder( this::createNewPropertyKeyId, TokenHolder.TYPE_PROPERTY_KEY );
            TokenHolder relationshipTypeTokenHolder = new DelegatingTokenHolder( this::createNewRelationshipType, TokenHolder.TYPE_RELATIONSHIP_TYPE );
            TokenHolder labelTokenHolder = new DelegatingTokenHolder( this::createNewLabelId, TokenHolder.TYPE_LABEL );
            tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );
            tokenHolders.setInitialTokens( StoreTokens.allTokens( neoStores ), cursorTracer );
            labelIdToLabelFunction = from ->
            {
                try
                {
                    return label( tokenHolders.labelTokens().getTokenById( safeCastLongToInt( from ) ).name() );
                }
                catch ( TokenNotFoundException e )
                {
                    throw new RuntimeException( e );
                }
            };

            monitors = new Monitors();

            storeIndexStoreView = new NeoStoreIndexStoreView( NO_LOCK_SERVICE, () -> new RecordStorageReader( neoStores ) );
            Dependencies deps = new Dependencies();
            deps.satisfyDependencies( fileSystem, jobScheduler, config, logService, storeIndexStoreView, tokenHolders, pageCache, monitors, immediate() );

            DatabaseExtensions databaseExtensions = life.add( new DatabaseExtensions(
                new DatabaseExtensionContext( this.databaseLayout, DbmsInfo.TOOL, deps ),
                extensions, deps, ExtensionFailureStrategies.ignore() ) );

            indexProviderMap = life.add( new DefaultIndexProviderMap( databaseExtensions, config ) );

            schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( schemaStore, tokenHolders );
            schemaCache = new SchemaCache( getConstraintSemantics(), indexProviderMap );
            schemaCache.load( schemaRuleAccess.getAll( cursorTracer ) );

            actions = new BatchSchemaActions();

            // Record access
            recordAccess = new DirectRecordAccessSet( neoStores, idGeneratorFactory );
            relationshipCreator = new RelationshipCreator(
                new RelationshipGroupGetter( relationshipGroupStore, cursorTracer ), relationshipGroupStore.getStoreHeaderInt(), cursorTracer );
            propertyTraverser = new PropertyTraverser( cursorTracer );
            propertyCreator = new PropertyCreator( propertyStore, propertyTraverser, cursorTracer, memoryTracker );
            propertyDeletor = new PropertyDeleter( propertyTraverser, cursorTracer );

            flushStrategy = new BatchedFlushStrategy( recordAccess, config.get( GraphDatabaseInternalSettings
                .batch_inserter_batch_size ) );
            storageReader = new RecordStorageReader( neoStores );
        }
        catch ( Exception e )
        {
            try
            {
                jobScheduler.shutdown();
            }
            catch ( Exception ex )
            {
                e.addSuppressed( ex );
            }
            throw e;
        }
    }

    private static Locker tryLockStore( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout )
    {
        Locker locker = new DatabaseLocker( fileSystem, databaseLayout );
        try
        {
            locker.checkLock();
        }
        catch ( Exception e )
        {
            try
            {
                locker.close();
            }
            catch ( IOException ce )
            {
                e.addSuppressed( ce );
            }
            throw e;
        }
        return locker;
    }

    private static Map<Setting<?>, Object> getDefaultParams()
    {
        Map<Setting<?>, Object> params = new HashMap<>();
        params.put( GraphDatabaseSettings.pagecache_memory, "32m" );
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
                recordAccess.getRelRecords().getOrLoad( relationship, null, cursorTracer ).forReadingData(), propertyName );
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

    private void validateIndexCanBeCreated( SchemaDescriptor schemaDescriptor )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( schemaDescriptor, "Index for given {label;property} already exists" );
    }

    private void validateUniquenessConstraintCanBeCreated( LabelSchemaDescriptor schemaDescriptor )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( schemaDescriptor,
                "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}" );
    }

    private void validateNodeKeyConstraintCanBeCreated( LabelSchemaDescriptor schemaDescriptor )
    {
        verifyIndexOrUniquenessConstraintCanBeCreated( schemaDescriptor,
                "It is not allowed to create node keys, uniqueness constraints or indexes on the same {label;property}" );
    }

    private void verifyIndexOrUniquenessConstraintCanBeCreated( SchemaDescriptor schemaDescriptor, String errorMessage )
    {
        if ( schemaDescriptor.isFulltextSchemaDescriptor() )
        {
            if ( schemaCache.hasIndex( schemaDescriptor ) )
            {
                throw new ConstraintViolationException( errorMessage );
            }
        }
        else
        {
            ConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.uniqueForSchema( schemaDescriptor );
            ConstraintDescriptor nodeKeyDescriptor = ConstraintDescriptorFactory.nodeKeyForSchema( schemaDescriptor );
            if ( schemaCache.hasIndex( schemaDescriptor ) ||
                    schemaCache.hasConstraintRule( constraintDescriptor ) ||
                    schemaCache.hasConstraintRule( nodeKeyDescriptor ) )
            {
                throw new ConstraintViolationException( errorMessage );
            }
        }
    }

    private void validateNodePropertyExistenceConstraintCanBeCreated( int labelId, int[] propertyKeyIds )
    {
        ConstraintDescriptor constraintDescriptor = existsForLabel( labelId, propertyKeyIds );

        if ( schemaCache.hasConstraintRule( constraintDescriptor ) )
        {
            throw new ConstraintViolationException(
                        "Node property existence constraint for given {label;property} already exists" );
        }
    }

    private void validateRelationshipConstraintCanBeCreated( int relTypeId, int propertyKeyId )
    {
        ConstraintDescriptor constraintDescriptor = existsForLabel( relTypeId, propertyKeyId );

        if ( schemaCache.hasConstraintRule( constraintDescriptor ) )
        {
            throw new ConstraintViolationException(
                        "Relationship property existence constraint for given {type;property} already exists" );
        }
    }

    private IndexDescriptor createIndex( IndexPrototype prototype )
    {
        IndexProvider provider = isFullTextIndexType( prototype ) ? indexProviderMap.getFulltextProvider() : indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = provider.getProviderDescriptor();
        prototype = prototype.withIndexProvider( providerDescriptor );
        prototype = ensureSchemaHasName( prototype );

        IndexDescriptor index = prototype.materialise( schemaStore.nextId( cursorTracer ) );
        index = provider.completeConfiguration( index );

        try
        {
            schemaRuleAccess.writeSchemaRule( index, cursorTracer, memoryTracker );
            schemaCache.addSchemaRule( index );
            updateTouchToken( index.schema() );
            flushStrategy.forceFlush();
            return index;
        }
        catch ( KernelException e )
        {
            throw kernelExceptionToUserException( e );
        }
    }

    private void updateTouchToken( SchemaDescriptor schema )
    {
        if ( schema.entityType() == EntityType.NODE )
        {
            labelsTouched = true;
        }
        else
        {
            relationshipTypesTouched = true;
        }
    }

    private boolean isFullTextIndexType( IndexPrototype prototype )
    {
        return prototype.getIndexType() == org.neo4j.internal.schema.IndexType.FULLTEXT;
    }

    private void repopulateAllIndexes( LabelScanStore labelIndex, RelationshipTypeScanStore relationshipTypeIndex ) throws IOException
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        LogProvider userLogProvider = logService.getUserLogProvider();
        var cacheTracer = PageCacheTracer.NULL;
        IndexStoreView indexStoreView = new DynamicIndexStoreView( storeIndexStoreView, labelIndex, relationshipTypeIndex,
                NO_LOCK_SERVICE, () -> new RecordStorageReader( neoStores ), logProvider, config );
        IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout.indexStatisticsStore(),
                immediate(), false, cacheTracer );
        IndexingService indexingService = IndexingServiceFactory
                .createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView, tokenHolders, emptyList(), logProvider, userLogProvider,
                        NO_MONITOR, new DatabaseSchemaState( logProvider ), indexStatisticsStore, cacheTracer, memoryTracker, databaseLayout.getDatabaseName(),
                        false );
        life.add( indexingService );
        try
        {
            IndexDescriptor[] descriptors = getIndexesNeedingPopulation( cursorTracer );
            indexingService.createIndexes( true /*verify constraints before flipping over*/, AUTH_DISABLED, descriptors );
            for ( IndexDescriptor descriptor : descriptors )
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
            indexingService.forceAll( IOLimiter.UNLIMITED, cursorTracer );
        }
        catch ( InterruptedException e )
        {
            // Someone wanted us to abort this. The indexes may not have been fully populated. This just means that they will be populated on next startup.
            Thread.currentThread().interrupt();
        }
    }

    private static IndexProxy getIndexProxy( IndexingService indexingService, IndexDescriptor index )
    {
        try
        {
            return indexingService.getIndexProxy( index );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new IllegalStateException( "Expected index by descriptor " + index + " to exist, but didn't", e );
        }
    }

    private void rebuildCounts( PageCacheTracer cacheTracer, MemoryTracker memoryTracker ) throws IOException
    {
        Path countsStoreFile = databaseLayout.countStore();
        fileSystem.deleteFile( countsStoreFile );
        CountsComputer initialCountsBuilder =
                new CountsComputer( neoStores, pageCache, cacheTracer, databaseLayout, memoryTracker, logService.getInternalLog( getClass() ) );
        try ( GBPTreeCountsStore countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fileSystem, immediate(),
                initialCountsBuilder, false, cacheTracer, GBPTreeCountsStore.NO_MONITOR ) )
        {
            countsStore.start( PageCursorTracer.NULL, memoryTracker );
            countsStore.checkpoint( IOLimiter.UNLIMITED, PageCursorTracer.NULL );
        }
    }

    private void createEmptyTransactionLog()
    {
        TransactionLogInitializer.getLogFilesInitializer().initializeLogFiles( databaseLayout, neoStores.getMetaDataStore(), fileSystem, CHECKPOINT_REASON );
    }

    private IndexDescriptor[] getIndexesNeedingPopulation( PageCursorTracer cursorTracer )
    {
        List<IndexDescriptor> indexesNeedingPopulation = new ArrayList<>();
        for ( IndexDescriptor descriptor : schemaCache.indexes() )
        {
            IndexProvider provider = indexProviderMap.lookup( descriptor.getIndexProvider() );
            if ( provider.getInitialState( descriptor, cursorTracer ) != InternalIndexState.FAILED )
            {
                indexesNeedingPopulation.add( descriptor );
            }
        }
        return indexesNeedingPopulation.toArray( new IndexDescriptor[0] );
    }

    @Override
    public ConstraintCreator createDeferredConstraint( Label label )
    {
        return new BaseNodeConstraintCreator( new BatchSchemaActions(), null, label, null, null );
    }

    private IndexBackedConstraintDescriptor createUniqueIndexAndOwningConstraint( LabelSchemaDescriptor schema, IndexBackedConstraintDescriptor constraint,
            PageCursorTracer cursorTracer )
    {
        // TODO: Do not create duplicate index
        long indexId = schemaStore.nextId( cursorTracer );
        long constraintRuleId = schemaStore.nextId( cursorTracer );
        constraint = ensureSchemaHasName( constraint );

        IndexProvider provider = indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = provider.getProviderDescriptor();
        IndexPrototype prototype = IndexPrototype.uniqueForSchema( schema, providerDescriptor );
        IndexDescriptor index = prototype.withName( constraint.getName() ).materialise( indexId );
        index = provider.completeConfiguration( index ).withOwningConstraintId( constraintRuleId );

        constraint = constraint.withId( constraintRuleId ).withOwnedIndexId( indexId );

        try
        {
            schemaRuleAccess.writeSchemaRule( constraint, cursorTracer, memoryTracker );
            schemaCache.addSchemaRule( constraint );
            schemaRuleAccess.writeSchemaRule( index, cursorTracer, memoryTracker );
            schemaCache.addSchemaRule( index );
            updateTouchToken( constraint.schema() );
            flushStrategy.forceFlush();
            return constraint;
        }
        catch ( KernelException e )
        {
            throw kernelExceptionToUserException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T extends SchemaDescriptorSupplier> T ensureSchemaHasName( T schemaish )
    {
        String name;
        if ( schemaish instanceof IndexPrototype )
        {
            name = ((IndexPrototype) schemaish).getName().orElse( null );
        }
        else if ( schemaish instanceof SchemaRule )
        {
            name = ((SchemaRule) schemaish).getName();
        }
        else
        {
            throw new IllegalArgumentException( "Don't know how to check the name of " + schemaish + "." );
        }
        if ( name == null || (name = name.trim()).isEmpty() || name.isBlank() )
        {
            try
            {
                SchemaDescriptor schema = schemaish.schema();
                TokenHolder entityTokenHolder = schema.entityType() == EntityType.NODE ? tokenHolders.labelTokens() : tokenHolders.relationshipTypeTokens();
                TokenHolder propertyKeyTokenHolder = tokenHolders.propertyKeyTokens();
                int[] entityTokenIds = schema.getEntityTokenIds();
                int[] propertyIds = schema.getPropertyIds();
                String[] entityTokenNames = new String[entityTokenIds.length];
                String[] propertyNames = new String[propertyIds.length];
                for ( int i = 0; i < entityTokenIds.length; i++ )
                {
                    entityTokenNames[i] = entityTokenHolder.getTokenById( entityTokenIds[i] ).name();
                }
                for ( int i = 0; i < propertyIds.length; i++ )
                {
                    propertyNames[i] = propertyKeyTokenHolder.getTokenById( propertyIds[i] ).name();
                }
                name = SchemaRule.generateName( schemaish, entityTokenNames, propertyNames );
                if ( schemaish instanceof IndexPrototype )
                {
                    schemaish = (T) ((IndexPrototype) schemaish).withName( name );
                }
                else
                {
                    schemaish = (T) ((SchemaRule) schemaish).withName( name );
                }
            }
            catch ( TokenNotFoundException e )
            {
                throw new TransactionFailureException( "Failed to generate name for constraint: " + schemaish, e );
            }
        }
        return schemaish;
    }

    private TransactionFailureException kernelExceptionToUserException( KernelException e )
    {
        // This may look odd, but previously TokenHolder#getOrCreateId silently converted KernelException into TransactionFailureException
        throw new TransactionFailureException( "Unexpected kernel exception writing schema rules", e );
    }

    private IndexBackedConstraintDescriptor createUniquenessConstraintRule( LabelSchemaDescriptor descriptor, String name )
    {
        return createUniqueIndexAndOwningConstraint( descriptor, ConstraintDescriptorFactory.uniqueForSchema( descriptor ).withName( name ), cursorTracer );
    }

    private IndexBackedConstraintDescriptor createNodeKeyConstraintRule( LabelSchemaDescriptor descriptor, String name )
    {
        return createUniqueIndexAndOwningConstraint( descriptor, ConstraintDescriptorFactory.nodeKeyForSchema( descriptor ).withName( name ), cursorTracer );
    }

    private ConstraintDescriptor createNodePropertyExistenceConstraintRule( String name, int labelId, int... propertyKeyIds )
    {
        ConstraintDescriptor rule = existsForLabel( labelId, propertyKeyIds ).withId( schemaStore.nextId( cursorTracer ) ).withName( name );
        rule = ensureSchemaHasName( rule );

        try
        {
            schemaRuleAccess.writeSchemaRule( rule, cursorTracer, memoryTracker );
            schemaCache.addSchemaRule( rule );
            updateTouchToken( rule.schema() );
            flushStrategy.forceFlush();
            return rule;
        }
        catch ( KernelException e )
        {
            throw kernelExceptionToUserException( e );
        }
    }

    private ConstraintDescriptor createRelTypePropertyExistenceConstraintRule( String name, int relTypeId, int... propertyKeyIds )
    {
        ConstraintDescriptor rule = existsForRelType( relTypeId, propertyKeyIds ).withId( schemaStore.nextId( cursorTracer ) ).withName( name );
        rule = ensureSchemaHasName( rule );

        try
        {
            schemaRuleAccess.writeSchemaRule( rule, cursorTracer, memoryTracker );
            schemaCache.addSchemaRule( rule );
            flushStrategy.forceFlush();
            return rule;
        }
        catch ( KernelException e )
        {
            throw kernelExceptionToUserException( e );
        }
    }

    private int silentGetOrCreateTokenId( TokenHolder tokens, String name )
    {
        try
        {
            return tokens.getOrCreateId( name );
        }
        catch ( KernelException e )
        {
            throw kernelExceptionToUserException( e );
        }
    }

    private int getOrCreatePropertyKeyId( String name )
    {
        return silentGetOrCreateTokenId( tokenHolders.propertyKeyTokens(), name );
    }

    private int getOrCreateRelationshipTypeId( String name )
    {
        return silentGetOrCreateTokenId( tokenHolders.relationshipTypeTokens(), name );
    }

    private int getOrCreateLabelId( String name )
    {
        return silentGetOrCreateTokenId( tokenHolders.labelTokens(), name );
    }

    private boolean primitiveHasProperty( PrimitiveRecord record, String propertyName )
    {
        int propertyKeyId = tokenHolders.propertyKeyTokens().getIdByName( propertyName );
        return propertyKeyId != NO_TOKEN && propertyTraverser.findPropertyRecordContaining( record, propertyKeyId,
                recordAccess.getPropertyRecords(), false ) != Record.NO_NEXT_PROPERTY.intValue();
    }

    private static void rejectAutoUpgrade( Config config )
    {
        if ( config.get( GraphDatabaseSettings.allow_upgrade ) )
        {
            throw new IllegalArgumentException( "Batch inserter is not allowed to do upgrade of a store." );
        }
    }

    @Override
    public long createNode( Map<String,Object> properties, Label... labels )
    {
        return internalCreateNode( nodeStore.nextId( cursorTracer ), properties, labels );
    }

    private long internalCreateNode( long nodeId, Map<String, Object> properties, Label... labels )
    {
        NodeRecord nodeRecord = recordAccess.getNodeRecords().create( nodeId, null, cursorTracer ).forChangingData();
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
        return new IteratorWrapper<>( properties.entrySet().iterator() )
        {
            @Override
            protected PropertyBlock underlyingObjectToObject( Entry<String,Object> property )
            {
                return propertyCreator.encodePropertyValue( getOrCreatePropertyKeyId( property.getKey() ), ValueUtils.asValue( property.getValue() ) );
            }
        };
    }

    private void setNodeLabels( NodeRecord nodeRecord, Label... labels )
    {
        NodeLabels nodeLabels = parseLabelsField( nodeRecord );
        nodeLabels.put( getOrCreateLabelIds( labels ), nodeStore, nodeStore.getDynamicLabelStore(), cursorTracer, memoryTracker );
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
        if ( nodeStore.isInUse( id, cursorTracer ) )
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
            long[] labels = parseLabelsField( record ).get( nodeStore, cursorTracer );
            return LongStream.of( labels ).mapToObj( labelIdToLabelFunction ).iterator();
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
        for ( long label : parseLabelsField( record ).get( nodeStore, cursorTracer ) )
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
        long id = relationshipStore.nextId( cursorTracer );
        int typeId = getOrCreateRelationshipTypeId( type.name() );
        relationshipCreator.relationshipCreate( id, typeId, node1, node2, recordAccess, noopLockClient );
        if ( properties != null && !properties.isEmpty() )
        {
            RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( id, null, cursorTracer ).forChangingData();
            record.setNextProp( propertyCreator.createPropertyChain( record,
                    propertiesIterator( properties ), recordAccess.getPropertyRecords() ) );
        }
        relationshipTypesTouched = true;
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
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( rel, null, cursorTracer ).forChangingData();
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
        return nodeStore.isInUse( nodeId, cursorTracer );
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
        return new BatchRelationshipIterable<>( storageReader, nodeId, cursorTracer )
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
        return new BatchRelationshipIterable<>( storageReader, nodeId, cursorTracer )
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
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( relId, null, cursorTracer ).forChangingData();
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

        try ( cursorTracer;
              var ignore = new Lifespan( life );
              locker;
              neoStores )
        {
            rebuildCounts( pageCacheTracer, memoryTracker );
            LabelScanStore labelIndex = buildLabelIndex();
            RelationshipTypeScanStore relationshipTypeIndex = buildRelationshipTypeIndex();
            repopulateAllIndexes( labelIndex, relationshipTypeIndex );
            idGeneratorFactory.visit( IdGenerator::markHighestWrittenAtHighId );
            neoStores.flush( IOLimiter.UNLIMITED, cursorTracer );
            createEmptyTransactionLog();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        msgLog.info( Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")" );
    }

    private LabelScanStore buildLabelIndex() throws IOException
    {
        FullLabelStream labelStream = new FullLabelStream( storeIndexStoreView );
        LabelScanStore labelIndex = TokenScanStore.labelScanStore( pageCache, databaseLayout, fileSystem, labelStream, false, monitors, immediate(),
                config, pageCacheTracer, memoryTracker );
        if ( labelsTouched )
        {
            labelIndex.drop();
        }
        // Rebuild will happen as part of this call if it was dropped
        life.add( labelIndex );
        return labelIndex;
    }

    private RelationshipTypeScanStore buildRelationshipTypeIndex() throws IOException
    {
        FullRelationshipTypeStream fullRelationshipTypeStream = new FullRelationshipTypeStream( storeIndexStoreView );
        RelationshipTypeScanStore relationshipTypeIndex = TokenScanStore
                .toggledRelationshipTypeScanStore( pageCache, databaseLayout, fileSystem, fullRelationshipTypeStream, false, monitors, immediate(), config,
                        pageCacheTracer, memoryTracker );
        if ( relationshipTypesTouched )
        {
            relationshipTypeIndex.drop();
        }
        // Rebuild will happen as part of this call if it was dropped
        life.add( relationshipTypeIndex );
        return relationshipTypeIndex;
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
                Value propertyValue = propBlock.newPropertyValue( propertyStore, cursorTracer );
                map.put( key, propertyValue.asObject() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        } );
        return map;
    }

    private int createNewPropertyKeyId( String stringKey, boolean internal )
    {
        return createNewToken( propertyKeyTokenStore, stringKey, internal );
    }

    private int createNewLabelId( String stringKey, boolean internal )
    {
        return createNewToken( labelTokenStore, stringKey, internal );
    }

    private int createNewRelationshipType( String name, boolean internal )
    {
        return createNewToken( relationshipTypeTokenStore, name, internal );
    }

    private <R extends TokenRecord> int createNewToken( TokenStore<R> store, String name, boolean internal )
    {
        int keyId = (int) store.nextId( cursorTracer );
        R record = store.newRecord();
        record.setId( keyId );
        record.setInUse( true );
        record.setInternal( internal );
        record.setCreated();
        Collection<DynamicRecord> keyRecords = store.allocateNameRecords( encodeString( name ), cursorTracer, memoryTracker );
        record.setNameId( (int) Iterables.first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        store.updateRecord( record, cursorTracer );
        return keyId;
    }

    private RecordProxy<NodeRecord,Void> getNodeRecord( long id )
    {
        if ( id < 0 || id >= nodeStore.getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getNodeRecords().getOrLoad( id, null, cursorTracer );
    }

    private RecordProxy<RelationshipRecord,Void> getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= relationshipStore.getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getRelRecords().getOrLoad( id, null, cursorTracer );
    }

    @Override
    public String getStoreDir()
    {
        return databaseLayout.databaseDirectory().toString();
    }

    private static void dumpConfiguration( Config config )
    {
        System.out.print( config.toString() );
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
        public IndexDefinition createIndexDefinition(
                Label[] labels, String indexName, IndexType indexType, IndexConfig indexConfig, String... propertyKeys )
        {
            if ( indexConfig.entries().notEmpty() )
            {
                throw unsupportedException();
            }
            int[] labelIds = Arrays.stream( labels ).mapToInt( label -> getOrCreateLabelId( label.name() ) ).toArray();
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( propertyKeys );
            SchemaDescriptor schema;
            if ( indexType == IndexType.FULLTEXT )
            {
                throw unsupportedException();
            }
            else if ( labelIds.length == 1 )
            {
                schema = SchemaDescriptor.forLabel( labelIds[0], propertyKeyIds );
            }
            else
            {
                throw new IllegalArgumentException( indexType + " indexes can only be created with exactly one label." );
            }

            validateIndexCanBeCreated( schema );
            IndexPrototype prototype = IndexPrototype.forSchema( schema ).withName( indexName ).withIndexType( fromPublicApi( indexType ) );

            IndexDescriptor index = createIndex( prototype );
            return new IndexDefinitionImpl( this, index, labels, propertyKeys, false );
        }

        @Override
        public IndexDefinition createIndexDefinition( RelationshipType[] types, String indexName, IndexType indexType, IndexConfig indexConfig,
                String... propertyKeys )
        {
            throw unsupportedException();
        }

        @Override
        public void dropIndexDefinitions( IndexDefinition indexDefinition )
        {
            throw unsupportedException();
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( IndexDefinition indexDefinition, String name, IndexType indexType,
                IndexConfig indexConfig )
        {
            LabelSchemaDescriptor descriptor = indexDefinitionToLabelSchemaDescriptor( indexDefinition, indexType, indexConfig );

            validateUniquenessConstraintCanBeCreated( descriptor );
            IndexBackedConstraintDescriptor constraint = createUniquenessConstraintRule( descriptor, name );
            return new UniquenessConstraintDefinition( this, constraint, indexDefinition );
        }

        @Override
        public ConstraintDefinition createNodeKeyConstraint( IndexDefinition indexDefinition, String name, IndexType indexType, IndexConfig indexConfig )
        {
            LabelSchemaDescriptor descriptor = indexDefinitionToLabelSchemaDescriptor( indexDefinition, indexType, indexConfig );

            validateNodeKeyConstraintCanBeCreated( descriptor );
            IndexBackedConstraintDescriptor constraint = createNodeKeyConstraintRule( descriptor, name );
            return new NodeKeyConstraintDefinition( this, constraint, indexDefinition );
        }

        private LabelSchemaDescriptor indexDefinitionToLabelSchemaDescriptor( IndexDefinition indexDefinition, IndexType indexType, IndexConfig indexConfig )
        {
            if ( indexType != null )
            {
                throw unsupportedException();
            }
            if ( indexConfig != null )
            {
                throw unsupportedException();
            }
            int labelId = getOrCreateLabelId( single( indexDefinition.getLabels() ).name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( indexDefinition.getPropertyKeys() );
            return SchemaDescriptor.forLabel( labelId, propertyKeyIds );
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( String name, Label label, String... propertyKeys )
        {
            int labelId = getOrCreateLabelId( label.name() );
            int[] propertyKeyIds = getOrCreatePropertyKeyIds( propertyKeys );

            validateNodePropertyExistenceConstraintCanBeCreated( labelId, propertyKeyIds );

            ConstraintDescriptor constraint = createNodePropertyExistenceConstraintRule( name, labelId, propertyKeyIds );
            return new NodePropertyExistenceConstraintDefinition( this, constraint, label, propertyKeys );
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( String name, RelationshipType type, String propertyKey )
        {
            int relationshipTypeId = getOrCreateRelationshipTypeId( type.name() );
            int propertyKeyId = getOrCreatePropertyKeyId( propertyKey );

            validateRelationshipConstraintCanBeCreated( relationshipTypeId, propertyKeyId );

            ConstraintDescriptor constraint = createRelTypePropertyExistenceConstraintRule( name, relationshipTypeId, propertyKeyId );
            return new RelationshipPropertyExistenceConstraintDefinition( this, constraint, type, propertyKey );
        }

        @Override
        public void dropConstraint( ConstraintDescriptor constraint )
        {
            throw unsupportedException();
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            throw unsupportedException();
        }

        @Override
        public String getUserDescription( IndexDescriptor index )
        {
            return index == null ? null : index.userDescription( tokenHolders );
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
