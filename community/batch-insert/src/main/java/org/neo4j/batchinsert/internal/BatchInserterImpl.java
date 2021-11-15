/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.counts.DegreesRebuildFromStore;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.IteratorWrapper;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.recordstorage.DirectRecordAccessSet;
import org.neo4j.internal.recordstorage.PropertyCreator;
import org.neo4j.internal.recordstorage.PropertyDeleter;
import org.neo4j.internal.recordstorage.PropertyTraverser;
import org.neo4j.internal.recordstorage.RecordAccess;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.recordstorage.RecordCursorTypes;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.RelationshipCreator;
import org.neo4j.internal.recordstorage.RelationshipGroupGetter;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.factory.DbmsInfo;
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
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
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
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.transaction.state.storeview.FullScanStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
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
import org.neo4j.storageengine.api.cursor.StoreCursors;
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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.counts_store_max_cached_entries;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.GBPTreeGenericCountsStore.NO_MONITOR;
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RelationshipModifier.DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH;
import static org.neo4j.kernel.impl.constraints.ConstraintSemantics.getConstraintSemantics;
import static org.neo4j.kernel.impl.locking.Locks.NO_LOCKS;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;

public class BatchInserterImpl implements BatchInserter
{
    private static final String BATCH_INSERTER_TAG = "batchInserter";
    private static final String CHECKPOINT_REASON = "Batch inserter checkpoint.";
    private final LifeSupport life;
    private final NeoStores neoStores;
    private final RecordDatabaseLayout databaseLayout;
    private final TokenHolders tokenHolders;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IndexProviderMap indexProviderMap;
    private final Log msgLog;
    private final SchemaCache schemaCache;
    private final Config config;
    private final Locker locker;
    private final PageCache pageCache;
    private final RecordStorageReader storageReader;
    private final SimpleLogService logService;
    private final FileSystemAbstraction fileSystem;
    private final Monitors monitors;
    private final JobScheduler jobScheduler;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final MemoryTracker memoryTracker;
    private final RelationshipGroupDegreesStore.Updater degreeUpdater;
    private final RelationshipGroupGetter relationshipGroupGetter;
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
    private final GBPTreeRelationshipGroupDegreesStore groupDegreesStore;
    private final FullScanStoreView fullScanStoreView;

    private final LabelTokenStore labelTokenStore;
    private final long maxNodeId;
    private final DatabaseReadOnlyChecker readOnlyChecker;

    public BatchInserterImpl( DatabaseLayout layoutArg, final FileSystemAbstraction fileSystem,
                       Config fromConfig, DatabaseTracers tracers ) throws IOException
    {
        RecordDatabaseLayout databaseLayout = RecordDatabaseLayout.convert( layoutArg );
        rejectAutoUpgrade( fromConfig );
        Neo4jLayout layout = databaseLayout.getNeo4jLayout();
        this.config = Config.newBuilder()
                .setDefault( pagecache_memory, ByteUnit.mebiBytes( 32 ) )
                .set( neo4j_home, layout.homeDirectory() )
                .set( databases_root_path, layout.databasesDirectory() )
                .set( transaction_logs_root_path, layout.transactionLogsRootDirectory() )
                .set( logs_directory, Path.of( "" ) )
                .fromConfig( fromConfig )
                .build();
        this.fileSystem = fileSystem;
        pageCacheTracer = tracers.getPageCacheTracer();
        cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( BATCH_INSERTER_TAG ) );
        memoryTracker = EmptyMemoryTracker.INSTANCE;
        life = new LifeSupport();
        this.databaseLayout = databaseLayout;
        this.jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        try
        {
            life.add( jobScheduler );

            locker = tryLockStore( fileSystem, databaseLayout );
            ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, pageCacheTracer, NullLog.getInstance(), jobScheduler, Clocks.nanoClock(),
                    new MemoryPools( config.get( memory_tracking ) ) );
            pageCache = pageCacheFactory.getOrCreatePageCache();
            life.add( new PageCacheLifecycle( pageCache ) );

            Neo4jLoggerContext ctx = LogConfig.createBuilder( fileSystem, config.get( store_internal_log_path ), Level.INFO ).build();

            logService = life.add( new SimpleLogService( ctx ) );
            msgLog = logService.getInternalLog( getClass() );

            boolean dump = config.get( GraphDatabaseInternalSettings.dump_configuration );
            this.idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), true, databaseLayout.getDatabaseName() );

            LogProvider internalLogProvider = logService.getInternalLogProvider();
            RecordFormats recordFormats = RecordFormatSelector.selectForStoreOrConfig( config, this.databaseLayout, fileSystem,
                pageCache, internalLogProvider, pageCacheTracer );
            readOnlyChecker = writable();
            StoreFactory sf = new StoreFactory( this.databaseLayout, config, idGeneratorFactory, pageCache, fileSystem,
                recordFormats, internalLogProvider, pageCacheTracer, readOnlyChecker, immutable.empty() );

            maxNodeId = recordFormats.node().getMaxId();

            if ( dump )
            {
                dumpConfiguration( config );
            }
            msgLog.info( Thread.currentThread() + " Starting BatchInserter(" + this + ")" );
            life.start();
            neoStores = sf.openAllNeoStores( true );
            neoStores.verifyStoreOk();
            neoStores.start( cursorContext );

            storeCursors = new CachedStoreCursors( neoStores, cursorContext );

            nodeStore = neoStores.getNodeStore();
            relationshipStore = neoStores.getRelationshipStore();
            relationshipTypeTokenStore = neoStores.getRelationshipTypeTokenStore();
            propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
            propertyStore = neoStores.getPropertyStore();
            RecordStore<RelationshipGroupRecord> relationshipGroupStore = neoStores.getRelationshipGroupStore();
            labelTokenStore = neoStores.getLabelTokenStore();

            groupDegreesStore = new GBPTreeRelationshipGroupDegreesStore( pageCache, databaseLayout.relationshipGroupDegreesStore(), fileSystem, immediate(),
                    new DegreesRebuildFromStore( neoStores ), readOnlyChecker, pageCacheTracer, NO_MONITOR,
                    databaseLayout.getDatabaseName(), config.get( counts_store_max_cached_entries ), logService.getUserLogProvider() );
            groupDegreesStore.start( cursorContext, storeCursors, memoryTracker );

            degreeUpdater = groupDegreesStore.directApply( cursorContext );

            TokenHolder propertyKeyTokenHolder = new DelegatingTokenHolder( this::createNewPropertyKeyId, TokenHolder.TYPE_PROPERTY_KEY );
            TokenHolder relationshipTypeTokenHolder = new DelegatingTokenHolder( this::createNewRelationshipType, TokenHolder.TYPE_RELATIONSHIP_TYPE );
            TokenHolder labelTokenHolder = new DelegatingTokenHolder( this::createNewLabelId, TokenHolder.TYPE_LABEL );
            tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );
            tokenHolders.setInitialTokens( StoreTokens.allTokens( neoStores ), storeCursors );
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

            fullScanStoreView = new FullScanStoreView( NO_LOCK_SERVICE, () -> new RecordStorageReader( neoStores ),
                                                       any -> new CachedStoreCursors( neoStores, cursorContext ), config, jobScheduler );
            indexProviderMap = life.add( StaticIndexProviderMapFactory.create(
                    life, config, pageCache, fileSystem, logService, monitors, readOnlyChecker, DbmsInfo.TOOL, immediate(), pageCacheTracer,
                    databaseLayout, tokenHolders, jobScheduler) );

            var schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders,
                                                                         neoStores.getMetaDataStore() );
            schemaCache = new SchemaCache( getConstraintSemantics(), indexProviderMap );
            schemaCache.load( schemaRuleAccess.getAll( storeCursors ) );

            // Record access
            recordAccess = new DirectRecordAccessSet( neoStores, idGeneratorFactory, cursorContext );
            relationshipGroupGetter = new RelationshipGroupGetter( relationshipGroupStore, cursorContext );
            relationshipCreator =
                    new RelationshipCreator( relationshipGroupStore.getStoreHeaderInt(), DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH, cursorContext );
            propertyTraverser = new PropertyTraverser();
            propertyCreator = new PropertyCreator( propertyStore, propertyTraverser, cursorContext, memoryTracker );
            propertyDeletor =
                    new PropertyDeleter( propertyTraverser, neoStores, tokenHolders, logService.getInternalLogProvider(), config, cursorContext, memoryTracker,
                            storeCursors );

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

    private void setPrimitiveProperty( RecordProxy<? extends PrimitiveRecord,Void> primitiveRecord,
            String propertyName, Object propertyValue )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords = recordAccess.getPropertyRecords();

        propertyCreator.primitiveSetProperty( primitiveRecord, propertyKey, ValueUtils.asValue( propertyValue ), propertyRecords );
    }

    private void repopulateAllIndexes() throws IOException
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        var cacheTracer = PageCacheTracer.NULL;

        IndexStoreViewFactory indexStoreViewFactory = new IndexStoreViewFactory( config, context -> new CachedStoreCursors( neoStores, context ),
                () -> new RecordStorageReader( neoStores, schemaCache ), NO_LOCKS, fullScanStoreView, NO_LOCK_SERVICE, logProvider );

        IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout.indexStatisticsStore(),
                immediate(), readOnlyChecker, databaseLayout.getDatabaseName(), cacheTracer );
        IndexingService indexingService = IndexingServiceFactory
                .createIndexingService( config, jobScheduler, indexProviderMap, indexStoreViewFactory, tokenHolders, emptyList(), logProvider,
                        IndexMonitor.NO_MONITOR, new DatabaseSchemaState( logProvider ), indexStatisticsStore, cacheTracer, memoryTracker,
                        databaseLayout.getDatabaseName(), readOnlyChecker );
        life.add( indexingService );
        try
        {
            IndexDescriptor[] descriptors = getIndexesNeedingPopulation( cursorContext );
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
            indexingService.forceAll( cursorContext );
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
        if ( fileSystem.fileExists( countsStoreFile ) )
        {
            fileSystem.deleteFile( countsStoreFile );
        }
        CountsComputer initialCountsBuilder =
                new CountsComputer( neoStores, pageCache, cacheTracer, databaseLayout, memoryTracker, logService.getInternalLog( getClass() ) );
        try ( GBPTreeCountsStore countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fileSystem, immediate(),
                initialCountsBuilder, readOnlyChecker, cacheTracer, NO_MONITOR, databaseLayout.getDatabaseName(),
                config.get( counts_store_max_cached_entries ), logService.getUserLogProvider() );
                var storeCursors = new CachedStoreCursors( neoStores, CursorContext.NULL ) )
        {
            countsStore.start( CursorContext.NULL, storeCursors, memoryTracker );
            countsStore.checkpoint( CursorContext.NULL );
        }
    }

    private void createEmptyTransactionLog()
    {
        TransactionLogInitializer.getLogFilesInitializer().initializeLogFiles( databaseLayout, neoStores.getMetaDataStore(), fileSystem, CHECKPOINT_REASON );
    }

    private IndexDescriptor[] getIndexesNeedingPopulation( CursorContext cursorContext )
    {
        List<IndexDescriptor> indexesNeedingPopulation = new ArrayList<>();
        for ( IndexDescriptor descriptor : schemaCache.indexes() )
        {
            IndexProvider provider = indexProviderMap.lookup( descriptor.getIndexProvider() );
            if ( provider.getInitialState( descriptor, cursorContext ) != InternalIndexState.FAILED )
            {
                indexesNeedingPopulation.add( descriptor );
            }
        }
        return indexesNeedingPopulation.toArray( new IndexDescriptor[0] );
    }

    private static TransactionFailureException kernelExceptionToUserException( KernelException e )
    {
        // This may look odd, but previously TokenHolder#getOrCreateId silently converted KernelException into TransactionFailureException
        throw new TransactionFailureException( "Unexpected kernel exception writing schema rules", e );
    }

    private static int silentGetOrCreateTokenId( TokenHolder tokens, String name )
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
        return internalCreateNode( nodeStore.nextId( cursorContext ), properties, labels );
    }

    private long internalCreateNode( long nodeId, Map<String, Object> properties, Label... labels )
    {
        NodeRecord nodeRecord = recordAccess.getNodeRecords().create( nodeId, null, cursorContext ).forChangingData();
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
        nodeLabels.put( getOrCreateLabelIds( labels ), nodeStore, nodeStore.getDynamicLabelStore(), cursorContext, storeCursors, memoryTracker );
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
        IdValidator.assertValidId( RecordIdType.NODE, id, maxNodeId );
        var nodeCursor = storeCursors.readCursor( RecordCursorTypes.NODE_CURSOR );
        if ( nodeStore.isInUse( id, nodeCursor ) )
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
            long[] labels = parseLabelsField( record ).get( nodeStore, storeCursors );
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
        for ( long label : parseLabelsField( record ).get( nodeStore, storeCursors ) )
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
        long id = relationshipStore.nextId( cursorContext );
        int typeId = getOrCreateRelationshipTypeId( type.name() );
        relationshipCreator.relationshipCreate( id, typeId, node1, node2, recordAccess, degreeUpdater,
                new RelationshipCreator.InsertFirst( relationshipGroupGetter, recordAccess, cursorContext ) );
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
        return nodeStore.isInUse( nodeId, storeCursors.readCursor( RecordCursorTypes.NODE_CURSOR ) );
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
        return new BatchRelationshipIterable<>( storageReader, nodeId, cursorContext, storeCursors )
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
        return new BatchRelationshipIterable<>( storageReader, nodeId, cursorContext, storeCursors )
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
        degreeUpdater.close();

        try ( cursorContext;
              var ignore = new Lifespan( life );
              locker;
              neoStores;
              groupDegreesStore;
              storeCursors )
        {
            rebuildCounts( pageCacheTracer, memoryTracker );
            repopulateAllIndexes();
            idGeneratorFactory.visit( IdGenerator::markHighestWrittenAtHighId );
            neoStores.flush( cursorContext );
            groupDegreesStore.checkpoint( cursorContext );
            recordAccess.close();
            createEmptyTransactionLog();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        msgLog.info( Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")" );
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
                Value propertyValue = propBlock.newPropertyValue( propertyStore, storeCursors );
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
        try ( var keyTokenCursor = storeCursors.writeCursor( PROPERTY_KEY_TOKEN_CURSOR ) )
        {
            return createNewToken( propertyKeyTokenStore, stringKey, internal, keyTokenCursor, storeCursors );
        }
    }

    private int createNewLabelId( String stringKey, boolean internal )
    {
        try ( var labelTokenCursor = storeCursors.writeCursor( LABEL_TOKEN_CURSOR ) )
        {
            return createNewToken( labelTokenStore, stringKey, internal, labelTokenCursor, storeCursors );
        }
    }

    private int createNewRelationshipType( String name, boolean internal )
    {
        try ( var relTypeToken = storeCursors.writeCursor( REL_TYPE_TOKEN_CURSOR ) )
        {
            return createNewToken( relationshipTypeTokenStore, name, internal, relTypeToken, storeCursors );
        }
    }

    private <R extends TokenRecord> int createNewToken( TokenStore<R> store, String name, boolean internal, PageCursor writeCursor, StoreCursors storeCursors )
    {
        int keyId = (int) store.nextId( cursorContext );
        R record = store.newRecord();
        record.setId( keyId );
        record.setInUse( true );
        record.setInternal( internal );
        record.setCreated();
        Collection<DynamicRecord> keyRecords = store.allocateNameRecords( encodeString( name ), cursorContext, memoryTracker );
        record.setNameId( (int) Iterables.first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        store.updateRecord( record, writeCursor, cursorContext, storeCursors );
        return keyId;
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
