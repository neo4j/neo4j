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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.api.LegacyBatchIndexApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.command.CacheInvalidationBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.HighIdBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexUpdatesWork;
import org.neo4j.kernel.impl.transaction.command.LabelUpdateWork;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.Loaders;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyDeleter;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreator;
import org.neo4j.kernel.impl.transaction.state.RelationshipDeleter;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.state.storeview.AdaptableIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

public class RecordStorageEngine implements StorageEngine, Lifecycle
{
    private static final boolean safeIdBuffering = flag( RecordStorageEngine.class, "safeIdBuffering", true );

    private final StoreReadLayer storeLayer;
    private final IndexingService indexingService;
    private final NeoStores neoStores;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final DatabaseHealth databaseHealth;
    private final IndexConfigStore indexConfigStore;
    private final SchemaCache schemaCache;
    private final IntegrityValidator integrityValidator;
    private final CacheAccessBackDoor cacheAccess;
    private final LabelScanStore labelScanStore;
    private final DefaultSchemaIndexProviderMap schemaIndexProviderMap;
    private final LegacyIndexApplierLookup legacyIndexApplierLookup;
    private final Runnable schemaStateChangeCallback;
    private final SchemaStorage schemaStorage;
    private final ConstraintSemantics constraintSemantics;
    private final IdOrderingQueue legacyIndexTransactionOrdering;
    private final JobScheduler scheduler;
    private final LockService lockService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;
    private final CommandReaderFactory commandReaderFactory;
    private final WorkSync<IndexingService,IndexUpdatesWork> indexUpdatesSync;
    private final NeoStoreIndexStoreView indexStoreView;
    private final LegacyIndexProviderLookup legacyIndexProviderLookup;
    private final PropertyPhysicalToLogicalConverter indexUpdatesConverter;
    private final Supplier<StorageStatement> storeStatementSupplier;

    private BufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private JobScheduler.JobHandle idBufferMaintenance;

    // Immutable state for creating/applying commands
    private final Loaders loaders;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter relationshipDeleter;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeleter;

    public RecordStorageEngine(
            File storeDir,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogProvider logProvider,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            Runnable schemaStateChangeCallback,
            ConstraintSemantics constraintSemantics,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            LockService lockService,
            SchemaIndexProvider indexProvider,
            IndexingService.Monitor indexingServiceMonitor,
            DatabaseHealth databaseHealth,
            LabelScanStoreProvider labelScanStoreProvider,
            LegacyIndexProviderLookup legacyIndexProviderLookup,
            IndexConfigStore indexConfigStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier,
            RecordFormats recordFormats )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokens;
        this.labelTokenHolder = labelTokens;
        this.schemaStateChangeCallback = schemaStateChangeCallback;
        this.scheduler = scheduler;
        this.lockService = lockService;
        this.databaseHealth = databaseHealth;
        this.legacyIndexProviderLookup = legacyIndexProviderLookup;
        this.indexConfigStore = indexConfigStore;
        this.constraintSemantics = constraintSemantics;
        this.legacyIndexTransactionOrdering = legacyIndexTransactionOrdering;

        final StoreFactory storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory,
                pageCache, fs, recordFormats, logProvider );
        if ( safeIdBuffering )
        {
            // This buffering id generator factory will have properly buffering id generators injected into
            // the stores. The buffering depends on knowledge about active transactions,
            // so we'll initialize it below when all those components have been instantiated.
            bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory(
                    idGeneratorFactory, transactionsSnapshotSupplier );
            storeFactory.setIdGeneratorFactory( bufferingIdGeneratorFactory );
        }
        neoStores = storeFactory.openAllNeoStores( true );

        try
        {
            indexUpdatesConverter = new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() );
            schemaCache = new SchemaCache( constraintSemantics, Collections.emptyList() );
            schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );

            labelScanStore = labelScanStoreProvider.getLabelScanStore();

            schemaIndexProviderMap = new DefaultSchemaIndexProviderMap( indexProvider );
            indexStoreView = new AdaptableIndexStoreView( labelScanStore, lockService, neoStores );
            indexingService = IndexingServiceFactory.createIndexingService( config, scheduler, schemaIndexProviderMap,
                    indexStoreView, tokenNameLookup,
                    Iterators.asList( new SchemaStorage( neoStores.getSchemaStore() ).allIndexRules() ), logProvider,
                    indexingServiceMonitor, schemaStateChangeCallback );

            integrityValidator = new IntegrityValidator( neoStores, indexingService );
            cacheAccess = new BridgingCacheAccess( schemaCache, schemaStateChangeCallback,
                    propertyKeyTokenHolder, relationshipTypeTokens, labelTokens );

            storeStatementSupplier = storeStatementSupplier( neoStores );
            DiskLayer diskLayer = new DiskLayer(
                    propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                    schemaStorage, neoStores, indexingService,
                    storeStatementSupplier );
            storeLayer = new CacheLayer( diskLayer, schemaCache );

            legacyIndexApplierLookup = new LegacyIndexApplierLookup.Direct( legacyIndexProviderLookup );

            labelScanStoreSync = new WorkSync<>( labelScanStore::newWriter );

            commandReaderFactory = new RecordStorageCommandReaderFactory();
            indexUpdatesSync = new WorkSync<>( indexingService );

            // Immutable state for creating/applying commands
            loaders = new Loaders( neoStores );
            RelationshipGroupGetter relationshipGroupGetter =
                    new RelationshipGroupGetter( neoStores.getRelationshipGroupStore() );
            relationshipCreator = new RelationshipCreator( relationshipGroupGetter,
                    config.get( GraphDatabaseSettings.dense_node_threshold ) );
            PropertyTraverser propertyTraverser = new PropertyTraverser();
            propertyDeleter = new PropertyDeleter( propertyTraverser );
            relationshipDeleter = new RelationshipDeleter( relationshipGroupGetter, propertyDeleter );
            propertyCreator = new PropertyCreator( neoStores.getPropertyStore(), propertyTraverser );
        }
        catch ( Throwable failure )
        {
            neoStores.close();
            throw failure;
        }
    }

    private Supplier<StorageStatement> storeStatementSupplier( NeoStores neoStores )
    {
        final Supplier<IndexReaderFactory> indexReaderFactory = () -> new IndexReaderFactory.Caching( indexingService );

        return () -> new StoreStatement( neoStores, indexReaderFactory, labelScanStore::newReader );
    }

    @Override
    public StoreReadLayer storeReadLayer()
    {
        return storeLayer;
    }

    @Override
    public CommandReaderFactory commandReaderFactory()
    {
        return commandReaderFactory;
    }

    @SuppressWarnings( "resource" )
    @Override
    public void createCommands(
            Collection<StorageCommand> commands,
            ReadableTransactionState txState,
            StorageStatement storageStatement,
            ResourceLocker locks,
            long lastTransactionIdWhenStarted )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationKernelException
    {
        if ( txState != null )
        {
            RecordChangeSet recordChangeSet = new RecordChangeSet( loaders );
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator,
                    recordChangeSet, lastTransactionIdWhenStarted, locks,
                    relationshipCreator, relationshipDeleter, propertyCreator, propertyDeleter );

            // Visit transaction state and populate these record state objects
            TxStateVisitor txStateVisitor = new TransactionToRecordStateVisitor( recordState,
                    schemaStateChangeCallback, schemaStorage, constraintSemantics, schemaIndexProviderMap );
            CountsRecordState countsRecordState = new CountsRecordState();
            txStateVisitor = constraintSemantics.decorateTxStateVisitor(
                    storeLayer,
                    txState,
                    txStateVisitor );
            txStateVisitor = new TransactionCountingStateVisitor(
                    txStateVisitor, storeLayer, storageStatement, txState, countsRecordState );
            try ( TxStateVisitor visitor = txStateVisitor )
            {
                txState.accept( txStateVisitor );
            }

            // Convert record state into commands
            recordState.extractCommands( commands );
            countsRecordState.extractCommands( commands );
        }
    }

    @Override
    public void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception
    {
        // Have these command appliers as separate try-with-resource to have better control over
        // point between closing this and the locks above
        try ( BatchTransactionApplier batchApplier = applier( mode ) )
        {
            while ( batch != null )
            {
                try ( LockGroup locks = new LockGroup() )
                {
                    try ( TransactionApplier txApplier = batchApplier.startTx( batch, locks ) )
                    {
                        batch.accept( txApplier );
                    }
                    batch = batch.next();
                }
                catch ( Throwable cause )
                {
                    databaseHealth.panic( cause );
                    throw cause;
                }
            }
        }
    }

    /**
     * Creates a {@link BatchTransactionApplierFacade} that is to be used for all transactions
     * in a batch. Each transaction is handled by a {@link TransactionApplierFacade} which wraps the
     * individual {@link TransactionApplier}s returned by the wrapped {@link BatchTransactionApplier}s.
     *
     * After all transactions have been applied the appliers are closed.
     */
    private BatchTransactionApplierFacade applier( TransactionApplicationMode mode )
    {
        ArrayList<BatchTransactionApplier> appliers = new ArrayList<>();
        // Graph store application. The order of the decorated store appliers is irrelevant
        appliers.add( new NeoStoreBatchTransactionApplier( neoStores, cacheAccess, lockService ) );
        if ( mode.needsHighIdTracking() )
        {
            appliers.add( new HighIdBatchTransactionApplier( neoStores ) );
        }
        if ( mode.needsCacheInvalidationOnUpdates() )
        {
            appliers.add( new CacheInvalidationBatchTransactionApplier( neoStores, cacheAccess ) );
        }

        // Schema index application
        appliers.add( new IndexBatchTransactionApplier( indexingService, labelScanStoreSync, indexUpdatesSync,
                neoStores.getNodeStore(), new PropertyLoader( neoStores ),
                indexUpdatesConverter, mode ) );

        // Legacy index application
        appliers.add(
                new LegacyBatchIndexApplier( indexConfigStore, legacyIndexApplierLookup, legacyIndexTransactionOrdering,
                        mode ) );

        // Counts store application
        appliers.add( new CountsStoreBatchTransactionApplier( neoStores.getCounts(), mode ) );

        // Perform the application
        return new BatchTransactionApplierFacade(
                appliers.toArray( new BatchTransactionApplier[appliers.size()] ) );
    }

    public void satisfyDependencies( DependencySatisfier satisfier )
    {
        satisfier.satisfyDependency( legacyIndexApplierLookup );
        satisfier.satisfyDependency( cacheAccess );
        satisfier.satisfyDependency( indexStoreView );
        satisfier.satisfyDependency( schemaIndexProviderMap );
        satisfier.satisfyDependency( integrityValidator );
        satisfier.satisfyDependency( labelScanStore );
        satisfier.satisfyDependency( indexingService );
        // providing TransactionIdStore, LogVersionRepository
        satisfier.satisfyDependency( neoStores.getMetaDataStore() );
    }

    @Override
    public void init() throws Throwable
    {
        indexingService.init();
        labelScanStore.init();
    }

    @Override
    public void start() throws Throwable
    {
        neoStores.makeStoreOk();

        propertyKeyTokenHolder.setInitialTokens(
                neoStores.getPropertyKeyTokenStore().getTokens( Integer.MAX_VALUE ) );
        relationshipTypeTokenHolder.setInitialTokens(
                neoStores.getRelationshipTypeTokenStore().getTokens( Integer.MAX_VALUE ) );
        labelTokenHolder.setInitialTokens(
                neoStores.getLabelTokenStore().getTokens( Integer.MAX_VALUE ) );

        neoStores.rebuildCountStoreIfNeeded(); // TODO: move this to counts store lifecycle
        loadSchemaCache();
        indexingService.start();
        labelScanStore.start();
        if ( safeIdBuffering )
        {
            Runnable maintenance = bufferingIdGeneratorFactory::maintenance;
            idBufferMaintenance = scheduler.scheduleRecurring(
                    JobScheduler.Groups.storageMaintenance, maintenance, 1, TimeUnit.SECONDS );
        }
    }

    @Override
    public void loadSchemaCache()
    {
        List<SchemaRule> schemaRules = Iterators.asList( neoStores.getSchemaStore().loadAllSchemaRules() );
        schemaCache.load( schemaRules );
    }

    @Override
    public void stop() throws Throwable
    {
        labelScanStore.stop();
        indexingService.stop();
        if ( safeIdBuffering )
        {
            idBufferMaintenance.cancel( false );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        labelScanStore.shutdown();
        indexingService.shutdown();
        neoStores.close();
    }

    @Override
    public void flushAndForce( IOLimiter limiter )
    {
        indexingService.forceAll();
        labelScanStore.force();
        for ( IndexImplementation index : legacyIndexProviderLookup.all() )
        {
            index.force();
        }
        neoStores.flush( limiter );
    }

    @Override
    public void registerDiagnostics( DiagnosticsManager diagnosticsManager )
    {
        neoStores.registerDiagnostics( diagnosticsManager );
    }

    @Override
    public void forceClose()
    {
        try
        {
            shutdown();
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void prepareForRecoveryRequired()
    {
        neoStores.deleteIdGenerators();
    }

    /**
     * @return the underlying {@link NeoStores} which should <strong>ONLY</strong> be accessed by tests
     * until all tests are properly converted to not rely on access to {@link NeoStores}. Currently there
     * are important tests which asserts details about the neo stores that are very important to test,
     * but to convert all those tests might be a bigger piece of work.
     */
    public NeoStores testAccessNeoStores()
    {
        return neoStores;
    }
}
