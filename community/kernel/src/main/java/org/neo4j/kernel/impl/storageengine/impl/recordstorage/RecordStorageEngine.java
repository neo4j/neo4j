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
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.LegacyBatchIndexApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
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
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.command.CacheInvalidationBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.HighIdBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexUpdatesWork;
import org.neo4j.kernel.impl.transaction.command.LabelUpdateWork;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

public class RecordStorageEngine implements StorageEngine, Lifecycle
{
    /**
     * This setting is hidden to the user and is here merely for making it easier to back out of
     * a change where reading property chains incurs read locks on {@link LockService}.
     */
    private static final Setting<Boolean> use_read_locks_on_property_reads =
            setting( "experimental.use_read_locks_on_property_reads", BOOLEAN, TRUE );

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
    private final DefaultSchemaIndexProviderMap providerMap;
    private final LegacyIndexApplierLookup legacyIndexApplierLookup;
    private final Runnable schemaStateChangeCallback;
    private final SchemaStorage schemaStorage;
    private final ConstraintSemantics constraintSemantics;
    private final IdOrderingQueue legacyIndexTransactionOrdering;
    private final LockService lockService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;
    private final CommandReaderFactory commandReaderFactory;
    private final WorkSync<IndexingService,IndexUpdatesWork> indexUpdatesSync;

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
            IndexConfigStore indexConfigStore )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokens;
        this.labelTokenHolder = labelTokens;
        this.schemaStateChangeCallback = schemaStateChangeCallback;
        this.lockService = lockService;
        this.databaseHealth = databaseHealth;
        this.indexConfigStore = indexConfigStore;
        this.constraintSemantics = constraintSemantics;

        final StoreFactory storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fs, logProvider );
        neoStores = storeFactory.openAllNeoStores( true );

        try
        {
            schemaCache = new SchemaCache( constraintSemantics, Collections.<SchemaRule>emptyList() );
            schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );

            providerMap = new DefaultSchemaIndexProviderMap( indexProvider );
            indexingService = IndexingServiceFactory.createIndexingService(
                    new IndexSamplingConfig( config ), scheduler, providerMap,
                    new NeoStoreIndexStoreView( lockService, neoStores ), tokenNameLookup,
                    toList( new SchemaStorage( neoStores.getSchemaStore() ).allIndexRules() ), logProvider,
                    indexingServiceMonitor, schemaStateChangeCallback );

            integrityValidator = new IntegrityValidator( neoStores, indexingService );
            cacheAccess = new BridgingCacheAccess( schemaCache, schemaStateChangeCallback,
                    propertyKeyTokenHolder, relationshipTypeTokens, labelTokens );

            labelScanStore = labelScanStoreProvider.getLabelScanStore();
            DiskLayer diskLayer = new DiskLayer(
                    propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                    schemaStorage, neoStores, indexingService,
                    storeStatementSupplier( neoStores, config, lockService ) );
            storeLayer = new CacheLayer( diskLayer, schemaCache );
            legacyIndexApplierLookup = new LegacyIndexApplierLookup.Direct( legacyIndexProviderLookup );
            legacyIndexTransactionOrdering = new SynchronizedArrayIdOrderingQueue( 20 );

            labelScanStoreSync = new WorkSync<>( labelScanStore::newWriter );

            this.commandReaderFactory = new RecordStorageCommandReaderFactory();
            indexUpdatesSync = new WorkSync<>( indexingService );
        }
        catch ( Throwable failure )
        {
            neoStores.close();
            throw failure;
        }
    }

    private Supplier<StorageStatement> storeStatementSupplier(
            NeoStores neoStores, Config config, LockService lockService )
    {
        final LockService currentLockService =
                config.get( use_read_locks_on_property_reads ) ? lockService : NO_LOCK_SERVICE;
        final Supplier<LabelScanReader> labelScanReaderSupplier = labelScanStore::newReader;

        return () -> {
            return new StoreStatement( neoStores, currentLockService,
                    new IndexReaderFactory.Caching( indexingService ), labelScanReaderSupplier );
        };
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
            ResourceLocker locks,
            long lastTransactionIdWhenStarted )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationKernelException
    {
        if ( txState != null )
        {
            NeoStoreTransactionContext context = new NeoStoreTransactionContext( neoStores, locks );
            TransactionRecordState recordState = new TransactionRecordState( neoStores, integrityValidator, context );
            recordState.initialize( lastTransactionIdWhenStarted );

            // Visit transaction state and populate these record state objects
            TxStateVisitor txStateVisitor = new TransactionToRecordStateVisitor( recordState,
                    schemaStateChangeCallback, schemaStorage, constraintSemantics, providerMap );
            CountsRecordState countsRecordState = new CountsRecordState();
            txStateVisitor = constraintSemantics.decorateTxStateVisitor(
                    storeLayer,
                    txState,
                    txStateVisitor );
            txStateVisitor = new TransactionCountingStateVisitor(
                    txStateVisitor, storeLayer, txState, countsRecordState );
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
                neoStores.getNodeStore(), neoStores.getPropertyStore(), new PropertyLoader( neoStores ), mode ) );

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

    @Override
    public TransactionIdStore transactionIdStore()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public LogVersionRepository logVersionRepository()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public NeoStores neoStores()
    {
        return neoStores;
    }

    @Override
    public MetaDataStore metaDataStore()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public IndexingService indexingService()
    {
        return indexingService;
    }

    @Override
    public LabelScanStore labelScanStore()
    {
        return labelScanStore;
    }

    @Override
    public IntegrityValidator integrityValidator()
    {
        return integrityValidator;
    }

    @Override
    public SchemaIndexProviderMap schemaIndexProviderMap()
    {
        return providerMap;
    }

    @Override
    public CacheAccessBackDoor cacheAccess()
    {
        return cacheAccess;
    }

    @Override
    public LegacyIndexApplierLookup legacyIndexApplierLookup()
    {
        return legacyIndexApplierLookup;
    }

    @Override
    public IndexConfigStore indexConfigStore()
    {
        return indexConfigStore;
    }

    @Override
    public IdOrderingQueue legacyIndexTransactionOrdering()
    {
        return legacyIndexTransactionOrdering;
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
    }

    @Override
    public void loadSchemaCache()
    {
        List<SchemaRule> schemaRules = toList( neoStores.getSchemaStore().loadAllSchemaRules() );
        schemaCache.load( schemaRules );
    }

    @Override
    public void stop() throws Throwable
    {
        labelScanStore.stop();
        indexingService.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        labelScanStore.shutdown();
        indexingService.shutdown();
        neoStores.close();
    }
}
