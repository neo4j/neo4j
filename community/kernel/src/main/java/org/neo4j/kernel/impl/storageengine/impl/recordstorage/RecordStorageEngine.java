/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.diagnostics.providers.NeoStoresDiagnostics.NeoStoreIdUsage;
import org.neo4j.kernel.diagnostics.providers.NeoStoresDiagnostics.NeoStoreRecords;
import org.neo4j.kernel.diagnostics.providers.NeoStoresDiagnostics.NeoStoreVersions;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.ReadOnlyCountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.command.CacheInvalidationBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.HighIdBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.command.IndexUpdatesWork;
import org.neo4j.kernel.impl.transaction.command.LabelUpdateWork;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.util.concurrent.WorkSync;

import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

public class RecordStorageEngine implements StorageEngine, Lifecycle
{
    private final IndexingService indexingService;
    private final NeoStores neoStores;
    private final TokenHolders tokenHolders;
    private final DatabaseHealth databaseHealth;
    private final SchemaCache schemaCache;
    private final IntegrityValidator integrityValidator;
    private final CacheAccessBackDoor cacheAccess;
    private final LabelScanStore labelScanStore;
    private final IndexProviderMap indexProviderMap;
    private final SchemaState schemaState;
    private final SchemaRuleAccess schemaRuleAccess;
    private final ConstraintSemantics constraintSemantics;
    private final LockService lockService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;
    private final CommandReaderFactory commandReaderFactory;
    private final WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync;
    private final IndexStoreView indexStoreView;
    private final PropertyPhysicalToLogicalConverter indexUpdatesConverter;
    private final IdController idController;
    private final CountsTracker countsStore;
    private final int denseNodeThreshold;
    private final int recordIdBatchSize;

    public RecordStorageEngine(
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogProvider logProvider,
            LogProvider userLogProvider,
            TokenHolders tokenHolders,
            SchemaState schemaState,
            ConstraintSemantics constraintSemantics,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            LockService lockService,
            IndexProviderMap indexProviderMap,
            IndexingService.Monitor indexingServiceMonitor,
            DatabaseHealth databaseHealth,
            IdGeneratorFactory idGeneratorFactory,
            IdController idController,
            Monitors monitors,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            OperationalMode operationalMode,
            VersionContextSupplier versionContextSupplier )
    {
        this.tokenHolders = tokenHolders;
        this.schemaState = schemaState;
        this.lockService = lockService;
        this.databaseHealth = databaseHealth;
        this.constraintSemantics = constraintSemantics;
        this.idController = idController;

        StoreFactory factory = new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fs, logProvider,
                versionContextSupplier );
        neoStores = factory.openAllNeoStores( true );

        try
        {
            indexUpdatesConverter = new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() );
            schemaCache = new SchemaCache( constraintSemantics, Collections.emptyList(), indexProviderMap );
            schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore() );

            countsStore = openCountsStore( fs, pageCache, databaseLayout, config, logProvider, versionContextSupplier );

            NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( lockService, neoStores, countsStore, this::newReader );
            boolean readOnly = config.get( GraphDatabaseSettings.read_only ) && operationalMode == OperationalMode.SINGLE;
            monitors.addMonitorListener( new LoggingMonitor( logProvider.getLog( NativeLabelScanStore.class ) ) );
            labelScanStore = new NativeLabelScanStore( pageCache, databaseLayout, fs, new FullLabelStream( neoStoreIndexStoreView ),
                    readOnly, monitors, recoveryCleanupWorkCollector );

            // We need to load the property tokens here, since we need them before we load the indexes.
            tokenHolders.propertyKeyTokens().setInitialTokens( neoStores.getPropertyKeyTokenStore().getTokens() );

            indexStoreView = new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, lockService, neoStores, logProvider );
            this.indexProviderMap = indexProviderMap;
            indexingService = IndexingServiceFactory.createIndexingService( config, scheduler, indexProviderMap,
                    indexStoreView, tokenNameLookup,
                    Iterators.asList( schemaRuleAccess.indexesGetAll() ), logProvider, userLogProvider,
                    indexingServiceMonitor, schemaState );

            integrityValidator = new IntegrityValidator( neoStores, indexingService );
            cacheAccess = new BridgingCacheAccess( schemaCache, schemaState, tokenHolders );

            labelScanStoreSync = new WorkSync<>( labelScanStore::newWriter );

            commandReaderFactory = new RecordStorageCommandReaderFactory();
            indexUpdatesSync = new WorkSync<>( indexingService );

            denseNodeThreshold = config.get( GraphDatabaseSettings.dense_node_threshold );
            recordIdBatchSize = config.get( GraphDatabaseSettings.record_id_batch_size );
        }
        catch ( Throwable failure )
        {
            neoStores.close();
            throw failure;
        }
    }

    private CountsTracker openCountsStore( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, Config config, LogProvider logProvider,
            VersionContextSupplier versionContextSupplier )
    {
        boolean readOnly = config.get( GraphDatabaseSettings.read_only );
        CountsTracker counts = readOnly
                               ? new ReadOnlyCountsTracker( logProvider, fs, pageCache, config, layout )
                               : new CountsTracker( logProvider, fs, pageCache, config, layout, versionContextSupplier );
        counts.setInitializer( new DataInitializer<CountsAccessor.Updater>()
        {
            private final Log log = logProvider.getLog( MetaDataStore.class );

            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                log.warn( "Missing counts store, rebuilding it." );
                new CountsComputer( neoStores, pageCache, layout ).initialize( updater );
                log.warn( "Counts store rebuild completed." );
            }

            @Override
            public long initialVersion()
            {
                return neoStores.getMetaDataStore().getLastCommittedTransactionId();
            }
        } );
        return counts;
    }

    @Override
    public StorageReader newReader()
    {
        return new RecordStorageReader( tokenHolders, neoStores, countsStore, indexingService, schemaCache );
    }

    @Override
    public RecordStorageCommandCreationContext newCommandCreationContext()
    {
        return new RecordStorageCommandCreationContext( neoStores, denseNodeThreshold, recordIdBatchSize );
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
            StorageReader storageReader,
            CommandCreationContext commandCreationContext,
            ResourceLocker locks,
            long lastTransactionIdWhenStarted,
            TxStateVisitor.Decorator additionalTxStateVisitor )
            throws TransactionFailureException, CreateConstraintFailureException, ConstraintValidationException
    {
        if ( txState != null )
        {
            // We can make this cast here because we expected that the storageReader passed in here comes from
            // this storage engine itself, anything else is considered a bug. And we do know the inner workings
            // of the storage statements that we create.
            RecordStorageCommandCreationContext creationContext = (RecordStorageCommandCreationContext) commandCreationContext;
            TransactionRecordState recordState = creationContext.createTransactionRecordState( integrityValidator, lastTransactionIdWhenStarted, locks );

            // Visit transaction state and populate these record state objects
            TxStateVisitor txStateVisitor = new TransactionToRecordStateVisitor( recordState, schemaState,
                    schemaRuleAccess, constraintSemantics );
            CountsRecordState countsRecordState = new CountsRecordState();
            txStateVisitor = additionalTxStateVisitor.apply( txStateVisitor );
            txStateVisitor = new TransactionCountingStateVisitor(
                    txStateVisitor, storageReader, txState, countsRecordState );
            try ( TxStateVisitor visitor = txStateVisitor )
            {
                txState.accept( visitor );
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
            }
        }
        catch ( Throwable cause )
        {
            TransactionApplyKernelException kernelException =
                    new TransactionApplyKernelException( cause, "Failed to apply transaction: %s", batch );
            databaseHealth.panic( kernelException );
            throw kernelException;
        }
    }

    /**
     * Creates a {@link BatchTransactionApplierFacade} that is to be used for all transactions
     * in a batch. Each transaction is handled by a {@link TransactionApplierFacade} which wraps the
     * individual {@link TransactionApplier}s returned by the wrapped {@link BatchTransactionApplier}s.
     *
     * After all transactions have been applied the appliers are closed.
     */
    protected BatchTransactionApplierFacade applier( TransactionApplicationMode mode )
    {
        ArrayList<BatchTransactionApplier> appliers = new ArrayList<>();
        // Graph store application. The order of the decorated store appliers is irrelevant
        appliers.add( new NeoStoreBatchTransactionApplier( mode.version(), neoStores, cacheAccess, lockService( mode ) ) );
        if ( mode.needsHighIdTracking() )
        {
            appliers.add( new HighIdBatchTransactionApplier( neoStores ) );
        }
        if ( mode.needsCacheInvalidationOnUpdates() )
        {
            appliers.add( new CacheInvalidationBatchTransactionApplier( neoStores, cacheAccess ) );
        }
        if ( mode.needsAuxiliaryStores() )
        {
            // Counts store application
            appliers.add( new CountsStoreBatchTransactionApplier( countsStore, mode ) );

            // Schema index application
            appliers.add( new IndexBatchTransactionApplier( indexingService, labelScanStoreSync, indexUpdatesSync,
                    neoStores.getNodeStore(), neoStores.getRelationshipStore(),
                    indexUpdatesConverter, this ) );
        }

        // Perform the application
        return new BatchTransactionApplierFacade(
                appliers.toArray( new BatchTransactionApplier[appliers.size()] ) );
    }

    private LockService lockService( TransactionApplicationMode mode )
    {
        return mode == RECOVERY || mode == REVERSE_RECOVERY ? NO_LOCK_SERVICE : lockService;
    }

    public void satisfyDependencies( DependencySatisfier satisfier )
    {
        satisfier.satisfyDependency( cacheAccess );
        satisfier.satisfyDependency( indexProviderMap );
        satisfier.satisfyDependency( integrityValidator );
        satisfier.satisfyDependency( labelScanStore );
        satisfier.satisfyDependency( indexingService );
        satisfier.satisfyDependency( neoStores.getMetaDataStore() );
        satisfier.satisfyDependency( indexStoreView );
    }

    @Override
    public void init() throws Throwable
    {
        countsStore.init();
        indexingService.init();
        labelScanStore.init();
    }

    @Override
    public void start() throws Throwable
    {
        neoStores.makeStoreOk();

        tokenHolders.propertyKeyTokens().setInitialTokens(
                neoStores.getPropertyKeyTokenStore().getTokens() );
        tokenHolders.relationshipTypeTokens().setInitialTokens(
                neoStores.getRelationshipTypeTokenStore().getTokens() );
        tokenHolders.labelTokens().setInitialTokens(
                neoStores.getLabelTokenStore().getTokens() );

        countsStore.start();
        loadSchemaCache();
        indexingService.start();
        labelScanStore.start();
        idController.start();
    }

    @VisibleForTesting
    public void loadSchemaCache()
    {
        List<SchemaRule> schemaRules = Iterators.asList( neoStores.getSchemaStore().loadAllSchemaRules() );
        schemaCache.load( schemaRules );
    }

    @Override
    public void stop() throws Throwable
    {
        idController.stop();
        labelScanStore.stop();
        indexingService.stop();
        countsStore.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        labelScanStore.shutdown();
        indexingService.shutdown();
        countsStore.shutdown();
        neoStores.close();
    }

    @Override
    public void flushAndForce( IOLimiter limiter )
    {
        indexingService.forceAll( limiter );
        labelScanStore.force( limiter );
        try
        {
            countsStore.rotate( neoStores.getMetaDataStore().getLastCommittedTransactionId() );
            neoStores.flush( limiter );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    @Override
    public void dumpDiagnostics( DiagnosticsManager diagnosticsManager, Log log )
    {
        diagnosticsManager.dump( new NeoStoreIdUsage( neoStores ), log );
        diagnosticsManager.dump( new NeoStoreRecords( neoStores ), log );
        diagnosticsManager.dump( new NeoStoreVersions( neoStores ), log );
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

    @Override
    public Collection<StoreFileMetadata> listStorageFiles()
    {
        List<StoreFileMetadata> files = new ArrayList<>();
        addCountStoreFiles( files );
        for ( StoreType type : StoreType.values() )
        {
            final RecordStore<AbstractBaseRecord> recordStore = neoStores.getRecordStore( type );
            StoreFileMetadata metadata =
                    new StoreFileMetadata( recordStore.getStorageFile(), recordStore.getRecordSize() );
            files.add( metadata );
        }
        return files;
    }

    private void addCountStoreFiles( List<StoreFileMetadata> files )
    {
        Iterable<File> countStoreFiles = countsStore.allFiles();
        for ( File countStoreFile : countStoreFiles )
        {
            StoreFileMetadata countStoreFileMetadata = new StoreFileMetadata( countStoreFile,
                    RecordFormat.NO_RECORD_SIZE );
            files.add( countStoreFileMetadata );
        }
    }

    /**
     * @return the underlying {@link NeoStores} which should <strong>ONLY</strong> be accessed by tests
     * until all tests are properly converted to not rely on access to {@link NeoStores}. Currently there
     * are important tests which asserts details about the neo stores that are very important to test,
     * but to convert all those tests might be a bigger piece of work.
     */
    @VisibleForTesting
    public NeoStores testAccessNeoStores()
    {
        return neoStores;
    }

    @VisibleForTesting
    public CountsTracker testAccessCountsStore()
    {
        return countsStore;
    }

    @Override
    public StoreId getStoreId()
    {
        return neoStores.getMetaDataStore().getStoreId();
    }
}
