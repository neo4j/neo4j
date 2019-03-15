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
package org.neo4j.internal.recordstorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.recordstorage.NeoStoresDiagnostics.NeoStoreIdUsage;
import org.neo4j.internal.recordstorage.NeoStoresDiagnostics.NeoStoreRecords;
import org.neo4j.internal.recordstorage.NeoStoresDiagnostics.NeoStoreVersions;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.ReadOnlyCountsTracker;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.util.concurrent.WorkSync;

import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

public class RecordStorageEngine implements StorageEngine, Lifecycle
{
    private final NeoStores neoStores;
    private final TokenHolders tokenHolders;
    private final Health databaseHealth;
    private final SchemaCache schemaCache;
    private final IntegrityValidator integrityValidator;
    private final CacheAccessBackDoor cacheAccess;
    private final SchemaState schemaState;
    private final SchemaRuleAccess schemaRuleAccess;
    private final ConstraintRuleAccessor constraintSemantics;
    private final LockService lockService;
    private WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanStoreSync;
    private WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync;
    private final PropertyPhysicalToLogicalConverter indexUpdatesConverter;
    private final IdController idController;
    private final CountsTracker countsStore;
    private final int denseNodeThreshold;
    private final int recordIdBatchSize;

    // installed later
    private IndexUpdateListener indexUpdateListener;
    private NodeLabelUpdateListener nodeLabelUpdateListener;

    public RecordStorageEngine(
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogProvider logProvider,
            TokenHolders tokenHolders,
            SchemaState schemaState,
            ConstraintRuleAccessor constraintSemantics,
            LockService lockService,
            Health databaseHealth,
            IdGeneratorFactory idGeneratorFactory,
            IdController idController,
            VersionContextSupplier versionContextSupplier )
    {
        this.tokenHolders = tokenHolders;
        this.schemaState = schemaState;
        this.lockService = lockService;
        this.databaseHealth = databaseHealth;
        this.constraintSemantics = constraintSemantics;
        this.idController = idController;

        StoreFactory factory = new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fs, logProvider );
        neoStores = factory.openAllNeoStores( true );

        try
        {
            indexUpdatesConverter = new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() );
            schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders );
            schemaCache = new SchemaCache( constraintSemantics );

            integrityValidator = new IntegrityValidator( neoStores );
            cacheAccess = new BridgingCacheAccess( schemaCache, schemaState, tokenHolders );

            denseNodeThreshold = config.get( GraphDatabaseSettings.dense_node_threshold );
            recordIdBatchSize = config.get( GraphDatabaseSettings.record_id_batch_size );

            countsStore = openCountsStore( fs, pageCache, databaseLayout, config, logProvider, versionContextSupplier );
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
    public RecordStorageReader newReader()
    {
        return new RecordStorageReader( tokenHolders, neoStores, countsStore, schemaCache );
    }

    @Override
    public RecordStorageCommandCreationContext newCommandCreationContext()
    {
        return new RecordStorageCommandCreationContext( neoStores, denseNodeThreshold, recordIdBatchSize );
    }

    @Override
    public void addIndexUpdateListener( IndexUpdateListener listener )
    {
        Preconditions.checkState( this.indexUpdateListener == null,
                "Only supports a single listener. Tried to add " + listener + ", but " + this.indexUpdateListener + " has already been added" );
        this.indexUpdateListener = listener;
        this.indexUpdatesSync = new WorkSync<>( listener );
        this.integrityValidator.setIndexValidator( listener );
    }

    @Override
    public void addNodeLabelUpdateListener( NodeLabelUpdateListener listener )
    {
        Preconditions.checkState( this.nodeLabelUpdateListener == null,
                "Only supports a single listener. Tried to add " + listener + ", but " + this.nodeLabelUpdateListener + " has already been added" );
        this.nodeLabelUpdateListener = listener;
        this.labelScanStoreSync = new WorkSync<>( listener );
    }

    /**
     * @throws TransactionFailureException if command generation fails or some prerequisite of some command didn't validate,
     * for example if trying to delete a node that still has relationships.
     * @throws CreateConstraintFailureException if this transaction was set to create a constraint and that failed.
     * @throws ConstraintValidationException if this transaction was set to create a constraint and some data violates that constraint.
     */
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
            throws KernelException
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
        try ( IndexActivator indexActivator = new IndexActivator( indexUpdateListener );
              LockGroup locks = new LockGroup();
              BatchTransactionApplier batchApplier = applier( mode, indexActivator ) )
        {
            while ( batch != null )
            {
                try ( TransactionApplier txApplier = batchApplier.startTx( batch, locks ) )
                {
                    batch.accept( txApplier );
                }
                batch = batch.next();
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
    protected BatchTransactionApplierFacade applier( TransactionApplicationMode mode, IndexActivator indexActivator )
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
            appliers.add( new IndexBatchTransactionApplier( indexUpdateListener, labelScanStoreSync, indexUpdatesSync,
                    neoStores.getNodeStore(), neoStores.getRelationshipStore(),
                    neoStores.getPropertyStore(), this, schemaCache, indexActivator ) );
        }

        // Perform the application
        return new BatchTransactionApplierFacade(
                appliers.toArray( new BatchTransactionApplier[0] ) );
    }

    private LockService lockService( TransactionApplicationMode mode )
    {
        return mode == RECOVERY || mode == REVERSE_RECOVERY ? NO_LOCK_SERVICE : lockService;
    }

    @Override
    public void init() throws Exception
    {
        countsStore.init();
    }

    @Override
    public void start() throws Exception
    {
        neoStores.makeStoreOk();
        countsStore.start();
        idController.start();
    }

    @VisibleForTesting
    public void loadSchemaCache()
    {
        schemaCache.load( schemaRuleAccess.getAll() );
    }

    @Override
    public void stop() throws Exception
    {
        executeAll( idController::stop, countsStore::stop );
    }

    @Override
    public void shutdown() throws Exception
    {
        executeAll( countsStore::shutdown, neoStores::close );
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        countsStore.rotate( neoStores.getMetaDataStore().getLastCommittedTransactionId() );
        neoStores.flush( limiter );
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

    @VisibleForTesting
    public SchemaRuleAccess testAccessSchemaRules()
    {
        return schemaRuleAccess;
    }

    @Override
    public StoreId getStoreId()
    {
        return neoStores.getMetaDataStore().getStoreId();
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle()
    {
        return new LifecycleAdapter()
        {
            @Override
            public void init()
            {
                tokenHolders.setInitialTokens( StoreTokens.allTokens( neoStores ) );
                loadSchemaCache();
            }
        };
    }

    public TransactionIdStore transactionIdStore()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public LogVersionRepository logVersionRepository()
    {
        return neoStores.getMetaDataStore();
    }
}
