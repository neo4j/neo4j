/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.heuristics.StatisticsData;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.statistics.StatisticsService;
import org.neo4j.kernel.impl.api.statistics.StatisticsServiceRepository;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.IntegrityValidator;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransactionContextSupplier;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.xaframework.LogFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogPositionCache;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.LogRotationControl;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * This is the beginnings of an implementation of the Kernel API, which is meant to be an internal API for
 * consumption by both the Core API, Cypher, and any other components that want to interface with the
 * underlying database.
 *
 * This is currently in an intermediate phase, with many features still unavailable unless the Core API is also
 * present. We are in the process of moving Core API features into the kernel.
 *
 * <h1>Structure</h1>
 *
 * The Kernel itself has a simple API - it lets you start transactions. The transactions, in turn, allow you to
 * create statements, which, in turn, operate against the database. The reason for the separation between statements
 * and transactions is database isolation. Please refer to the {@link KernelTransaction} javadoc for details.
 *
 * The architecture of the kernel is based around a layered design, where one layer performs some task, and potentially
 * delegates down to a lower layer. For instance, writing to the database will pass through
 * {@link LockingStatementOperations}, which will grab locks and delegate to {@link StateHandlingStatementOperations}
 * which will store the change in the transaction state, to be applied later if the transaction is committed.
 *
 * A read will, similarly, pass through {@link LockingStatementOperations}, which should (but does not currently) grab
 * read locks. It then reaches {@link StateHandlingStatementOperations}, which includes any changes that exist in the
 * current transaction, and then finally {@link org.neo4j.kernel.impl.api.store.DiskLayer} will read the current committed state from
 * the stores or caches.
 *
 * <h1>A story of JTA</h1>
 *
 * We have, for a long time, supported full X.Open two-phase commits, which is handled by our TxManager implementation
 * of the JTA interfaces. However, two phase commit is slow and complex. Ideally we don't want every day use of neo4j
 * to require JTA anymore, but rather have it be a bonus feature that can be enabled when the user wants two-phase
 * commit support. As such, we are working to keep the Kernel compatible with a JTA system built on top of it, but
 * at the same time it should remain independent and runnable without a transaction manager.
 *
 * The heart of this work is in the relationship between {@link KernelTransaction} and
 * {@link org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState}. The latter should become a wrapper around
 * KernelTransactions, exposing them as JTA-capable transactions. The Write transaction should be hidden from the outside,
 * an implementation detail living inside the kernel.
 *
 * <h1>Refactoring</h1>
 *
 * There are several sources of pain around the current state, which we hope to refactor away down the line.
 *
 * One pain is transaction state, where lots of legacy code still rules supreme. Please refer to {@link TxState}
 * for details about the work in this area.
 *
 * Cache invalidation is similarly problematic, where cache invalidation really should be done when changes are applied
 * to the store, through the logical log. However, this is mostly not the case, cache invalidation is done as we work
 * through the Core API. Only in HA mode is cache invalidation done through log application, and then only through
 * evicting whole entities from the cache whenever they change, leading to large performance hits on writes. This area
 * is still open for investigation, but an approach where the logical log simply tells a store write API to apply some
 * change, and the implementation of that API is responsible for keeping caches in sync.
 *
 * Please expand and update this as you learn things or find errors in the text above.
 */
public class Kernel extends LifecycleAdapter implements KernelAPI
{
    private final UpdateableSchemaState schemaState;
    private final SchemaWriteGuard schemaWriteGuard;
    private final IndexingService indexService;
    private final NeoStore neoStore;
    private final SchemaCache schemaCache;
    private final SchemaIndexProviderMap providerMap;
    private final LabelScanStore labelScanStore;
    private final StatementOperationParts statementOperations;
    private final StoreReadLayer storeLayer;
    private final TransactionHooks hooks = new TransactionHooks();
    private final LegacyPropertyTrackers legacyPropertyTrackers;
    private final StatisticsService statisticsService;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final JobScheduler scheduler;
    private final TransactionMonitor transactionMonitor;
    private final boolean readOnly;
    private boolean isShutdown = false;
    private final IntegrityValidator integrityValidator;
    private final Locks locks;
    private final NodeManager nodeManager;
    private final NeoStoreTransactionContextSupplier neoStoreTransactionContextSupplier;
    private final TxIdGenerator txIdGenerator;
    private final TransactionRepresentationCommitProcess commitProcess;
    private final TransactionHeaderInformation transactionHeaderInformation;
    private final TransactionStore transactionStore;
    private final StartupStatisticsProvider startupStatistics;
    private final PersistenceCache persistenceCache;

    public Kernel( PropertyKeyTokenHolder propertyKeyTokenHolder, UpdateableSchemaState schemaState,
            SchemaWriteGuard schemaWriteGuard, IndexingService indexService, NodeManager nodeManager,
            Provider<NeoStore> neoStoreProvider, PersistenceCache persistenceCache, SchemaCache schemaCache,
            SchemaIndexProviderMap providerMap, FileSystemAbstraction fs, Config config, LabelScanStore labelScanStore,
            StoreReadLayer storeLayer, JobScheduler scheduler, TransactionMonitor transactionMonitor,
            KernelHealth kernelHealth, boolean readOnly, CacheAccessBackDoor cacheAccess,
            IntegrityValidator integrityValidator, Locks locks, LockService lockService, TxIdGenerator txIdGenerator,
            TransactionHeaderInformation transactionHeaderInformation, LogRotationControl logRotationControl,
            StartupStatisticsProvider startupStatistics, Logging logging )
    {
        this.nodeManager = nodeManager;
        this.persistenceCache = persistenceCache;
        this.fs = fs;
        this.config = config;
        this.schemaState = schemaState;
        this.providerMap = providerMap;
        this.transactionMonitor = transactionMonitor;
        this.readOnly = readOnly;
        this.schemaWriteGuard = schemaWriteGuard;
        this.indexService = indexService;
        this.integrityValidator = integrityValidator;
        this.locks = locks;
        this.txIdGenerator = txIdGenerator;
        this.transactionHeaderInformation = transactionHeaderInformation;
        this.startupStatistics = startupStatistics;
        this.neoStore = neoStoreProvider.instance();
        this.schemaCache = schemaCache;
        this.labelScanStore = labelScanStore;
        this.scheduler = scheduler;
        this.legacyPropertyTrackers = new LegacyPropertyTrackers( propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(), nodeManager.getRelationshipPropertyTrackers(), nodeManager );
        this.storeLayer = storeLayer;
        this.statementOperations = buildStatementOperations();
        this.statisticsService = new StatisticsServiceRepository( fs, config, storeLayer, scheduler ).loadStatistics();
        this.neoStoreTransactionContextSupplier = new NeoStoreTransactionContextSupplier( neoStore );
        this.transactionStore = createTransactionStore( logRotationControl, logging );
        this.commitProcess = new TransactionRepresentationCommitProcess( transactionStore, kernelHealth, indexService,
                labelScanStore, neoStore, cacheAccess, lockService, false );
    }

    private TransactionStore createTransactionStore( LogRotationControl logRotationControl, Logging logging )
    {
        File directory = config.get( GraphDatabaseSettings.store_dir );
        LogPositionCache logPositionCache = new LogPositionCache( 1000, 100_000 );
        LogFile logFile = new PhysicalLogFile( fs, directory, PhysicalLogFile.DEFAULT_NAME,
                config.get( GraphDatabaseSettings.logical_log_rotation_threshold ), LogPruneStrategies.fromConfigValue(
                        fs, null, null, null, config.get( GraphDatabaseSettings.keep_logical_logs ) ), neoStore,
                neoStore, new PhysicalLogFile.LoggingMonitor( logging.getMessagesLog( getClass() ) ),
                logRotationControl, logPositionCache );
        return new PhysicalTransactionStore( logFile, txIdGenerator, logPositionCache, new VersionAwareLogEntryReader(
                XaCommandReaderFactory.DEFAULT ) );
    }

    @Override
    public void init() throws Throwable
    {
        final AtomicInteger recoveredCount = new AtomicInteger();
        transactionStore.open( new Visitor<TransactionRepresentation, IOException>()
        {
            @Override
            public boolean visit( TransactionRepresentation transaction ) throws IOException
            {
                try
                {
                    commitProcess.commit( transaction );
                    recoveredCount.incrementAndGet();
                    return true;
                }
                catch ( TransactionFailureException e )
                {
                    throw new IOException( "Unable to recover transaction " + transaction, e );
                }
            }
        } );
        startupStatistics.setNumberOfRecoveredTransactions( recoveredCount.get() );
        indexService.startIndexes();
    }

    @Override
    public void start() throws Throwable
    {
        for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAllSchemaRules() ) )
        {
            schemaCache.addSchemaRule( schemaRule );
        }
        statisticsService.start();
    }

    @Override
    public void stop() throws Throwable
    {
        isShutdown = true;
        new StatisticsServiceRepository( fs, config, storeLayer, scheduler );
        statisticsService.stop();
    }

    @Override
    public KernelTransaction newTransaction()
    {
        checkIfShutdown();
        NeoStoreTransactionContext context = neoStoreTransactionContextSupplier.acquire();
        Locks.Client locksClient = locks.newClient();
        context.bind( locksClient );
        TransactionRecordState neoStoreTransaction = new TransactionRecordState(
                neoStore.getLastCommittingTransactionId(), neoStore, integrityValidator, context );
        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( this, indexService );
        return new KernelTransactionImplementation( statementOperations, readOnly, schemaWriteGuard, labelScanStore,
                indexService, schemaState, neoStoreTransaction, providerMap, neoStore, locksClient, hooks,
                constraintIndexCreator, transactionHeaderInformation, commitProcess, transactionMonitor, neoStore,
                persistenceCache, storeLayer );
    }

    @Override
    public void registerTransactionHook( TransactionHook hook )
    {
        hooks.register( hook );
    }

    @Override
    public void unregisterTransactionHook( TransactionHook hook )
    {
        hooks.unregister( hook );
    }

    @Override
    public StatisticsData heuristics()
    {
        return statisticsService.statistics();
    }

    private void checkIfShutdown()
    {
        if ( isShutdown )
        {
            throw new DatabaseShutdownException();
        }
    }

    private StatementOperationParts buildStatementOperations()
    {
        // Bottom layer: Read-access to committed data
        StoreReadLayer storeLayer = this.storeLayer;
        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations( storeLayer,
                legacyPropertyTrackers, new ConstraintIndexCreator( this, indexService ) );
        StatementOperationParts parts = new StatementOperationParts( stateHandlingContext, stateHandlingContext,
                stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( schemaState ), null );
        // + Constraints
        ConstraintEnforcingEntityOperations constraintEnforcingEntityOperations = new ConstraintEnforcingEntityOperations(
                parts.entityWriteOperations(), parts.entityReadOperations(), parts.schemaReadOperations() );
        // + Data integrity
        DataIntegrityValidatingStatementOperations dataIntegrityContext = new DataIntegrityValidatingStatementOperations(
                parts.keyWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations() );
        parts = parts.override( null, dataIntegrityContext, constraintEnforcingEntityOperations,
                constraintEnforcingEntityOperations, null, dataIntegrityContext, null, null );
        // + Locking
        LockingStatementOperations lockingContext = new LockingStatementOperations( parts.entityReadOperations(),
                parts.entityWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations(),
                parts.schemaStateOperations() );
        parts = parts.override( null, null, null, lockingContext, lockingContext, lockingContext, lockingContext,
                lockingContext );
        return parts;
    }
}
