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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.DatabaseKernelExtensions;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactionMonitorScheduler;
import org.neo4j.kernel.impl.api.KernelTransactionTimeoutMonitor;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StackingQueryRegistrationOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.monitoring.LogProgressReporter;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.Lifecycles;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.kernel.recovery.CorruptedLogsTruncator;
import org.neo4j.kernel.recovery.DefaultRecoveryService;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.recovery.RecoveryMonitor;
import org.neo4j.kernel.recovery.RecoveryService;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.helpers.Exceptions.throwIfUnchecked;

public class NeoStoreDataSource extends LifecycleAdapter
{

    enum Diagnostics implements DiagnosticsExtractor<NeoStoreDataSource>
    {
        TRANSACTION_RANGE( "Transaction log:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger log )
                    {
                        LogFiles logFiles = source.getDependencyResolver().resolveDependency( LogFiles.class );
                        try
                        {
                            for ( long logVersion = logFiles.getLowestLogVersion();
                                  logFiles.versionExists( logVersion ); logVersion++ )
                            {
                                if ( logFiles.hasAnyEntries( logVersion ) )
                                {
                                    LogHeader header = logFiles.extractHeader( logVersion );
                                    long firstTransactionIdInThisLog = header.lastCommittedTxId + 1;
                                    log.log( "Oldest transaction " + firstTransactionIdInThisLog +
                                             " found in log with version " + logVersion );
                                    return;
                                }
                            }
                            log.log( "No transactions found in any log" );
                        }
                        catch ( IOException e )
                        {   // It's fine, we just tried to be nice and log this. Failing is OK
                            log.log( "Error trying to figure out oldest transaction in log" );
                        }
                    }
                };

        private final String message;

        Diagnostics( String message )
        {
            this.message = message;
        }

        @Override
        public void dumpDiagnostics( final NeoStoreDataSource source, DiagnosticsPhase phase, Logger logger )
        {
            if ( applicable( phase ) )
            {
                logger.log( message );
                dump( source, logger );
            }
        }

        boolean applicable( DiagnosticsPhase phase )
        {
            return phase.isInitialization() || phase.isExplicitlyRequested();
        }

        abstract void dump( NeoStoreDataSource source, Logger logger );
    }

    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    private final Monitors monitors;
    private final Tracers tracers;

    private final Log msgLog;
    private final LogService logService;
    private final AutoIndexing autoIndexing;
    private final LogProvider logProvider;
    private final DependencyResolver dependencyResolver;
    private final TokenNameLookup tokenNameLookup;
    private final TokenHolders tokenHolders;
    private final StatementLocksFactory statementLocksFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final IdGeneratorFactory idGeneratorFactory;
    private final JobScheduler scheduler;
    private final Config config;
    private final LockService lockService;
    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private final TransactionMonitor transactionMonitor;
    private final DatabaseHealth databaseHealth;
    private final LogFileCreationMonitor physicalLogMonitor;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final ConstraintSemantics constraintSemantics;
    private final Procedures procedures;
    private final IOLimiter ioLimiter;
    private final AvailabilityGuard availabilityGuard;
    private final SystemNanoClock clock;
    private final IndexConfigStore indexConfigStore;
    private final ExplicitIndexProvider explicitIndexProvider;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;

    private Dependencies dataSourceDependencies;
    private LifeSupport life;
    private IndexProviderMap indexProviderMap;
    private File databaseDirectory;
    private boolean readOnly;
    private final IdController idController;
    private final DatabaseInfo databaseInfo;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final VersionContextSupplier versionContextSupplier;
    private final AccessCapability accessCapability;

    private StorageEngine storageEngine;
    private QueryExecutionEngine executionEngine;
    private NeoStoreTransactionLogModule transactionLogModule;
    private NeoStoreKernelModule kernelModule;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensionFactories;
    private final Function<File,FileSystemWatcherService> watcherServiceFactory;
    private final GraphDatabaseFacade facade;
    private final Iterable<QueryEngineProvider> engineProviders;
    private final boolean failOnCorruptedLogFiles;

    public NeoStoreDataSource( File databaseDirectory, Config config, IdGeneratorFactory idGeneratorFactory, LogService logService, JobScheduler scheduler,
            TokenNameLookup tokenNameLookup, DependencyResolver dependencyResolver, TokenHolders tokenHolders, StatementLocksFactory statementLocksFactory,
            SchemaWriteGuard schemaWriteGuard, TransactionEventHandlers transactionEventHandlers, IndexingService.Monitor indexingServiceMonitor,
            FileSystemAbstraction fs, TransactionMonitor transactionMonitor, DatabaseHealth databaseHealth, LogFileCreationMonitor physicalLogMonitor,
            TransactionHeaderInformationFactory transactionHeaderInformationFactory, CommitProcessFactory commitProcessFactory, AutoIndexing autoIndexing,
            IndexConfigStore indexConfigStore, ExplicitIndexProvider explicitIndexProvider, PageCache pageCache, ConstraintSemantics constraintSemantics,
            Monitors monitors, Tracers tracers, Procedures procedures, IOLimiter ioLimiter, AvailabilityGuard availabilityGuard, SystemNanoClock clock,
            AccessCapability accessCapability, StoreCopyCheckPointMutex storeCopyCheckPointMutex, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IdController idController, DatabaseInfo databaseInfo, VersionContextSupplier versionContextSupplier,
            CollectionsFactorySupplier collectionsFactorySupplier, Iterable<KernelExtensionFactory<?>> kernelExtensionFactories,
            Function<File,FileSystemWatcherService> watcherServiceFactory, GraphDatabaseFacade facade, Iterable<QueryEngineProvider> engineProviders )
    {
        this.databaseDirectory = databaseDirectory;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenNameLookup = tokenNameLookup;
        this.dependencyResolver = dependencyResolver;
        this.scheduler = scheduler;
        this.logService = logService;
        this.autoIndexing = autoIndexing;
        this.indexConfigStore = indexConfigStore;
        this.explicitIndexProvider = explicitIndexProvider;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.logProvider = logService.getInternalLogProvider();
        this.tokenHolders = tokenHolders;
        this.statementLocksFactory = statementLocksFactory;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionEventHandlers = transactionEventHandlers;
        this.indexingServiceMonitor = indexingServiceMonitor;
        this.fs = fs;
        this.transactionMonitor = transactionMonitor;
        this.databaseHealth = databaseHealth;
        this.physicalLogMonitor = physicalLogMonitor;
        this.transactionHeaderInformationFactory = transactionHeaderInformationFactory;
        this.constraintSemantics = constraintSemantics;
        this.monitors = monitors;
        this.tracers = tracers;
        this.procedures = procedures;
        this.ioLimiter = ioLimiter;
        this.availabilityGuard = availabilityGuard;
        this.clock = clock;
        this.accessCapability = accessCapability;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;

        readOnly = config.get( GraphDatabaseSettings.read_only );
        this.idController = idController;
        this.databaseInfo = databaseInfo;
        this.versionContextSupplier = versionContextSupplier;
        this.kernelExtensionFactories = kernelExtensionFactories;
        this.watcherServiceFactory = watcherServiceFactory;
        this.facade = facade;
        this.engineProviders = engineProviders;
        this.msgLog = logProvider.getLog( getClass() );
        this.lockService = new ReentrantLockService();
        this.commitProcessFactory = commitProcessFactory;
        this.pageCache = pageCache;
        this.monitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
        this.collectionsFactorySupplier = collectionsFactorySupplier;
        this.failOnCorruptedLogFiles = config.get( GraphDatabaseSettings.fail_on_corrupted_log_files );
    }

    // We do our own internal life management:
    // start() does life.init() and life.start(),
    // stop() does life.stop() and life.shutdown().
    @Override
    public void start() throws IOException
    {
        dataSourceDependencies = new Dependencies( dependencyResolver );
        dataSourceDependencies.satisfyDependency( monitors );
        dataSourceDependencies.satisfyDependency( tokenHolders );
        dataSourceDependencies.satisfyDependency( facade );

        life = new LifeSupport();
        life.add( initializeExtensions( dataSourceDependencies ) );
        life.add( recoveryCleanupWorkCollector );
        dataSourceDependencies.satisfyDependency( lockService );
        life.add( indexConfigStore );

        FileSystemWatcherService watcherService = watcherServiceFactory.apply( databaseDirectory );
        life.add( watcherService );
        dataSourceDependencies.satisfyDependency( watcherService );

        life.add( Lifecycles.multiple( explicitIndexProvider.allIndexProviders() ) );

        // Check the tail of transaction logs and validate version
        final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();

        LogFiles logFiles = LogFilesBuilder.builder( databaseDirectory, fs )
                .withLogEntryReader( logEntryReader )
                .withLogFileMonitor( physicalLogMonitor )
                .withConfig( config )
                .withDependencies( dataSourceDependencies ).build();

        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, monitors, failOnCorruptedLogFiles );
        monitors.addMonitorListener(
                new LoggingLogTailScannerMonitor( logService.getInternalLog( LogTailScanner.class ) ) );
        monitors.addMonitorListener( new ReverseTransactionCursorLoggingMonitor(
                logService.getInternalLog( ReversedSingleFileTransactionCursor.class ) ) );
        LogVersionUpgradeChecker.check( tailScanner, config );

        // Upgrade the store before we begin
        RecordFormats formats = selectStoreFormats( config, databaseDirectory, fs, pageCache, logService );
        upgradeStore( formats, tailScanner );

        // Build all modules and their services
        StorageEngine storageEngine = null;
        try
        {
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( logProvider );

            SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering = new SynchronizedArrayIdOrderingQueue( 20 );

            Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier = () -> kernelModule.kernelTransactions().get();
            idController.initialize( transactionsSnapshotSupplier );

            storageEngine = buildStorageEngine( explicitIndexProvider,
                    indexConfigStore, databaseSchemaState, explicitIndexTransactionOrdering, databaseInfo.operationalMode,
                    versionContextSupplier );
            life.add( logFiles );

            TransactionIdStore transactionIdStore = dataSourceDependencies.resolveDependency( TransactionIdStore.class );

            versionContextSupplier.init( transactionIdStore::getLastClosedTransactionId );

            LogVersionRepository logVersionRepository = dataSourceDependencies.resolveDependency( LogVersionRepository.class );
            NeoStoreTransactionLogModule transactionLogModule = buildTransactionLogs( logFiles, config, logProvider,
                    scheduler, storageEngine, logEntryReader, explicitIndexTransactionOrdering, transactionIdStore );
            transactionLogModule.satisfyDependencies( dataSourceDependencies );

            buildRecovery( fs,
                    transactionIdStore,
                    tailScanner,
                    monitors.newMonitor( RecoveryMonitor.class ),
                    monitors.newMonitor( RecoveryStartInformationProvider.Monitor.class ),
                    logFiles,
                    storageEngine, transactionLogModule.logicalTransactionStore(), logVersionRepository
            );

            // At the time of writing this comes from the storage engine (IndexStoreView)
            NodePropertyAccessor nodePropertyAccessor = dataSourceDependencies.resolveDependency( NodePropertyAccessor.class );

            final NeoStoreKernelModule kernelModule = buildKernel(
                    logFiles,
                    transactionLogModule.transactionAppender(),
                    dataSourceDependencies.resolveDependency( IndexingService.class ),
                    databaseSchemaState,
                    dataSourceDependencies.resolveDependency( LabelScanStore.class ),
                    storageEngine,
                    indexConfigStore,
                    transactionIdStore,
                    availabilityGuard,
                    clock, nodePropertyAccessor );

            kernelModule.satisfyDependencies( dataSourceDependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.storageEngine = storageEngine;
            this.transactionLogModule = transactionLogModule;
            this.kernelModule = kernelModule;

            dataSourceDependencies.satisfyDependency( this );
            dataSourceDependencies.satisfyDependency( databaseSchemaState );
            dataSourceDependencies.satisfyDependency( logEntryReader );
            dataSourceDependencies.satisfyDependency( storageEngine );
            dataSourceDependencies.satisfyDependency( explicitIndexProvider );

            executionEngine = QueryEngineProvider.initialize( dataSourceDependencies, facade, engineProviders );
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while setting up store modules. Attempting to close things down.", e );
            try
            {
                // Close the neostore, so that locks are released properly
                if ( storageEngine != null )
                {
                    storageEngine.forceClose();
                }
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close neostore after startup failure", closeException );
            }
            throwIfUnchecked( e );
            throw new RuntimeException( e );
        }

        life.add( new DatabaseDiagnostics( dataSourceDependencies.resolveDependency( DiagnosticsManager.class ), this, databaseInfo ) );
        life.setLast( lifecycleToTriggerCheckPointOnShutdown() );

        try
        {
            life.start();
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while starting the datasource. Attempting to close things down.", e );
            try
            {
                life.shutdown();
                // Close the neostore, so that locks are released properly
                storageEngine.forceClose();
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close neostore after startup failure", closeException );
            }
            throw new RuntimeException( e );
        }
        /*
         * At this point recovery has completed and the datasource is ready for use. Whatever panic might have
         * happened before has been healed. So we can safely set the kernel health to ok.
         * This right now has any real effect only in the case of internal restarts (for example, after a store copy
         * in the case of HA). Standalone instances will have to be restarted by the user, as is proper for all
         * kernel panics.
         */
        databaseHealth.healed();
    }

    private LifeSupport initializeExtensions( Dependencies dependencies )
    {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add( new DatabaseKernelExtensions( new SimpleKernelContext( databaseDirectory, databaseInfo, dependencies ), kernelExtensionFactories,
                dependencies, UnsatisfiedDependencyStrategies.fail() ) );

        indexProviderMap = extensionsLife.add( new DefaultIndexProviderMap( dependencies ) );
        dependencies.satisfyDependency( indexProviderMap );
        extensionsLife.init();
        return extensionsLife;
    }

    private static RecordFormats selectStoreFormats( Config config, File databaseDirectory, FileSystemAbstraction fs, PageCache pageCache,
            LogService logService )
    {
        LogProvider logging = logService.getInternalLogProvider();
        RecordFormats formats = RecordFormatSelector.selectNewestFormat( config, databaseDirectory, fs, pageCache, logging );
        new RecordFormatPropertyConfigurator( formats, config ).configure();
        return formats;
    }

    private void upgradeStore( RecordFormats format, LogTailScanner tailScanner )
    {
        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( logService.getUserLog( StoreMigrator.class ) );
        new DatabaseMigrator(
                progressMonitor,
                fs,
                config,
                logService,
                indexProviderMap,
                explicitIndexProvider,
                pageCache,
                format, tailScanner ).migrate( databaseDirectory );
    }

    private StorageEngine buildStorageEngine(
            ExplicitIndexProvider explicitIndexProviderLookup, IndexConfigStore indexConfigStore,
            SchemaState schemaState, SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering,
            OperationalMode operationalMode, VersionContextSupplier versionContextSupplier )
    {
        RecordStorageEngine storageEngine =
                new RecordStorageEngine( databaseDirectory, config, pageCache, fs, logProvider, tokenHolders,
                        schemaState, constraintSemantics, scheduler,
                        tokenNameLookup, lockService, indexProviderMap, indexingServiceMonitor, databaseHealth,
                        explicitIndexProviderLookup, indexConfigStore,
                        explicitIndexTransactionOrdering, idGeneratorFactory, idController, monitors,
                        recoveryCleanupWorkCollector,
                        operationalMode, versionContextSupplier );

        // We pretend that the storage engine abstract hides all details within it. Whereas that's mostly
        // true it's not entirely true for the time being. As long as we need this call below, which
        // makes available one or more internal things to the outside world, there are leaks to plug.
        storageEngine.satisfyDependencies( dataSourceDependencies );

        return life.add( storageEngine );
    }

    private NeoStoreTransactionLogModule buildTransactionLogs( LogFiles logFiles, Config config,
            LogProvider logProvider, JobScheduler scheduler, StorageEngine storageEngine,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader,
            SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering, TransactionIdStore transactionIdStore )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 100_000 );
        if ( config.get( GraphDatabaseSettings.ephemeral ) )
        {
            config.augmentDefaults( GraphDatabaseSettings.keep_logical_logs, "1 files" );
        }

        final LogPruning logPruning =
                new LogPruningImpl( fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config );

        final LogRotation logRotation =
                new LogRotationImpl( monitors.newMonitor( LogRotation.Monitor.class ), logFiles, databaseHealth );

        final TransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFiles, logRotation, transactionMetadataCache, transactionIdStore, explicitIndexTransactionOrdering,
                databaseHealth ) );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader, monitors,
                        failOnCorruptedLogFiles );

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );

        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                transactionIdStore, threshold, storageEngine, logPruning, appender, databaseHealth, logProvider,
                tracers.checkPointTracer, ioLimiter, storeCopyCheckPointMutex );

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioLimiter, scheduler,
                recurringPeriod, databaseHealth );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new NeoStoreTransactionLogModule( logicalTransactionStore, logFiles,
                logRotation, checkPointer, appender, explicitIndexTransactionOrdering );
    }

    private void buildRecovery(
            final FileSystemAbstraction fileSystemAbstraction,
            TransactionIdStore transactionIdStore,
            LogTailScanner tailScanner,
            RecoveryMonitor recoveryMonitor,
            RecoveryStartInformationProvider.Monitor positionMonitor,
            final LogFiles logFiles,
            StorageEngine storageEngine,
            LogicalTransactionStore logicalTransactionStore,
            LogVersionRepository logVersionRepository )
    {
        RecoveryService recoveryService = new DefaultRecoveryService( storageEngine, tailScanner, transactionIdStore,
                logicalTransactionStore, logVersionRepository, positionMonitor );
        CorruptedLogsTruncator logsTruncator = new CorruptedLogsTruncator( databaseDirectory, logFiles, fileSystemAbstraction );
        ProgressReporter progressReporter = new LogProgressReporter( logService.getInternalLog( Recovery.class ) );
        Recovery recovery = new Recovery( recoveryService, logsTruncator, recoveryMonitor, progressReporter, failOnCorruptedLogFiles );
        life.add( recovery );
    }

    private NeoStoreKernelModule buildKernel( LogFiles logFiles, TransactionAppender appender,
            IndexingService indexingService, DatabaseSchemaState databaseSchemaState, LabelScanStore labelScanStore,
            StorageEngine storageEngine, IndexConfigStore indexConfigStore, TransactionIdStore transactionIdStore,
            AvailabilityGuard availabilityGuard, SystemNanoClock clock, NodePropertyAccessor nodePropertyAccessor )
    {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();
        AtomicReference<HeapAllocation> heapAllocationRef = setupHeapAllocationAtomicReference();

        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine,
                config );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService, nodePropertyAccessor, logProvider );

        ExplicitIndexStore explicitIndexStore = new ExplicitIndexStore( config,
                indexConfigStore, kernelProvider, explicitIndexProvider );

        StatementOperationParts statementOperationParts = dataSourceDependencies.satisfyDependency(
                buildStatementOperations( cpuClockRef, heapAllocationRef ) );

        TransactionHooks hooks = new TransactionHooks();

        KernelTransactions kernelTransactions = life.add( new KernelTransactions( statementLocksFactory,
                constraintIndexCreator, statementOperationParts, schemaWriteGuard, transactionHeaderInformationFactory,
                transactionCommitProcess, indexConfigStore, explicitIndexProvider, hooks, transactionMonitor,
                availabilityGuard, tracers, storageEngine, procedures, transactionIdStore, clock,
                cpuClockRef, heapAllocationRef, accessCapability, autoIndexing,
                explicitIndexStore, versionContextSupplier, collectionsFactorySupplier, constraintSemantics,
                databaseSchemaState, indexingService, tokenHolders, dataSourceDependencies ) );

        buildTransactionMonitor( kernelTransactions, clock, config );

        final KernelImpl kernel = new KernelImpl( kernelTransactions, hooks, databaseHealth, transactionMonitor, procedures,
                config );

        kernel.registerTransactionHook( transactionEventHandlers );
        life.add( kernel );

        final NeoStoreFileListing fileListing = new NeoStoreFileListing( databaseDirectory, logFiles, labelScanStore,
                indexingService, explicitIndexProvider, storageEngine );
        dataSourceDependencies.satisfyDependency( fileListing );

        return new NeoStoreKernelModule( transactionCommitProcess, kernel, kernelTransactions, fileListing );
    }

    private AtomicReference<CpuClock> setupCpuClockAtomicReference()
    {
        AtomicReference<CpuClock> cpuClock = new AtomicReference<>( CpuClock.NOT_AVAILABLE );
        BiConsumer<Boolean,Boolean> cpuClockUpdater = ( before, after ) ->
        {
            if ( after )
            {
                cpuClock.set( CpuClock.CPU_CLOCK );
            }
            else
            {
                cpuClock.set( CpuClock.NOT_AVAILABLE );
            }
        };
        cpuClockUpdater.accept( null, config.get( GraphDatabaseSettings.track_query_cpu_time ) );
        config.registerDynamicUpdateListener( GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater );
        return cpuClock;
    }

    private AtomicReference<HeapAllocation> setupHeapAllocationAtomicReference()
    {
        AtomicReference<HeapAllocation> heapAllocation = new AtomicReference<>( HeapAllocation.NOT_AVAILABLE );
        BiConsumer<Boolean,Boolean> heapAllocationUpdater = ( before, after ) ->
        {
            if ( after )
            {
                heapAllocation.set( HeapAllocation.HEAP_ALLOCATION );
            }
            else
            {
                heapAllocation.set( HeapAllocation.NOT_AVAILABLE );
            }
        };
        heapAllocationUpdater.accept( null, config.get( GraphDatabaseSettings.track_query_allocation ) );
        config.registerDynamicUpdateListener( GraphDatabaseSettings.track_query_allocation, heapAllocationUpdater );
        return heapAllocation;
    }

    private void buildTransactionMonitor( KernelTransactions kernelTransactions, Clock clock, Config config )
    {
        KernelTransactionTimeoutMonitor kernelTransactionTimeoutMonitor =
                new KernelTransactionTimeoutMonitor( kernelTransactions, clock, logService );
        dataSourceDependencies.satisfyDependency( kernelTransactionTimeoutMonitor );
        KernelTransactionMonitorScheduler transactionMonitorScheduler =
                new KernelTransactionMonitorScheduler( kernelTransactionTimeoutMonitor, scheduler,
                        config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis() );
        life.add( transactionMonitorScheduler );
    }

    @Override
    public synchronized void stop()
    {
        if ( !life.isRunning() )
        {
            return;
        }

        life.stop();
        awaitAllClosingTransactions();
        // Checkpointing is now triggered as part of life.shutdown see lifecycleToTriggerCheckPointOnShutdown()
        // Shut down all services in here, effectively making the database unusable for anyone who tries.
        life.shutdown();
    }

    private void awaitAllClosingTransactions()
    {
        KernelTransactions kernelTransactions = kernelModule.kernelTransactions();
        kernelTransactions.terminateTransactions();

        while ( kernelTransactions.haveClosingTransaction() )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }

    private Lifecycle lifecycleToTriggerCheckPointOnShutdown()
    {
        // Write new checkpoint in the log only if the kernel is healthy.
        // We cannot throw here since we need to shutdown without exceptions,
        // so let's make the checkpointing part of the life, so LifeSupport can handle exceptions properly
        return new LifecycleAdapter()
        {
            @Override
            public void shutdown() throws IOException
            {
                if ( databaseHealth.isHealthy() )
                {
                    // Flushing of neo stores happens as part of the checkpoint
                    transactionLogModule.checkPointing()
                            .forceCheckPoint( new SimpleTriggerInfo( "database shutdown" ) );
                }
            }
        };
    }

    public StoreId getStoreId()
    {
        return storageEngine.getStoreId();
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public QueryExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    public InwardKernel getKernel()
    {
        return kernelModule.kernelAPI();
    }

    public ResourceIterator<StoreFileMetadata> listStoreFiles( boolean includeLogs ) throws IOException
    {
        NeoStoreFileListing.StoreFileListingBuilder fileListingBuilder = getNeoStoreFileListing().builder();
        if ( !includeLogs )
        {
            fileListingBuilder.excludeLogFiles();
        }
        return fileListingBuilder.build();
    }

    public NeoStoreFileListing getNeoStoreFileListing()
    {
        return kernelModule.fileListing();
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        storageEngine.registerDiagnostics( manager );
        manager.registerAll( Diagnostics.class, this );
    }

    public DependencyResolver getDependencyResolver()
    {
        return dataSourceDependencies;
    }

    private StatementOperationParts buildStatementOperations( AtomicReference<CpuClock> cpuClockRef,
            AtomicReference<HeapAllocation> heapAllocationRef )
    {
        QueryRegistrationOperations queryRegistrationOperations =
                new StackingQueryRegistrationOperations( clock, cpuClockRef, heapAllocationRef );

        return new StatementOperationParts( queryRegistrationOperations );
    }

    /**
     * Hook that must be called before there is an HA mode switch (eg master/slave switch),
     * i.e. after state has changed to pending and before state is about to change to the new target state.
     * This must only be called when the database is otherwise inaccessible.
     */
    public void beforeModeSwitch()
    {
        clearTransactions();
    }

    private void clearTransactions()
    {
        // We don't want to have buffered ids carry over to the new role
        storageEngine.clearBufferedIds();

        // Get rid of all pooled transactions, as they will otherwise reference
        // components that have been swapped out during the mode switch.
        kernelModule.kernelTransactions().disposeAll();
    }

    /**
     * Hook that must be called after an HA mode switch (eg master/slave switch) have completed.
     * This must only be called when the database is otherwise inaccessible.
     */
    public void afterModeSwitch()
    {
        storageEngine.loadSchemaCache();
        clearTransactions();
    }

    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    // For test purposes only, not thread safe
    @VisibleForTesting
    public LifeSupport getLife()
    {
        return life;
    }
}
