/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.DataIntegrityValidatingStatementOperations;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.ExplicitIndexProviderLookup;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelTransactionMonitorScheduler;
import org.neo4j.kernel.impl.api.KernelTransactionTimeoutMonitor;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.api.LockingStatementOperations;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaStateConcern;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StackingQueryRegistrationOperations;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.MetaDataStore;
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
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailScanner;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFileInformation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholds;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CountCommittedTransactionThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TimeCheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
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
import org.neo4j.kernel.recovery.DefaultRecoverySPI;
import org.neo4j.kernel.recovery.PositionToRecoverFrom;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.kernel.spi.explicitindex.IndexProviders;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.fromConfigValue;

public class NeoStoreDataSource implements Lifecycle, IndexProviders
{

    enum Diagnostics implements DiagnosticsExtractor<NeoStoreDataSource>
    {
        TRANSACTION_RANGE( "Transaction log:" )
        {
            @Override
            void dump( NeoStoreDataSource source, Logger log )
            {
                PhysicalLogFiles logFiles =
                        source.getDependencyResolver().resolveDependency( PhysicalLogFiles.class );
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
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;
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
    private final PhysicalLogFile.Monitor physicalLogMonitor;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final StartupStatisticsProvider startupStatistics;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final Guard guard;
    private final Map<String,IndexImplementation> indexProviders = new HashMap<>();
    private final ExplicitIndexProviderLookup explicitIndexProviderLookup;
    private final ConstraintSemantics constraintSemantics;
    private final Procedures procedures;
    private final IOLimiter ioLimiter;
    private final AvailabilityGuard availabilityGuard;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;

    private Dependencies dependencies;
    private LifeSupport life;
    private SchemaIndexProviderMap schemaIndexProviderMap;
    private File storeDir;
    private boolean readOnly;
    private final IdController idController;
    private final OperationalMode operationalMode;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final AccessCapability accessCapability;

    private StorageEngine storageEngine;
    private NeoStoreTransactionLogModule transactionLogModule;
    private NeoStoreKernelModule kernelModule;

    /**
     * Note that the tremendous number of dependencies for this class, clearly, is an architecture smell. It is part
     * of the ongoing work on introducing the Kernel API, where components that were previously spread throughout the
     * core API are now slowly accumulating in the Kernel implementation. Over time, these components should be
     * refactored into bigger components that wrap the very granular things we depend on here.
     */
    public NeoStoreDataSource(
            File storeDir,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            LogService logService,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            DependencyResolver dependencyResolver,
            PropertyKeyTokenHolder propertyKeyTokens,
            LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            StatementLocksFactory statementLocksFactory,
            SchemaWriteGuard schemaWriteGuard,
            TransactionEventHandlers transactionEventHandlers,
            IndexingService.Monitor indexingServiceMonitor,
            FileSystemAbstraction fs,
            TransactionMonitor transactionMonitor,
            DatabaseHealth databaseHealth,
            PhysicalLogFile.Monitor physicalLogMonitor,
            TransactionHeaderInformationFactory transactionHeaderInformationFactory,
            StartupStatisticsProvider startupStatistics,
            Guard guard,
            CommitProcessFactory commitProcessFactory,
            AutoIndexing autoIndexing,
            PageCache pageCache,
            ConstraintSemantics constraintSemantics,
            Monitors monitors,
            Tracers tracers,
            Procedures procedures,
            IOLimiter ioLimiter,
            AvailabilityGuard availabilityGuard,
            SystemNanoClock clock,
            AccessCapability accessCapability,
            StoreCopyCheckPointMutex storeCopyCheckPointMutex,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IdController idController,
            OperationalMode operationalMode )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenNameLookup = tokenNameLookup;
        this.dependencyResolver = dependencyResolver;
        this.scheduler = scheduler;
        this.logService = logService;
        this.autoIndexing = autoIndexing;
        this.storeCopyCheckPointMutex = storeCopyCheckPointMutex;
        this.logProvider = logService.getInternalLogProvider();
        this.propertyKeyTokenHolder = propertyKeyTokens;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        this.statementLocksFactory = statementLocksFactory;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionEventHandlers = transactionEventHandlers;
        this.indexingServiceMonitor = indexingServiceMonitor;
        this.fs = fs;
        this.transactionMonitor = transactionMonitor;
        this.databaseHealth = databaseHealth;
        this.physicalLogMonitor = physicalLogMonitor;
        this.transactionHeaderInformationFactory = transactionHeaderInformationFactory;
        this.startupStatistics = startupStatistics;
        this.guard = guard;
        this.constraintSemantics = constraintSemantics;
        this.monitors = monitors;
        this.tracers = tracers;
        this.procedures = procedures;
        this.ioLimiter = ioLimiter;
        this.availabilityGuard = availabilityGuard;
        this.clock = clock;
        this.accessCapability = accessCapability;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;

        readOnly = config.get( Configuration.read_only );
        this.idController = idController;
        this.operationalMode = operationalMode;
        msgLog = logProvider.getLog( getClass() );
        this.lockService = new ReentrantLockService();
        this.explicitIndexProviderLookup = new ExplicitIndexProviderLookup()
        {
            @Override
            public IndexImplementation apply( String name )
            {
                assert name != null : "Null provider name supplied";
                IndexImplementation provider = indexProviders.get( name );
                if ( provider == null )
                {
                    throw new IllegalArgumentException( "No index provider '" + name +
                                                        "' found. Maybe the intended provider (or one more of its " +
                                                        "dependencies) " +
                                                        "aren't on the classpath or it failed to load." );
                }
                return provider;
            }

            @Override
            public Iterable<IndexImplementation> all()
            {
                return indexProviders.values();
            }
        };

        this.commitProcessFactory = commitProcessFactory;
        this.pageCache = pageCache;
        this.monitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
    }

    @Override
    public void init()
    {
        // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    @Override
    public void start() throws IOException
    {
        dependencies = new Dependencies();
        life = new LifeSupport();

        life.add( recoveryCleanupWorkCollector );

        AllByPrioritySelectionStrategy<SchemaIndexProvider> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
        SchemaIndexProvider defaultIndexProvider =
                dependencyResolver.resolveDependency( SchemaIndexProvider.class, indexProviderSelection );

        schemaIndexProviderMap =
                new DefaultSchemaIndexProviderMap( defaultIndexProvider, indexProviderSelection.lowerPrioritizedCandidates() );
        dependencies.satisfyDependency( schemaIndexProviderMap );

        IndexConfigStore indexConfigStore = new IndexConfigStore( storeDir, fs );
        dependencies.satisfyDependency( lockService );
        dependencies.satisfyDependency( indexConfigStore );
        life.add( indexConfigStore );

        life.add( new Delegate( Lifecycles.multiple( indexProviders.values() ) ) );

        // Check the tail of transaction logs and validate version
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, PhysicalLogFile.DEFAULT_NAME, fs );
        final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, fs, logEntryReader );
        LogVersionUpgradeChecker.check( tailScanner, config );

        // Upgrade the store before we begin
        RecordFormats formats = selectStoreFormats( config, storeDir, fs, pageCache, logService );
        upgradeStore( formats, tailScanner );

        // Build all modules and their services
        StorageEngine storageEngine = null;
        try
        {
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( logProvider );

            SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering = new SynchronizedArrayIdOrderingQueue( 20 );

            Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier = () -> kernelModule.kernelTransactions().get();
            idController.initialize( transactionsSnapshotSupplier );

            storageEngine = buildStorageEngine(
                    propertyKeyTokenHolder, labelTokens, relationshipTypeTokens, explicitIndexProviderLookup,
                    indexConfigStore, databaseSchemaState, explicitIndexTransactionOrdering, operationalMode );

            TransactionIdStore transactionIdStore = dependencies.resolveDependency( TransactionIdStore.class );
            LogVersionRepository logVersionRepository = dependencies.resolveDependency( LogVersionRepository.class );
            NeoStoreTransactionLogModule transactionLogModule =
                    buildTransactionLogs( logFiles, config, logProvider, scheduler, fs,
                            storageEngine, logEntryReader, explicitIndexTransactionOrdering,
                            transactionIdStore, logVersionRepository );
            transactionLogModule.satisfyDependencies(dependencies);

            buildRecovery( fs,
                    transactionIdStore,
                    tailScanner,
                    monitors.newMonitor( Recovery.Monitor.class ),
                    monitors.newMonitor( PositionToRecoverFrom.Monitor.class ),
                    logFiles, startupStatistics,
                    storageEngine, transactionLogModule.logicalTransactionStore()
            );

            // At the time of writing this comes from the storage engine (IndexStoreView)
            PropertyAccessor propertyAccessor = dependencies.resolveDependency( PropertyAccessor.class );

            final NeoStoreKernelModule kernelModule = buildKernel(
                    transactionLogModule.transactionAppender(),
                    dependencies.resolveDependency( IndexingService.class ),
                    storageEngine.storeReadLayer(),
                    databaseSchemaState,
                    dependencies.resolveDependency( LabelScanStore.class ),
                    storageEngine,
                    indexConfigStore,
                    transactionIdStore,
                    availabilityGuard,
                    clock,
                    propertyAccessor );

            kernelModule.satisfyDependencies( dependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.storageEngine = storageEngine;
            this.transactionLogModule = transactionLogModule;
            this.kernelModule = kernelModule;

            dependencies.satisfyDependency( this );
            dependencies.satisfyDependency( databaseSchemaState );
            dependencies.satisfyDependency( storageEngine.storeReadLayer() );
            dependencies.satisfyDependency( logEntryReader );
            dependencies.satisfyDependency( storageEngine );
            dependencies.satisfyDependency( explicitIndexProviderLookup );
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
            throw Exceptions.launderedException( e );
        }

        // NOTE: please make sure this is performed after having added everything to the life, in fact we would like
        // to perform the checkpointing as first step when the life is shutdown.
        life.add( lifecycleToTriggerCheckPointOnShutdown() );

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
            throw Exceptions.launderedException( e );
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

    private static RecordFormats selectStoreFormats( Config config, File storeDir, FileSystemAbstraction fs,
            PageCache pageCache, LogService logService )
    {
        LogProvider logging = logService.getInternalLogProvider();
        RecordFormats formats = RecordFormatSelector.selectNewestFormat( config, storeDir, fs, pageCache, logging );
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
                schemaIndexProviderMap,
                indexProviders,
                pageCache,
                format, tailScanner ).migrate( storeDir );
    }

    private StorageEngine buildStorageEngine(
            PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            ExplicitIndexProviderLookup explicitIndexProviderLookup, IndexConfigStore indexConfigStore,
            SchemaState schemaState, SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering, OperationalMode operationalMode )
    {
        RecordStorageEngine storageEngine =
                new RecordStorageEngine( storeDir, config, pageCache, fs, logProvider, propertyKeyTokenHolder,
                        labelTokens, relationshipTypeTokens, schemaState, constraintSemantics, scheduler,
                        tokenNameLookup, lockService, schemaIndexProviderMap, indexingServiceMonitor, databaseHealth,
                        explicitIndexProviderLookup, indexConfigStore,
                        explicitIndexTransactionOrdering, idGeneratorFactory, idController, monitors, recoveryCleanupWorkCollector,
                        operationalMode );

        // We pretend that the storage engine abstract hides all details within it. Whereas that's mostly
        // true it's not entirely true for the time being. As long as we need this call below, which
        // makes available one or more internal things to the outside world, there are leaks to plug.
        storageEngine.satisfyDependencies( dependencies );

        return life.add( storageEngine );
    }

    private NeoStoreTransactionLogModule buildTransactionLogs(
            PhysicalLogFiles logFiles,
            Config config,
            LogProvider logProvider,
            JobScheduler scheduler,
            FileSystemAbstraction fileSystemAbstraction,
            StorageEngine storageEngine, LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader,
            SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering,
            TransactionIdStore transactionIdStore, LogVersionRepository logVersionRepository )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 100_000 );
        LogHeaderCache logHeaderCache = new LogHeaderCache( 1000 );

        final PhysicalLogFile logFile = life.add( new PhysicalLogFile( fileSystemAbstraction, logFiles,
                config.get( GraphDatabaseSettings.logical_log_rotation_threshold ),
                transactionIdStore::getLastCommittedTransactionId, logVersionRepository, physicalLogMonitor,
                logHeaderCache ) );

        final PhysicalLogFileInformation.LogVersionToTimestamp logInformation = version ->
        {
            LogPosition position = LogPosition.start( version );
            try ( ReadableLogChannel channel = logFile.getReader( position ) )
            {
                LogEntry entry;
                while ( (entry = logEntryReader.readLogEntry( channel )) != null )
                {
                    if ( entry instanceof LogEntryStart )
                    {
                        return entry.<LogEntryStart>as().getTimeWritten();
                    }
                }
            }
            return -1;
        };
        final LogFileInformation logFileInformation =
                new PhysicalLogFileInformation( logFiles, logHeaderCache,
                        transactionIdStore::getLastCommittedTransactionId, logInformation );

        if ( config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral ) )
        {
            config.augmentDefaults( GraphDatabaseSettings.keep_logical_logs, "1 files" );
        }
        String pruningConf = config.get( GraphDatabaseSettings.keep_logical_logs );

        LogPruneStrategy logPruneStrategy = fromConfigValue( fs, logFileInformation, logFiles, pruningConf );

        final LogPruning logPruning = new LogPruningImpl( logPruneStrategy, logProvider );

        final LogRotation logRotation =
                new LogRotationImpl( monitors.newMonitor( LogRotation.Monitor.class ), logFile, databaseHealth );

        final TransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFile, logRotation, transactionMetadataCache, transactionIdStore, explicitIndexTransactionOrdering,
                databaseHealth ) );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFile, transactionMetadataCache, logEntryReader );

        int txThreshold = config.get( GraphDatabaseSettings.check_point_interval_tx );
        final CountCommittedTransactionThreshold countCommittedTransactionThreshold =
                new CountCommittedTransactionThreshold( txThreshold );

        long timeMillisThreshold = config.get( GraphDatabaseSettings.check_point_interval_time ).toMillis();
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( timeMillisThreshold, clock );

        CheckPointThreshold threshold =
                CheckPointThresholds.or( countCommittedTransactionThreshold, timeCheckPointThreshold );

        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                transactionIdStore, threshold, storageEngine, logPruning, appender, databaseHealth, logProvider,
                tracers.checkPointTracer, ioLimiter, storeCopyCheckPointMutex );

        long recurringPeriod = Math.min( timeMillisThreshold, TimeUnit.SECONDS.toMillis( 10 ) );
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioLimiter, scheduler,
                recurringPeriod, databaseHealth );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new NeoStoreTransactionLogModule( logicalTransactionStore, logFileInformation, logFiles, logFile,
                logRotation, checkPointer, appender, explicitIndexTransactionOrdering );
    }

    private void buildRecovery(
            final FileSystemAbstraction fileSystemAbstraction,
            TransactionIdStore transactionIdStore,
            LogTailScanner tailScanner,
            Recovery.Monitor recoveryMonitor,
            PositionToRecoverFrom.Monitor positionMonitor,
            final PhysicalLogFiles logFiles,
            final StartupStatisticsProvider startupStatistics,
            StorageEngine storageEngine,
            LogicalTransactionStore logicalTransactionStore )
    {
        Recovery.SPI spi =
                new DefaultRecoverySPI( storageEngine, logFiles, fileSystemAbstraction, tailScanner, transactionIdStore,
                        logicalTransactionStore, positionMonitor );
        Recovery recovery = new Recovery( spi, recoveryMonitor );
        monitors.addMonitorListener( new Recovery.Monitor()
        {
            @Override
            public void recoveryCompleted( int numberOfRecoveredTransactions )
            {
                startupStatistics.setNumberOfRecoveredTransactions( numberOfRecoveredTransactions );
            }
        } );
        life.add( recovery );
    }

    private NeoStoreKernelModule buildKernel( TransactionAppender appender,
                                      IndexingService indexingService,
                                      StoreReadLayer storeLayer,
                                      DatabaseSchemaState databaseSchemaState, LabelScanStore labelScanStore,
                                      StorageEngine storageEngine,
                                      IndexConfigStore indexConfigStore,
                                      TransactionIdStore transactionIdStore,
                                      AvailabilityGuard availabilityGuard,
                                      Clock clock,
                                      PropertyAccessor propertyAccessor ) throws KernelException, IOException
    {
        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine,
                config );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<KernelAPI> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService,
                propertyAccessor );

        ExplicitIndexStore explicitIndexStore = new ExplicitIndexStore( config,
                indexConfigStore, kernelProvider, explicitIndexProviderLookup );

        StatementOperationParts statementOperationParts = dependencies.satisfyDependency(
                buildStatementOperations( storeLayer, autoIndexing,
                        constraintIndexCreator, databaseSchemaState, guard, explicitIndexStore ) );

        TransactionHooks hooks = new TransactionHooks();
        KernelTransactions kernelTransactions = life.add( new KernelTransactions( statementLocksFactory,
                constraintIndexCreator, statementOperationParts, schemaWriteGuard, transactionHeaderInformationFactory,
                transactionCommitProcess, indexConfigStore, explicitIndexProviderLookup, hooks, transactionMonitor,
                availabilityGuard, tracers, storageEngine, procedures, transactionIdStore, clock, accessCapability ) );

        buildTransactionMonitor( kernelTransactions, clock, config );

        final Kernel kernel = new Kernel( kernelTransactions, hooks, databaseHealth, transactionMonitor, procedures,
                config );

        kernel.registerTransactionHook( transactionEventHandlers );

        final NeoStoreFileListing fileListing = new NeoStoreFileListing( storeDir, labelScanStore, indexingService,
                explicitIndexProviderLookup, storageEngine );

        return new NeoStoreKernelModule( transactionCommitProcess, kernel, kernelTransactions, fileListing );
    }

    private void buildTransactionMonitor( KernelTransactions kernelTransactions, Clock clock, Config config )
    {
        KernelTransactionTimeoutMonitor kernelTransactionTimeoutMonitor =
                new KernelTransactionTimeoutMonitor( kernelTransactions, clock, logService );
        dependencies.satisfyDependency( kernelTransactionTimeoutMonitor );
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
        return  new LifecycleAdapter()
        {
            @Override
            public void shutdown() throws Throwable
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

    @Override
    public void shutdown()
    {   // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    public StoreId getStoreId()
    {
        return getDependencyResolver().resolveDependency( MetaDataStore.class ).getStoreId();
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public KernelAPI getKernel()
    {
        return kernelModule.kernelAPI();
    }

    public ResourceIterator<StoreFileMetadata> listStoreFiles( boolean includeLogs ) throws IOException
    {
        return kernelModule.fileListing().listStoreFiles( includeLogs );
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        storageEngine.registerDiagnostics( manager );
        manager.registerAll( Diagnostics.class, this );
    }

    public StoreReadLayer getStoreLayer()
    {
        return storageEngine.storeReadLayer();
    }

    public DependencyResolver getDependencyResolver()
    {
        return dependencies;
    }

    private StatementOperationParts buildStatementOperations(
            StoreReadLayer storeReadLayer, AutoIndexing autoIndexing,
            ConstraintIndexCreator constraintIndexCreator, DatabaseSchemaState databaseSchemaState,
            Guard guard, ExplicitIndexStore explicitIndexStore )
    {
        // The passed in StoreReadLayer is the bottom most layer: Read-access to committed data.
        // To it we add:
        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations( storeReadLayer,
                autoIndexing, constraintIndexCreator, explicitIndexStore );

        CpuClock cpuClock = CpuClock.NOT_AVAILABLE;
        if ( config.get( GraphDatabaseSettings.track_query_cpu_time ) )
        {
            cpuClock = CpuClock.CPU_CLOCK;
        }
        HeapAllocation heapAllocation = HeapAllocation.NOT_AVAILABLE;
        if ( config.get( GraphDatabaseSettings.track_query_allocation ) )
        {
            heapAllocation = HeapAllocation.HEAP_ALLOCATION;
        }
        QueryRegistrationOperations queryRegistrationOperations =
                new StackingQueryRegistrationOperations( clock, cpuClock, heapAllocation );

        StatementOperationParts parts = new StatementOperationParts( stateHandlingContext, stateHandlingContext,
                stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( databaseSchemaState ), null, stateHandlingContext, stateHandlingContext,
                stateHandlingContext, queryRegistrationOperations );
        // + Constraints
        ConstraintEnforcingEntityOperations constraintEnforcingEntityOperations =
                new ConstraintEnforcingEntityOperations( constraintSemantics, parts.entityWriteOperations(), parts.entityReadOperations(),
                        parts.schemaWriteOperations(), parts.schemaReadOperations() );
        // + Data integrity
        DataIntegrityValidatingStatementOperations dataIntegrityContext =
                new DataIntegrityValidatingStatementOperations(
                        parts.keyWriteOperations(), parts.schemaReadOperations(), constraintEnforcingEntityOperations );
        parts = parts.override( null, dataIntegrityContext, constraintEnforcingEntityOperations,
                constraintEnforcingEntityOperations, null, dataIntegrityContext, null, null, null, null, null, null );
        // + Locking
        LockingStatementOperations lockingContext = new LockingStatementOperations( parts.entityReadOperations(),
                parts.entityWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations(),
                parts.schemaStateOperations() );
        parts = parts.override( null, null, null, lockingContext, lockingContext, lockingContext, lockingContext,
                lockingContext, null, null, null, null );

        return parts;
    }

    @Override
    public void registerIndexProvider( String name, IndexImplementation index )
    {
        assert !indexProviders.containsKey( name ) : "Index provider '" + name + "' already registered";
        indexProviders.put( name, index );
    }

    @Override
    public boolean unregisterIndexProvider( String name )
    {
        IndexImplementation removed = indexProviders.remove( name );
        return removed != null;
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

    // For test purposes only, not thread safe
    public LifeSupport getLife()
    {
        return life;
    }

    @SuppressWarnings( "deprecation" )
    public abstract static class Configuration
    {
        public static final Setting<String> keep_logical_logs = GraphDatabaseSettings.keep_logical_logs;
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
    }

}
