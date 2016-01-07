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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.dependency.HighestSelectionStrategy;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.DataIntegrityValidatingStatementOperations;
import org.neo4j.kernel.impl.api.GuardingStatementOperations;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.LockingStatementOperations;
import org.neo4j.kernel.impl.api.SchemaStateConcern;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.builtinprocs.BuiltInProcedures;
import org.neo4j.proc.Procedures;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogFileRecoverer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFileInformation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholds;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CountCommittedTransactionThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
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
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.transaction.state.RecoveryVisitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.Lifecycles;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.kernel.recovery.DefaultRecoverySPI;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.fromConfigValue;

public class NeoStoreDataSource implements NeoStoresSupplier, Lifecycle, IndexProviders
{
    private interface TransactionLogModule
    {
        LogicalTransactionStore logicalTransactionStore();

        PhysicalLogFiles logFiles();

        LogFileInformation logFileInformation();

        LogFile logFile();

        StoreFlusher storeFlusher();

        LogRotation logRotation();

        CheckPointer checkPointing();

        TransactionAppender transactionAppender();

        IdOrderingQueue legacyIndexTransactionOrderingQueue();
    }

    private interface KernelModule
    {
        TransactionCommitProcess transactionCommitProcess();

        KernelTransactions kernelTransactions();

        KernelAPI kernelAPI();

        NeoStoreFileListing fileListing();
    }

    enum Diagnostics implements DiagnosticsExtractor<NeoStoreDataSource>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger logger )
                    {
                        neoStores( source.storageEngine ).logVersions( logger );
                    }
                },
        NEO_STORE_ID_USAGE( "Id usage:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger logger )
                    {
                        neoStores( source.storageEngine ).logIdUsage( logger );
                    }
                },
        NEO_STORE_RECORDS( "Neostore records:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger log )
                    {
                        neoStores( source.storageEngine ).getMetaDataStore().logRecords( log );
                    }
                },
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
                                if ( logFiles.hasAnyTransaction( logVersion ) )
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

        protected NeoStores neoStores( StorageEngine storageEngine )
        {
            return (NeoStores) storageEngine.neoStores();
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
    private final LogProvider logProvider;
    private final DependencyResolver dependencyResolver;
    private final TokenNameLookup tokenNameLookup;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;
    private final Locks locks;
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
    private final NodeManager nodeManager;
    private final CommitProcessFactory commitProcessFactory;
    private final PageCache pageCache;
    private final AtomicInteger recoveredCount = new AtomicInteger();
    private final Guard guard;
    private final Map<String,IndexImplementation> indexProviders = new HashMap<>();
    private final LegacyIndexProviderLookup legacyIndexProviderLookup;
    private final ConstraintSemantics constraintSemantics;

    private Dependencies dependencies;
    private LifeSupport life;
    private SchemaIndexProvider schemaIndexProvider;
    private File storeDir;
    private boolean readOnly;

    private StorageEngine storageEngine;
    private TransactionLogModule transactionLogModule;
    private KernelModule kernelModule;

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
            Locks lockManager,
            SchemaWriteGuard schemaWriteGuard,
            TransactionEventHandlers transactionEventHandlers,
            IndexingService.Monitor indexingServiceMonitor,
            FileSystemAbstraction fs,
            TransactionMonitor transactionMonitor,
            DatabaseHealth databaseHealth,
            PhysicalLogFile.Monitor physicalLogMonitor,
            TransactionHeaderInformationFactory transactionHeaderInformationFactory,
            StartupStatisticsProvider startupStatistics,
            NodeManager nodeManager,
            Guard guard,
            CommitProcessFactory commitProcessFactory,
            PageCache pageCache,
            ConstraintSemantics constraintSemantics,
            Monitors monitors,
            Tracers tracers )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.tokenNameLookup = tokenNameLookup;
        this.dependencyResolver = dependencyResolver;
        this.scheduler = scheduler;
        this.logService = logService;
        this.logProvider = logService.getInternalLogProvider();
        this.propertyKeyTokenHolder = propertyKeyTokens;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        this.locks = lockManager;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionEventHandlers = transactionEventHandlers;
        this.indexingServiceMonitor = indexingServiceMonitor;
        this.fs = fs;
        this.transactionMonitor = transactionMonitor;
        this.databaseHealth = databaseHealth;
        this.physicalLogMonitor = physicalLogMonitor;
        this.transactionHeaderInformationFactory = transactionHeaderInformationFactory;
        this.startupStatistics = startupStatistics;
        this.nodeManager = nodeManager;
        this.guard = guard;
        this.constraintSemantics = constraintSemantics;
        this.monitors = monitors;
        this.tracers = tracers;

        readOnly = config.get( Configuration.read_only );
        msgLog = logProvider.getLog( getClass() );
        this.idGeneratorFactory = idGeneratorFactory;
        this.lockService = new ReentrantLockService();
        this.legacyIndexProviderLookup = new LegacyIndexProviderLookup()
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
    }

    @Override
    public void init()
    { // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    @Override
    public void start() throws IOException
    {
        dependencies = new Dependencies();
        life = new LifeSupport();

        schemaIndexProvider = dependencyResolver.resolveDependency( SchemaIndexProvider.class,
                HighestSelectionStrategy.getInstance() );

        IndexConfigStore indexConfigStore = new IndexConfigStore( storeDir, fs );
        dependencies.satisfyDependency( lockService );
        dependencies.satisfyDependency( indexConfigStore );
        life.add( indexConfigStore );

        // Monitor listeners
        LoggingLogFileMonitor loggingLogMonitor = new LoggingLogFileMonitor( msgLog );
        monitors.addMonitorListener( loggingLogMonitor );
        monitors.addMonitorListener( (RecoveryVisitor.Monitor) txId -> recoveredCount.incrementAndGet() );

        life.add( new Delegate( Lifecycles.multiple( indexProviders.values() ) ) );

        // Upgrade the store before we begin
        upgradeStore();

        // Build all modules and their services
        StorageEngine storageEngine = null;
        try
        {
            UpdateableSchemaState updateableSchemaState = new KernelSchemaStateStore( logProvider );

            // TODO Introduce a StorageEngine abstraction at the StoreLayerModule boundary
            storageEngine = buildStorageEngine(
                    propertyKeyTokenHolder, labelTokens, relationshipTypeTokens, legacyIndexProviderLookup,
                    indexConfigStore,  updateableSchemaState::clear );
            LogEntryReader<ReadableLogChannel> logEntryReader =
                    new VersionAwareLogEntryReader<>( storageEngine.commandReaderFactory() );

            TransactionLogModule transactionLogModule =
                    buildTransactionLogs( storeDir, config, logProvider, scheduler, fs,
                            indexProviders.values(), storageEngine, logEntryReader );

            buildRecovery( fs,
                    (NeoStores) storageEngine.neoStores(),
                    monitors.newMonitor( RecoveryVisitor.Monitor.class ), monitors.newMonitor( Recovery.Monitor.class ),
                    transactionLogModule.logFiles(), transactionLogModule.storeFlusher(), startupStatistics,
                    storageEngine, logEntryReader );

            KernelModule kernelModule = buildKernel(
                    transactionLogModule.transactionAppender(),
                    (IndexingService) storageEngine.indexingService(),
                    storageEngine.storeReadLayer(),
                    updateableSchemaState, (LabelScanStore) storageEngine.labelScanStore(),
                    storageEngine );


            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.storageEngine = storageEngine;
            this.transactionLogModule = transactionLogModule;
            this.kernelModule = kernelModule;

            dependencies.satisfyDependency( this );
            dependencies.satisfyDependency( updateableSchemaState );
            dependencies.satisfyDependency( storageEngine.cacheAccess() );
            dependencies.satisfyDependency( storageEngine.indexingService() );
            dependencies.satisfyDependency( storageEngine.integrityValidator() );
            dependencies.satisfyDependency( storageEngine.labelScanStore() );
            dependencies.satisfyDependency( storageEngine.metaDataStore() );
            dependencies.satisfyDependency( storageEngine.neoStores() );
            dependencies.satisfyDependency( storageEngine.schemaIndexProviderMap() );
            dependencies.satisfyDependency( storageEngine.legacyIndexApplierLookup() );
            dependencies.satisfyDependency( storageEngine.storeReadLayer() );
            dependencies.satisfyDependency( logEntryReader );
            dependencies.satisfyDependency( storageEngine );
            satisfyDependencies( transactionLogModule, kernelModule );
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while setting up store modules. Attempting to close things down.",
                    e, true );
            try
            {
                // Close the neostore, so that locks are released properly
                if ( storageEngine != null )
                {
                    ((NeoStores)storageEngine.neoStores()).close();
                }
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close neostore after startup failure" );
            }
            throw Exceptions.launderedException( e );
        }

        try
        {
            life.start();
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while starting the datasource. Attempting to close things down.",
                    e, true );
            try
            {
                life.shutdown();
                // Close the neostore, so that locks are released properly
                ((NeoStores)storageEngine.neoStores()).close();
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close neostore after startup failure" );
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

    private void upgradeStore()
    {
        LabelScanStoreProvider labelScanStoreProvider =
                dependencyResolver.resolveDependency( LabelScanStoreProvider.class,
                        HighestSelectionStrategy.getInstance() );

        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( logService.getUserLog( StoreMigrator.class ) );
        new DatabaseMigrator(
                progressMonitor,
                fs,
                config,
                logService,
                schemaIndexProvider,
                labelScanStoreProvider,
                indexProviders,
                pageCache ).migrate( storeDir );
    }

    private StorageEngine buildStorageEngine(
            PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            LegacyIndexProviderLookup legacyIndexProviderLookup, IndexConfigStore indexConfigStore,
            Runnable schemaStateChangeCallback )
    {
        LabelScanStoreProvider labelScanStore = dependencyResolver.resolveDependency( LabelScanStoreProvider.class,
                HighestSelectionStrategy.getInstance());
        return life.add(
                new RecordStorageEngine( storeDir, config, idGeneratorFactory, pageCache, fs, logProvider, propertyKeyTokenHolder,
                        labelTokens, relationshipTypeTokens, schemaStateChangeCallback, constraintSemantics, scheduler,
                        tokenNameLookup, lockService, schemaIndexProvider, indexingServiceMonitor, databaseHealth,
                        labelScanStore, legacyIndexProviderLookup, indexConfigStore ) );
    }

    private TransactionLogModule buildTransactionLogs(
            File storeDir,
            Config config,
            LogProvider logProvider,
            JobScheduler scheduler,
            FileSystemAbstraction fileSystemAbstraction,
            Iterable<IndexImplementation> indexProviders,
            StorageEngine storageEngine, LogEntryReader<ReadableLogChannel> logEntryReader )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 1000, 100_000 );
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, PhysicalLogFile.DEFAULT_NAME,
                fileSystemAbstraction );

        final IdOrderingQueue legacyIndexTransactionOrdering =
                (IdOrderingQueue) storageEngine.legacyIndexTransactionOrdering();

        TransactionIdStore transactionIdStore = (TransactionIdStore) storageEngine.transactionIdStore();
        final PhysicalLogFile logFile = life.add( new PhysicalLogFile( fileSystemAbstraction, logFiles,
                config.get( GraphDatabaseSettings.logical_log_rotation_threshold ), transactionIdStore,
                (LogVersionRepository) storageEngine.logVersionRepository(), physicalLogMonitor, transactionMetadataCache ) );

        final PhysicalLogFileInformation.LogVersionToTimestamp
                logInformation = new PhysicalLogFileInformation.LogVersionToTimestamp()
        {
            @Override
            public long getTimestampForVersion( long version ) throws IOException
            {
                LogPosition position = LogPosition.start( version );
                try ( ReadableVersionableLogChannel channel = logFile.getReader( position ) )
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
            }
        };
        final LogFileInformation logFileInformation =
                new PhysicalLogFileInformation( logFiles, transactionMetadataCache, transactionIdStore, logInformation );

        String pruningConf = config.get(
                config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral )
                ? GraphDatabaseFacadeFactory.Configuration.ephemeral_keep_logical_logs
                : GraphDatabaseSettings.keep_logical_logs );

        LogPruneStrategy logPruneStrategy = fromConfigValue( fs, logFileInformation, logFiles, pruningConf );

        final LogPruning logPruning = new LogPruningImpl( logPruneStrategy, logProvider );

        final StoreFlusher storeFlusher = new StoreFlusher( storageEngine, indexProviders );

        final LogRotation logRotation =
                new LogRotationImpl( monitors.newMonitor( LogRotation.Monitor.class ), logFile, databaseHealth );

        final TransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFile, logRotation, transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering,
                databaseHealth ) );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFile, transactionMetadataCache, logEntryReader );

        int txThreshold = config.get( GraphDatabaseSettings.check_point_interval_tx );
        final CountCommittedTransactionThreshold countCommittedTransactionThreshold =
                new CountCommittedTransactionThreshold( txThreshold );

        long timeMillisThreshold = config.get( GraphDatabaseSettings.check_point_interval_time );
        TimeCheckPointThreshold timeCheckPointThreshold =
                new TimeCheckPointThreshold( timeMillisThreshold, Clock.SYSTEM_CLOCK );

        CheckPointThreshold threshold =
                CheckPointThresholds.or( countCommittedTransactionThreshold, timeCheckPointThreshold );

        final CheckPointerImpl checkPointer = new CheckPointerImpl(
                transactionIdStore, threshold, storeFlusher, logPruning, appender, databaseHealth, logProvider,
                tracers.checkPointTracer );

        long recurringPeriod = Math.min( timeMillisThreshold, TimeUnit.SECONDS.toMillis( 10 ) );
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, scheduler, recurringPeriod );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new TransactionLogModule()
        {
            @Override
            public LogicalTransactionStore logicalTransactionStore()
            {
                return logicalTransactionStore;
            }

            @Override
            public LogFileInformation logFileInformation()
            {
                return logFileInformation;
            }

            @Override
            public PhysicalLogFiles logFiles()
            {
                return logFiles;
            }

            @Override
            public LogFile logFile()
            {
                return logFile;
            }

            @Override
            public StoreFlusher storeFlusher()
            {
                return storeFlusher;
            }

            @Override
            public LogRotation logRotation()
            {
                return logRotation;
            }

            @Override
            public CheckPointer checkPointing()
            {
                return checkPointer;
            }

            @Override
            public TransactionAppender transactionAppender()
            {
                return appender;
            }

            @Override
            public IdOrderingQueue legacyIndexTransactionOrderingQueue()
            {
                return legacyIndexTransactionOrdering;
            }
        };
    }

    private void buildRecovery(
            final FileSystemAbstraction fileSystemAbstraction,
            final NeoStores neoStores,
            RecoveryVisitor.Monitor recoveryVisitorMonitor,
            Recovery.Monitor recoveryMonitor,
            final PhysicalLogFiles logFiles,
            final StoreFlusher storeFlusher,
            final StartupStatisticsProvider startupStatistics,
            StorageEngine storageEngine,
            LogEntryReader<ReadableLogChannel> logEntryReader )
    {
        MetaDataStore metaDataStore = neoStores.getMetaDataStore();
        RecoveryVisitor recoveryVisitor = new RecoveryVisitor( metaDataStore, storageEngine, recoveryVisitorMonitor );

        final Visitor<LogVersionedStoreChannel,Exception> logFileRecoverer =
                new LogFileRecoverer( logEntryReader, recoveryVisitor );

        final LatestCheckPointFinder checkPointFinder =
                new LatestCheckPointFinder( logFiles, fileSystemAbstraction, logEntryReader );
        Recovery.SPI spi = new DefaultRecoverySPI(
                storeFlusher, neoStores, logFileRecoverer, logFiles, fileSystemAbstraction, metaDataStore,
                checkPointFinder );
        Recovery recovery = new Recovery( spi, recoveryMonitor );

        life.add( recovery );

        life.add( new LifecycleAdapter()
        {
            @Override
            public void init() throws Throwable
            {
                startupStatistics.setNumberOfRecoveredTransactions( recoveredCount.get() );
                recoveredCount.set( 0 );
            }
        } );
    }

    private KernelModule buildKernel( TransactionAppender appender,
                                      IndexingService indexingService,
                                      StoreReadLayer storeLayer,
                                      UpdateableSchemaState updateableSchemaState, LabelScanStore labelScanStore,
                                      StorageEngine storageEngine ) throws ProcedureException
    {
        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine,
                config );

        /*
         * This is used by legacy indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<KernelAPI> kernelProvider = new Supplier<KernelAPI>()
        {
            @Override
            public KernelAPI get()
            {
                return kernelModule.kernelAPI();
            }
        };

        ConstraintIndexCreator constraintIndexCreator =
                new ConstraintIndexCreator( kernelProvider, indexingService );

        LegacyIndexStore legacyIndexStore = new LegacyIndexStore( config,
                (IndexConfigStore) storageEngine.indexConfigStore(), kernelProvider, legacyIndexProviderLookup );

        LegacyPropertyTrackers legacyPropertyTrackers = new LegacyPropertyTrackers( propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(), nodeManager.getRelationshipPropertyTrackers(), nodeManager );

        StatementOperationParts statementOperations = dependencies.satisfyDependency( buildStatementOperations(
                storeLayer, legacyPropertyTrackers, constraintIndexCreator, updateableSchemaState, guard,
                legacyIndexStore ) );

        Procedures procedures = dependencies.satisfyDependency(new Procedures());

        TransactionHooks hooks = new TransactionHooks();
        KernelTransactions kernelTransactions = life.add( new KernelTransactions( locks, constraintIndexCreator,
                statementOperations, schemaWriteGuard, transactionHeaderInformationFactory,
                transactionCommitProcess, (IndexConfigStore) storageEngine.indexConfigStore(),
                legacyIndexProviderLookup, hooks, transactionMonitor, life, tracers, storageEngine, procedures ) );

        final Kernel kernel = new Kernel( kernelTransactions, hooks, databaseHealth, transactionMonitor, procedures );

        kernel.registerTransactionHook( transactionEventHandlers );

        BuiltInProcedures.addTo( kernel );

        final NeoStoreFileListing fileListing = new NeoStoreFileListing( storeDir, labelScanStore, indexingService,
                legacyIndexProviderLookup );

        return new KernelModule()
        {
            @Override
            public TransactionCommitProcess transactionCommitProcess()
            {
                return transactionCommitProcess;
            }

            @Override
            public KernelAPI kernelAPI()
            {
                return kernel;
            }

            @Override
            public KernelTransactions kernelTransactions()
            {
                return kernelTransactions;
            }

            @Override
            public NeoStoreFileListing fileListing()
            {
                return fileListing;
            }
        };
    }

    // We do this last to ensure no one is cheating with dependency access
    private void satisfyDependencies( Object... modules )
    {
        for ( Object module : modules )
        {
            for ( Method method : module.getClass().getMethods() )
            {
                if ( !method.getDeclaringClass().equals( Object.class ) && method.getReturnType() != void.class )
                {
                    try
                    {
                        dependencies.satisfyDependency( method.invoke( module ) );
                    }
                    catch ( IllegalAccessException | InvocationTargetException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
    }

    // Only public for testing purpose
    public NeoStores getNeoStores()
    {
        return (NeoStores) storageEngine.neoStores();
    }

    public IndexingService getIndexService()
    {
        return (IndexingService) storageEngine.indexingService();
    }

    public LabelScanStore getLabelScanStore()
    {
        return (LabelScanStore) storageEngine.labelScanStore();
    }

    @Override
    public synchronized void stop()
    {
        if ( !life.isRunning() )
        {
            return;
        }

        CheckPointer checkPointer = transactionLogModule.checkPointing();

        // First kindly await all committing transactions to close. Do this without interfering with the
        // log file monitor. Keep in mind that at this point the availability guard is raised and some time spent
        // awaiting active transaction to close, on a more coarse-grained level, so no new transactions
        // should get started. With that said there's actually a race between checking the availability guard
        // in beginTx, incrementing number of open transactions and raising the guard in shutdown, there might
        // be some in flight that will get to commit at some point
        // in the future. Such transactions will fail if they come to commit after our synchronized block below.
        // Here we're zooming in and focusing on getting committed transactions to close.
        awaitAllTransactionsClosed();
        LogFile logFile = transactionLogModule.logFile();
        // In order to prevent various issues with life components that can perform operations with logFile on their
        // stop phase before performing further shutdown/cleanup work and taking a lock on a logfile
        // we stop all other life components to make sure that we are the last and only one (from current life)
        life.stop();
        synchronized ( logFile )
        {
            // Under the guard of the logFile monitor do a second pass of waiting committing transactions
            // to close. This is because there might have been transactions that were in flight and just now
            // want to commit. We will allow committed transactions be properly closed, but no new transactions
            // will be able to start committing at this point.
            awaitAllTransactionsClosed();

            // Write new checkpoint in the log only if the kernel is healthy.
            // We cannot throw here since we need to shutdown without exceptions.
            if ( databaseHealth.isHealthy() )
            {
                try
                {
                    // Flushing of neo stores happens as part of the checkpoint
                    checkPointer.forceCheckPoint( new SimpleTriggerInfo( "database shutdown" ) );
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            // Shut down all services in here, effectively making the database unusable for anyone who tries.
            life.shutdown();

            // Close the NeoStores
            ((NeoStores)storageEngine.neoStores()).close();
            msgLog.info( "NeoStores closed" );
        }
        // After we've released the logFile monitor there might be transactions that wants to commit, but had
        // to wait for the logFile monitor until now. When they finally get it and try to commit they will
        // fail since the logFile no longer works.
    }

    private void awaitAllTransactionsClosed()
    {
        // Only wait for committed transactions to be applied if the kernel is healthy (i.e. no panic)
        // otherwise if there has been a panic transactions will not be applied properly anyway.
        TransactionIdStore txIdStore = ((NeoStores) storageEngine.neoStores()).getMetaDataStore();
        while ( databaseHealth.isHealthy() &&
                !txIdStore.closedTransactionIdIsOnParWithOpenedTransactionId() )
        {
            LockSupport.parkNanos( 10_000_000 ); // 10 ms
        }
    }

    @Override
    public void shutdown()
    { // We do our own internal life management:
        // start() does life.init() and life.start(),
        // stop() does life.stop() and life.shutdown().
    }

    public StoreId getStoreId()
    {
        return getNeoStores().getMetaDataStore().getStoreId();
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    public long getCreationTime()
    {
        return getNeoStores().getMetaDataStore().getCreationTime();
    }

    public long getRandomIdentifier()
    {
        return getNeoStores().getMetaDataStore().getRandomNumber();
    }

    public long getCurrentLogVersion()
    {
        return getNeoStores().getMetaDataStore().getCurrentLogVersion();
    }

    public long getLastCommittedTransactionId()
    {
        return getNeoStores().getMetaDataStore().getLastCommittedTransactionId();
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public KernelAPI getKernel()
    {
        return kernelModule.kernelAPI();
    }

    public ResourceIterator<File> listStoreFiles( boolean includeLogs ) throws IOException
    {
        return kernelModule.fileListing().listStoreFiles( includeLogs );
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        manager.registerAll( Diagnostics.class, this );
    }

    @Override
    public NeoStores get()
    {
        return (NeoStores) storageEngine.neoStores();
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
            StoreReadLayer storeReadLayer, LegacyPropertyTrackers legacyPropertyTrackers,
            ConstraintIndexCreator constraintIndexCreator, UpdateableSchemaState updateableSchemaState,
            Guard guard, LegacyIndexStore legacyIndexStore )
    {
        // The passed in StoreReadLayer is the bottom most layer: Read-access to committed data.
        // To it we add:
        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations( storeReadLayer,
                legacyPropertyTrackers, constraintIndexCreator,
                legacyIndexStore );
        StatementOperationParts parts = new StatementOperationParts( stateHandlingContext, stateHandlingContext,
                stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( updateableSchemaState ), null, stateHandlingContext, stateHandlingContext,
                stateHandlingContext );
        // + Constraints
        ConstraintEnforcingEntityOperations constraintEnforcingEntityOperations =
                new ConstraintEnforcingEntityOperations( constraintSemantics, parts.entityWriteOperations(), parts.entityReadOperations(),
                        parts.schemaWriteOperations(), parts.schemaReadOperations() );
        // + Data integrity
        DataIntegrityValidatingStatementOperations dataIntegrityContext =
                new DataIntegrityValidatingStatementOperations(
                        parts.keyWriteOperations(), parts.schemaReadOperations(), constraintEnforcingEntityOperations );
        parts = parts.override( null, dataIntegrityContext, constraintEnforcingEntityOperations,
                constraintEnforcingEntityOperations, null, dataIntegrityContext, null, null, null, null, null );
        // + Locking
        LockingStatementOperations lockingContext = new LockingStatementOperations( parts.entityReadOperations(),
                parts.entityWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations(),
                parts.schemaStateOperations() );
        parts = parts.override( null, null, null, lockingContext, lockingContext, lockingContext, lockingContext,
                lockingContext, null, null, null );
        // + Guard
        if ( guard != null )
        {
            GuardingStatementOperations guardingOperations = new GuardingStatementOperations(
                    parts.entityWriteOperations(), parts.entityReadOperations(), guard );
            parts = parts.override( null, null, guardingOperations, guardingOperations, null, null, null, null,
                    null, null, null );
        }

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
        // Get rid of all pooled transactions, as they will otherwise reference
        // components that have been swapped out during the mode switch.
        kernelModule.kernelTransactions().disposeAll();
    }

    @SuppressWarnings( "deprecation" )
    public static abstract class Configuration
    {
        public static final Setting<String> keep_logical_logs = GraphDatabaseSettings.keep_logical_logs;
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
    }
}
