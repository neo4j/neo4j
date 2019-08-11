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
package org.neo4j.kernel.database;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.LoggingMonitor;
import org.neo4j.internal.index.label.NativeLabelScanStore;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailability;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.diagnostics.providers.DbmsDiagnosticsManager;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitorScheduler;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.pagecache.PageCacheStartMetricsReporter;
import org.neo4j.kernel.impl.pagecache.PageCacheStopMetricsReporter;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.store.stats.DatabaseEntityCounters;
import org.neo4j.kernel.impl.storemigration.DatabaseMigratorFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogVersionUpgradeChecker;
import org.neo4j.kernel.impl.transaction.log.LoggingLogFileMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointerLifecycle;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.reverse.ReverseTransactionCursorLoggingMonitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedSingleFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.DynamicIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.internal.locker.LockerLifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.LoggingLogTailScannerMonitor;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.fail_on_corrupted_log_files;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.database.DatabaseFileHelper.filesToDeleteOnTruncation;
import static org.neo4j.kernel.database.DatabaseFileHelper.filesToKeepOnTruncation;
import static org.neo4j.kernel.extension.ExtensionFailureStrategies.fail;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class Database extends LifecycleAdapter
{
    private final Monitors parentMonitors;
    private final DependencyResolver globalDependencies;
    private final PageCache globalPageCache;

    private final Log msgLog;
    private final DatabaseLogService databaseLogService;
    private final DatabaseLogProvider internalLogProvider;
    private final DatabaseLogProvider userLogProvider;
    private final TokenNameLookup tokenNameLookup;
    private final TokenHolders tokenHolders;
    private final StatementLocksFactory statementLocksFactory;
    private final GlobalTransactionEventListeners transactionEventListeners;
    private final IdGeneratorFactory idGeneratorFactory;
    private final JobScheduler scheduler;
    private final LockService lockService;
    private final FileSystemAbstraction fs;
    private final DatabaseTransactionStats transactionStats;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final CommitProcessFactory commitProcessFactory;
    private final ConstraintSemantics constraintSemantics;
    private final GlobalProcedures globalProcedures;
    private final IOLimiter ioLimiter;
    private final SystemNanoClock clock;
    private final StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    private final CollectionsFactorySupplier collectionsFactorySupplier;
    private final Locks locks;
    private final DatabaseEventListeners eventListeners;
    private final DatabaseTracer databaseTracer;
    private final PageCursorTracerSupplier pageCursorTracerSupplier;
    private final LockTracer lockTracer;
    private final AccessCapabilityFactory accessCapabilityFactory;

    private Dependencies databaseDependencies;
    private LifeSupport life;
    private IndexProviderMap indexProviderMap;
    private DatabaseHealth databaseHealth;
    private final DatabaseAvailabilityGuard databaseAvailabilityGuard;
    private final DatabaseConfig databaseConfig;
    private final DatabaseId databaseId;
    private final DatabaseLayout databaseLayout;
    private final boolean readOnly;
    private final IdController idController;
    private final DatabaseInfo databaseInfo;
    private final VersionContextSupplier versionContextSupplier;
    private AccessCapability accessCapability;

    private final StorageEngineFactory storageEngineFactory;
    private StorageEngine storageEngine;
    private QueryExecutionEngine executionEngine;
    private DatabaseKernelModule kernelModule;
    private final Iterable<ExtensionFactory<?>> extensionFactories;
    private final Function<DatabaseLayout,DatabaseLayoutWatcher> watcherServiceFactory;
    private final Factory<DatabaseHealth> databaseHealthFactory;
    private final QueryEngineProvider engineProvider;
    private volatile boolean started;
    private Monitors databaseMonitors;
    private DatabasePageCache databasePageCache;
    private CheckpointerLifecycle checkpointerLifecycle;
    private final GraphDatabaseFacade databaseFacade;
    private final FileLockerService fileLockerService;
    private final KernelTransactionFactory kernelTransactionFactory;

    public Database( DatabaseCreationContext context )
    {
        this.databaseId = context.getDatabaseId();
        this.databaseLayout = context.getDatabaseLayout();
        this.databaseConfig = context.getDatabaseConfig();
        this.idGeneratorFactory = context.getIdGeneratorFactory();
        this.tokenNameLookup = context.getTokenNameLookup();
        this.globalDependencies = context.getGlobalDependencies();
        this.scheduler = context.getScheduler();
        this.databaseLogService = context.getDatabaseLogService();
        this.storeCopyCheckPointMutex = context.getStoreCopyCheckPointMutex();
        this.internalLogProvider = context.getDatabaseLogService().getInternalLogProvider();
        this.userLogProvider = context.getDatabaseLogService().getUserLogProvider();
        this.tokenHolders = context.getTokenHolders();
        this.locks = context.getLocks();
        this.statementLocksFactory = context.getStatementLocksFactory();
        this.transactionEventListeners = context.getTransactionEventListeners();
        this.fs = context.getFs();
        this.transactionStats = context.getTransactionStats();
        this.databaseHealthFactory = context.getDatabaseHealthFactory();
        this.transactionHeaderInformationFactory = context.getTransactionHeaderInformationFactory();
        this.constraintSemantics = context.getConstraintSemantics();
        this.parentMonitors = context.getMonitors();
        this.globalProcedures = context.getGlobalProcedures();
        this.ioLimiter = context.getIoLimiter();
        this.clock = context.getClock();
        this.eventListeners = context.getDatabaseEventListeners();
        this.accessCapabilityFactory = context.getAccessCapabilityFactory();

        this.readOnly = databaseConfig.get( read_only );
        this.idController = context.getIdController();
        this.databaseInfo = context.getDatabaseInfo();
        this.versionContextSupplier = context.getVersionContextSupplier();
        this.extensionFactories = context.getExtensionFactories();
        this.watcherServiceFactory = context.getWatcherServiceFactory();
        this.engineProvider = context.getEngineProvider();
        this.msgLog = internalLogProvider.getLog( getClass() );
        this.lockService = new ReentrantLockService();
        this.commitProcessFactory = context.getCommitProcessFactory();
        this.globalPageCache = context.getPageCache();
        this.collectionsFactorySupplier = context.getCollectionsFactorySupplier();
        this.storageEngineFactory = context.getStorageEngineFactory();
        long availabilityGuardTimeout = databaseConfig.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();
        this.databaseAvailabilityGuard = context.getDatabaseAvailabilityGuardFactory().apply( availabilityGuardTimeout );
        this.databaseFacade = new GraphDatabaseFacade( this, context.getContextBridge(), databaseConfig, databaseInfo, databaseAvailabilityGuard );
        this.kernelTransactionFactory = new FacadeKernelTransactionFactory( databaseConfig, databaseFacade );
        Tracers globalTracers = context.getTracers();
        this.databaseTracer = globalTracers.getDatabaseTracer();
        this.pageCursorTracerSupplier = globalTracers.getPageCursorTracerSupplier();
        this.lockTracer = globalTracers.getLockTracer();
        this.fileLockerService = context.getFileLockerService();
    }

    @Override
    public synchronized void start()
    {
        if ( started )
        {
            return;
        }
        try
        {
            databaseDependencies = new Dependencies( globalDependencies );
            databasePageCache = new DatabasePageCache( globalPageCache, versionContextSupplier );
            databaseMonitors = new Monitors( parentMonitors );

            life = new LifeSupport();
            life.add( new LockerLifecycleAdapter( fileLockerService.createDatabaseLocker( fs, databaseLayout ) ) );
            life.add( databaseConfig );

            databaseHealth = databaseHealthFactory.newInstance();
            accessCapability = accessCapabilityFactory.newAccessCapability( databaseConfig );
            DatabaseAvailability databaseAvailability =
                    new DatabaseAvailability( databaseAvailabilityGuard, transactionStats, clock, getAwaitActiveTransactionDeadlineMillis() );

            databaseDependencies.satisfyDependency( this );
            databaseDependencies.satisfyDependency( databaseConfig );
            databaseDependencies.satisfyDependency( databaseMonitors );
            databaseDependencies.satisfyDependency( databaseLogService );
            databaseDependencies.satisfyDependency( databasePageCache );
            databaseDependencies.satisfyDependency( tokenHolders );
            databaseDependencies.satisfyDependency( databaseFacade );
            databaseDependencies.satisfyDependency( kernelTransactionFactory );
            databaseDependencies.satisfyDependency( databaseHealth );
            databaseDependencies.satisfyDependency( storeCopyCheckPointMutex );
            databaseDependencies.satisfyDependency( transactionStats );
            databaseDependencies.satisfyDependency( locks );
            databaseDependencies.satisfyDependency( databaseAvailabilityGuard );
            databaseDependencies.satisfyDependency( databaseAvailability );
            databaseDependencies.satisfyDependency( idGeneratorFactory );
            databaseDependencies.satisfyDependency( idController );
            databaseDependencies.satisfyDependency( lockService );
            databaseDependencies.satisfyDependency( versionContextSupplier );
            databaseDependencies.satisfyDependency( new DefaultValueMapper( databaseFacade ) );
            databaseDependencies.satisfyDependency( databaseTracer );
            databaseDependencies.satisfyDependency( lockTracer );

            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
            databaseDependencies.satisfyDependency( recoveryCleanupWorkCollector );

            life.add( new PageCacheStopMetricsReporter( pageCursorTracerSupplier ) );
            life.add( new PageCacheLifecycle( databasePageCache ) );
            life.add( initializeExtensions( databaseDependencies ) );

            DatabaseLayoutWatcher watcherService = watcherServiceFactory.apply( databaseLayout );
            life.add( watcherService );
            databaseDependencies.satisfyDependency( watcherService );

            // Upgrade the store before we begin
            upgradeStore( databaseConfig, databasePageCache );

            // Check the tail of transaction logs and validate version
            final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();

            LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs ).withLogEntryReader( logEntryReader )
                                               .withConfig( databaseConfig )
                                               .withDependencies( databaseDependencies )
                                               .withLogProvider( internalLogProvider )
                                               .withDatabaseTracer( databaseTracer )
                                               .build();

            databaseMonitors.addMonitorListener( new LoggingLogFileMonitor( msgLog ) );
            databaseMonitors.addMonitorListener( new LoggingLogTailScannerMonitor( internalLogProvider.getLog( LogTailScanner.class ) ) );
            databaseMonitors.addMonitorListener(
                    new ReverseTransactionCursorLoggingMonitor( internalLogProvider.getLog( ReversedSingleFileTransactionCursor.class ) ) );
            LogTailScanner tailScanner =
                    new LogTailScanner( logFiles, logEntryReader, databaseMonitors, databaseConfig.get( fail_on_corrupted_log_files ) );
            LogVersionUpgradeChecker.check( tailScanner, databaseConfig );

            performRecovery( fs, databasePageCache, databaseConfig, databaseLayout, storageEngineFactory, internalLogProvider, databaseMonitors,
                    extensionFactories,
                    Optional.of( tailScanner ) );

            // Build all modules and their services
            DatabaseSchemaState databaseSchemaState = new DatabaseSchemaState( internalLogProvider );

            Supplier<IdController.ConditionSnapshot> transactionsSnapshotSupplier = () -> kernelModule.kernelTransactions().get();
            idController.initialize( transactionsSnapshotSupplier );

            boolean storageExists = storageEngineFactory.storageExists( fs, databaseLayout, databasePageCache );
            storageEngine = storageEngineFactory.instantiate( fs, databaseLayout, databaseConfig, databasePageCache, tokenHolders, databaseSchemaState,
                    constraintSemantics, indexProviderMap, lockService, idGeneratorFactory, idController, databaseHealth, versionContextSupplier,
                    internalLogProvider, !storageExists );

            life.add( storageEngine );
            life.add( storageEngine.schemaAndTokensLifecycle() );
            life.add( logFiles );

            // Label index
            NeoStoreIndexStoreView neoStoreIndexStoreView = new NeoStoreIndexStoreView( lockService, storageEngine::newReader );
            LabelScanStore labelScanStore =
                    buildLabelIndex( databasePageCache, recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, databaseMonitors );

            // Schema indexes
            DynamicIndexStoreView indexStoreView =
                    new DynamicIndexStoreView( neoStoreIndexStoreView, labelScanStore, lockService, storageEngine::newReader, internalLogProvider );
            IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( databasePageCache, databaseLayout, recoveryCleanupWorkCollector );
            IndexingService indexingService = buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore );

            TransactionIdStore transactionIdStore = storageEngine.transactionIdStore();
            databaseDependencies.satisfyDependency( transactionIdStore );
            databaseDependencies.satisfyDependency( storageEngine.logVersionRepository() );
            databaseDependencies.satisfyDependency( storageEngine.countsAccessor() );

            versionContextSupplier.init( transactionIdStore::getLastClosedTransactionId );

            CheckPointerImpl.ForceOperation forceOperation = new DefaultForceOperation( indexingService, labelScanStore, storageEngine );
            DatabaseTransactionLogModule transactionLogModule =
                    buildTransactionLogs( logFiles, databaseConfig, internalLogProvider, scheduler, forceOperation,
                            logEntryReader, transactionIdStore, databaseMonitors );
            transactionLogModule.satisfyDependencies( databaseDependencies );

            final DatabaseKernelModule kernelModule = buildKernel(
                    logFiles,
                    transactionLogModule.transactionAppender(),
                    indexingService,
                    databaseSchemaState,
                    labelScanStore,
                    storageEngine,
                    transactionIdStore,
                    databaseAvailabilityGuard,
                    clock,
                    indexStatisticsStore, databaseFacade );

            kernelModule.satisfyDependencies( databaseDependencies );

            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.kernelModule = kernelModule;

            databaseDependencies.satisfyDependency( databaseSchemaState );
            databaseDependencies.satisfyDependency( logEntryReader );
            databaseDependencies.satisfyDependency( storageEngine );
            databaseDependencies.satisfyDependency( labelScanStore );
            databaseDependencies.satisfyDependency( indexingService );
            databaseDependencies.satisfyDependency( indexStoreView );
            databaseDependencies.satisfyDependency( indexStatisticsStore );
            databaseDependencies.satisfyDependency( indexProviderMap );
            databaseDependencies.satisfyDependency( forceOperation );
            databaseDependencies.satisfyDependency( new DatabaseEntityCounters( this.idGeneratorFactory,
                    databaseDependencies.resolveDependency( CountsAccessor.class ) ) );

            var providerSpi = QueryEngineProvider.spi( internalLogProvider, databaseMonitors, scheduler, life, getKernel(), databaseConfig );
            this.executionEngine = QueryEngineProvider.initialize( databaseDependencies, databaseFacade, engineProvider, isSystem(), providerSpi );

            this.checkpointerLifecycle = new CheckpointerLifecycle( transactionLogModule.checkPointer(), databaseHealth );

            life.add( databaseHealth );
            life.add( databaseAvailabilityGuard );
            life.add( databaseAvailability );
            life.add( new PageCacheStartMetricsReporter( pageCursorTracerSupplier ) );
            life.setLast( checkpointerLifecycle );

            databaseDependencies.resolveDependency( DbmsDiagnosticsManager.class ).dumpDatabaseDiagnostics( this );
            life.start();
            eventListeners.databaseStart( databaseId.name() );
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while starting the database. Trying to stop already started components.", e );
            try
            {
                executeAll( () -> safeLifeShutdown( life ), () -> safeStorageEngineClose( storageEngine ) );
            }
            catch ( Exception closeException )
            {
                msgLog.error( "Couldn't close database after startup failure", closeException );
            }
            throw new RuntimeException( e );
        }
        /*
         * At this point recovery has completed and the database is ready for use. Whatever panic might have
         * happened before has been healed. So we can safely set the kernel health to ok.
         * This right now has any real effect only in the case of internal restarts (for example, after a store copy).
         * Standalone instances will have to be restarted by the user, as is proper for all database panics.
         */
        databaseHealth.healed();
        started = true;
    }

    private LifeSupport initializeExtensions( Dependencies dependencies )
    {
        LifeSupport extensionsLife = new LifeSupport();

        extensionsLife.add( new DatabaseExtensions( new DatabaseExtensionContext( databaseLayout, databaseInfo, dependencies ), extensionFactories,
                dependencies, fail() ) );

        indexProviderMap = extensionsLife.add( new DefaultIndexProviderMap( dependencies, databaseConfig ) );
        dependencies.satisfyDependency( indexProviderMap );
        extensionsLife.init();
        return extensionsLife;
    }

    private void upgradeStore( DatabaseConfig databaseConfig, DatabasePageCache databasePageCache ) throws IOException
    {
        new DatabaseMigratorFactory( fs, databaseConfig, databaseLogService, databasePageCache, scheduler, databaseId )
                .createDatabaseMigrator( databaseLayout, storageEngineFactory, databaseDependencies ).migrate();
    }

    /**
     * Builds an {@link IndexingService} and adds it to this database's {@link LifeSupport}.
     */
    private IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            DynamicIndexStoreView indexStoreView,
            IndexStatisticsStore indexStatisticsStore )
    {
        return life.add( buildIndexingService( storageEngine, databaseSchemaState, indexStoreView, indexStatisticsStore, databaseConfig, scheduler,
                indexProviderMap, tokenNameLookup, internalLogProvider, userLogProvider, databaseMonitors.newMonitor( IndexingService.Monitor.class ) ) );
    }

    /**
     * Convenience method for building am {@link IndexingService}. Doesn't add it to a {@link LifeSupport}.
     */
    public static IndexingService buildIndexingService(
            StorageEngine storageEngine,
            DatabaseSchemaState databaseSchemaState,
            DynamicIndexStoreView indexStoreView,
            IndexStatisticsStore indexStatisticsStore,
            Config config,
            JobScheduler jobScheduler,
            IndexProviderMap indexProviderMap,
            TokenNameLookup tokenNameLookup,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            IndexingService.Monitor indexingServiceMonitor )
    {
        IndexingService indexingService = IndexingServiceFactory.createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView,
                tokenNameLookup, initialSchemaRulesLoader( storageEngine ), internalLogProvider, userLogProvider, indexingServiceMonitor,
                databaseSchemaState, indexStatisticsStore );
        storageEngine.addIndexUpdateListener( indexingService );
        return indexingService;
    }

    public boolean isSystem()
    {
        return databaseId.isSystemDatabase();
    }

    /**
     * Builds a {@link LabelScanStore} and adds it to this database's {@link LifeSupport}.
     */
    private LabelScanStore buildLabelIndex(
            PageCache pageCache,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            NeoStoreIndexStoreView neoStoreIndexStoreView,
            Monitors monitors )
    {
        return life.add( buildLabelIndex( recoveryCleanupWorkCollector, storageEngine, neoStoreIndexStoreView, monitors, internalLogProvider,
                pageCache, databaseLayout, fs, readOnly ) );
    }

    /**
     * Convenience method for building a {@link LabelScanStore}. Doesn't add it to a {@link LifeSupport}.
     */
    public static LabelScanStore buildLabelIndex(
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            StorageEngine storageEngine,
            IndexStoreView indexStoreView,
            Monitors monitors,
            LogProvider logProvider,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            boolean readOnly )
    {
        monitors.addMonitorListener( new LoggingMonitor( logProvider.getLog( NativeLabelScanStore.class ) ) );
        NativeLabelScanStore labelScanStore = new NativeLabelScanStore( pageCache, databaseLayout, fs, new FullLabelStream( indexStoreView ),
                readOnly, monitors, recoveryCleanupWorkCollector );
        storageEngine.addNodeLabelUpdateListener( labelScanStore );
        return labelScanStore;
    }

    private DatabaseTransactionLogModule buildTransactionLogs( LogFiles logFiles, Config config,
            LogProvider logProvider, JobScheduler scheduler, CheckPointerImpl.ForceOperation forceOperation,
            LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader, TransactionIdStore transactionIdStore, Monitors monitors )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache();

        final LogPruning logPruning =
                new LogPruningImpl( fs, logFiles, logProvider, new LogPruneStrategyFactory(), clock, config );

        final LogRotation logRotation =
                new LogRotationImpl( logFiles, clock, databaseHealth, monitors.newMonitor( LogRotationMonitor.class ) );

        final TransactionAppender appender = life.add( new BatchingTransactionAppender(
                logFiles, logRotation, transactionMetadataCache, transactionIdStore, databaseHealth ) );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFiles, transactionMetadataCache, logEntryReader, monitors, true );

        CheckPointThreshold threshold = CheckPointThreshold.createThreshold( config, clock, logPruning, logProvider );

        final CheckPointerImpl checkPointer =
                new CheckPointerImpl( transactionIdStore, threshold, forceOperation, logPruning, appender, databaseHealth, logProvider, databaseTracer,
                        ioLimiter, storeCopyCheckPointMutex );

        long recurringPeriod = threshold.checkFrequencyMillis();
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, ioLimiter, scheduler,
                recurringPeriod, databaseHealth );

        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new DatabaseTransactionLogModule( logicalTransactionStore, logFiles, logRotation, checkPointer, appender );
    }

    private DatabaseKernelModule buildKernel( LogFiles logFiles, TransactionAppender appender,
            IndexingService indexingService, DatabaseSchemaState databaseSchemaState, LabelScanStore labelScanStore,
            StorageEngine storageEngine, TransactionIdStore transactionIdStore,
            AvailabilityGuard databaseAvailabilityGuard, SystemNanoClock clock,
            IndexStatisticsStore indexStatisticsStore, GraphDatabaseFacade facade )
    {
        AtomicReference<CpuClock> cpuClockRef = setupCpuClockAtomicReference();
        AtomicReference<HeapAllocation> heapAllocationRef = setupHeapAllocationAtomicReference();

        TransactionCommitProcess transactionCommitProcess = commitProcessFactory.create( appender, storageEngine, databaseConfig );

        /*
         * This is used by explicit indexes and constraint indexes whenever a transaction is to be spawned
         * from within an existing transaction. It smells, and we should look over alternatives when time permits.
         */
        Supplier<Kernel> kernelProvider = () -> kernelModule.kernelAPI();

        ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService, internalLogProvider );

        DatabaseTransactionEventListeners databaseTransactionEventListeners =
                new DatabaseTransactionEventListeners( facade, transactionEventListeners, databaseId );
        KernelTransactions kernelTransactions = life.add(
                new KernelTransactions( databaseConfig, statementLocksFactory, constraintIndexCreator,
                        transactionHeaderInformationFactory, transactionCommitProcess, databaseTransactionEventListeners, transactionStats,
                        databaseAvailabilityGuard,
                        storageEngine, globalProcedures, transactionIdStore, clock, cpuClockRef,
                        heapAllocationRef, accessCapability, versionContextSupplier, collectionsFactorySupplier,
                        constraintSemantics, databaseSchemaState, tokenHolders, getDatabaseId(), indexingService, labelScanStore, indexStatisticsStore,
                        databaseDependencies, databaseTracer, pageCursorTracerSupplier, lockTracer ) );

        buildTransactionMonitor( kernelTransactions, clock, databaseConfig );

        KernelImpl kernel = new KernelImpl( kernelTransactions, databaseHealth, transactionStats, globalProcedures, databaseConfig, storageEngine );

        life.add( kernel );

        final DatabaseFileListing fileListing = new DatabaseFileListing( databaseLayout, logFiles, labelScanStore, indexingService, storageEngine );
        databaseDependencies.satisfyDependency( fileListing );

        return new DatabaseKernelModule( transactionCommitProcess, kernel, kernelTransactions, fileListing );
    }

    private AtomicReference<CpuClock> setupCpuClockAtomicReference()
    {
        AtomicReference<CpuClock> cpuClock = new AtomicReference<>( CpuClock.NOT_AVAILABLE );
        SettingChangeListener<Boolean> cpuClockUpdater = ( before, after ) ->
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
        cpuClockUpdater.accept( null, databaseConfig.get( GraphDatabaseSettings.track_query_cpu_time ) );
        databaseConfig.addListener( GraphDatabaseSettings.track_query_cpu_time, cpuClockUpdater );
        return cpuClock;
    }

    private AtomicReference<HeapAllocation> setupHeapAllocationAtomicReference()
    {
        AtomicReference<HeapAllocation> heapAllocation = new AtomicReference<>( HeapAllocation.NOT_AVAILABLE );
        SettingChangeListener<Boolean> heapAllocationUpdater = ( before, after ) ->
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
        heapAllocationUpdater.accept( null, databaseConfig.get( GraphDatabaseSettings.track_query_allocation ) );
        databaseConfig.addListener( GraphDatabaseSettings.track_query_allocation, heapAllocationUpdater );
        return heapAllocation;
    }

    private void buildTransactionMonitor( KernelTransactions kernelTransactions, SystemNanoClock clock, Config config )
    {
        KernelTransactionMonitor kernelTransactionTimeoutMonitor = new KernelTransactionMonitor( kernelTransactions, clock, databaseLogService );
        databaseDependencies.satisfyDependency( kernelTransactionTimeoutMonitor );
        KernelTransactionMonitorScheduler transactionMonitorScheduler =
                new KernelTransactionMonitorScheduler( kernelTransactionTimeoutMonitor, scheduler,
                        config.get( GraphDatabaseSettings.transaction_monitor_check_interval ).toMillis() );
        life.add( transactionMonitorScheduler );
    }

    @Override
    public synchronized void stop()
    {
        if ( !started )
        {
            return;
        }

        eventListeners.databaseShutdown( databaseId.name() );
        life.stop();
        awaitAllClosingTransactions();
        life.shutdown();
        started = false;
    }

    public void prepareToDrop()
    {
        prepareStop( alwaysTrue() );
    }

    public synchronized void drop()
    {
        if ( started )
        {
            prepareStop( alwaysTrue() );
            checkpointerLifecycle.setCheckpointOnShutdown( false );
            stop();
        }
        deleteDatabaseFiles( List.of( databaseLayout.databaseDirectory(), databaseLayout.getTransactionLogsDirectory() ) );
    }

    public synchronized void truncate()
    {
        boolean truncateStartedDatabase = started;
        List<File> filesToKeep = filesToKeepOnTruncation( databaseLayout );
        File[] transactionLogs = databaseDependencies != null ? databaseDependencies.resolveDependency( LogFiles.class ).logFiles()
                                                              : new TransactionLogFilesHelper( fs, databaseLayout.getTransactionLogsDirectory() ).getLogFiles();
        if ( truncateStartedDatabase )
        {
            prepareStop( pagedFile -> !filesToKeep.contains( pagedFile.file() ) );
            stop();
        }

        List<File> filesToDelete = filesToDeleteOnTruncation( filesToKeep, databaseLayout, transactionLogs );
        deleteDatabaseFiles( filesToDelete );
        if ( truncateStartedDatabase )
        {
            start();
        }
    }

    private void deleteDatabaseFiles( List<File> files )
    {
        try
        {
            for ( File fileToDelete : files )
            {
                FileSystemUtils.deleteFile( fs, fileToDelete );
            }
        }
        catch ( IOException e )
        {
            internalLogProvider.getLog( Database.class ).error( format( "Failed to delete database '%s' files.", databaseId.name() ), e );
            throw new UncheckedIOException( e );
        }
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

    public Config getConfig()
    {
        return databaseConfig;
    }

    public DatabaseLogService getLogService()
    {
        return databaseLogService;
    }

    public DatabaseLogProvider getInternalLogProvider()
    {
        return internalLogProvider;
    }

    public StoreId getStoreId()
    {
        return storageEngine.getStoreId();
    }

    public DatabaseLayout getDatabaseLayout()
    {
        return databaseLayout;
    }

    public Monitors getMonitors()
    {
        return databaseMonitors;
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
        DatabaseFileListing.StoreFileListingBuilder fileListingBuilder = getDatabaseFileListing().builder();
        if ( !includeLogs )
        {
            fileListingBuilder.excludeLogFiles();
        }
        return fileListingBuilder.build();
    }

    public DatabaseFileListing getDatabaseFileListing()
    {
        return kernelModule.fileListing();
    }

    public Dependencies getDependencyResolver()
    {
        return databaseDependencies;
    }

    public JobScheduler getScheduler()
    {
        return scheduler;
    }

    public StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        return storeCopyCheckPointMutex;
    }

    public DatabaseId getDatabaseId()
    {
        return databaseId;
    }

    public TokenHolders getTokenHolders()
    {
        return tokenHolders;
    }

    public DatabaseAvailabilityGuard getDatabaseAvailabilityGuard()
    {
        return databaseAvailabilityGuard;
    }

    public GraphDatabaseFacade getDatabaseFacade()
    {
        return databaseFacade;
    }

    public Health getDatabaseHealth()
    {
        return databaseHealth;
    }

    public VersionContextSupplier getVersionContextSupplier()
    {
        return versionContextSupplier;
    }

    private void prepareStop( Predicate<PagedFile> deleteFilePredicate )
    {
        databasePageCache.listExistingMappings()
                .stream().filter( deleteFilePredicate )
                .forEach( file -> file.setDeleteOnClose( true ) );
    }

    private long getAwaitActiveTransactionDeadlineMillis()
    {
        return databaseConfig.get( GraphDatabaseSettings.shutdown_transaction_end_timeout ).toMillis();
    }

    @VisibleForTesting
    public LifeSupport getLife()
    {
        return life;
    }

    public static Iterable<IndexDescriptor> initialSchemaRulesLoader( StorageEngine storageEngine )
    {
        return () ->
        {
            try ( StorageReader reader = storageEngine.newReader() )
            {
                return asList( reader.indexesGetAll() ).iterator();
            }
        };
    }

    public boolean isStarted()
    {
        return started;
    }

    private static void safeStorageEngineClose( StorageEngine storageEngine )
    {
        if ( storageEngine != null )
        {
            storageEngine.forceClose();
        }
    }

    private static void safeLifeShutdown( LifeSupport life )
    {
        if ( life != null )
        {
            life.shutdown();
        }
    }
}
