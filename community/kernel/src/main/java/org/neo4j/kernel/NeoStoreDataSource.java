/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.DataIntegrityValidatingStatementOperations;
import org.neo4j.kernel.impl.api.GuardingStatementOperations;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.LockingStatementOperations;
import org.neo4j.kernel.impl.api.RecoveryLegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.SchemaStateConcern;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.RecoveryIndexingUpdatesValidator;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
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
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogFileRecoverer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
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
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointScheduler;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholds;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CountCommittedTransactionThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TimeCheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.transaction.state.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreSupplier;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextFactory;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.transaction.state.RecoveryVisitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
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
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.fromConfigValue;

public class NeoStoreDataSource implements NeoStoreSupplier, Lifecycle, IndexProviders
{
    private interface NeoStoreModule
    {
        NeoStore neoStore();
    }

    private interface CacheModule
    {
        UpdateableSchemaState updateableSchemaState();

        CacheAccessBackDoor cacheAccess();

        SchemaCache schemaCache();

        ProcedureCache procedureCache();
    }

    private interface IndexingModule
    {
        IndexingService indexingService();

        IndexUpdatesValidator indexUpdatesValidator();

        LabelScanStore labelScanStore();

        IntegrityValidator integrityValidator();

        SchemaIndexProviderMap schemaIndexProviderMap();
    }

    private interface StoreLayerModule
    {
        StoreReadLayer storeLayer();
    }

    private interface TransactionLogModule
    {
        TransactionRepresentationStoreApplier storeApplier();

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

    private enum Diagnostics implements DiagnosticsExtractor<NeoStoreDataSource>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger logger )
                    {
                        source.neoStoreModule.neoStore().logVersions( logger );
                    }
                },
        NEO_STORE_ID_USAGE( "Id usage:" )
                {
                    @Override
                    void dump( NeoStoreDataSource source, Logger logger )
                    {
                        source.neoStoreModule.neoStore().logIdUsage( logger );
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
    private final LogProvider logProvider;
    private final DependencyResolver dependencyResolver;
    private final TokenNameLookup tokenNameLookup;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokens;
    private final RelationshipTypeTokenHolder relationshipTypeTokens;
    private final Locks locks;
    private final SchemaWriteGuard schemaWriteGuard;
    private final TransactionEventHandlers transactionEventHandlers;
    private final StoreFactory storeFactory;
    private final JobScheduler scheduler;
    private final Config config;
    private final LockService lockService;
    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private final StoreUpgrader storeMigrationProcess;
    private final TransactionMonitor transactionMonitor;
    private final KernelHealth kernelHealth;
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
    private final IndexConfigStore indexConfigStore;
    private final ConstraintSemantics constraintSemantics;

    private Dependencies dependencies;
    private LifeSupport life;
    private SchemaIndexProvider indexProvider;
    private File storeDir;
    private boolean readOnly;

    private NeoStoreModule neoStoreModule;
    private CacheModule cacheModule;
    private IndexingModule indexingModule;
    private StoreLayerModule storeLayerModule;
    private TransactionLogModule transactionLogModule;
    private KernelModule kernelModule;

    /**
     * Creates a <CODE>NeoStoreXaDataSource</CODE> using configuration from
     * <CODE>params</CODE>. First the map is checked for the parameter
     * <CODE>config</CODE>.
     * If that parameter exists a config file with that value is loaded (via
     * {@link Properties#load}). Any parameter that exist in the config file
     * and in the map passed into this constructor will take the value from the
     * map.
     * <p>
     * If <CODE>config</CODE> parameter is set but file doesn't exist an
     * <CODE>IOException</CODE> is thrown. If any problem is found with that
     * configuration file or Neo4j store can't be loaded an <CODE>IOException is
     * thrown</CODE>.
     * <p>
     * Note that the tremendous number of dependencies for this class, clearly, is an architecture smell. It is part
     * of the ongoing work on introducing the Kernel API, where components that were previously spread throughout the
     * core API are now slowly accumulating in the Kernel implementation. Over time, these components should be
     * refactored into bigger components that wrap the very granular things we depend on here.
     */
    public NeoStoreDataSource( File storeDir, Config config, StoreFactory sf, LogProvider logProvider,
            JobScheduler scheduler, TokenNameLookup tokenNameLookup, DependencyResolver dependencyResolver,
            PropertyKeyTokenHolder propertyKeyTokens, LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens, Locks lockManager,
            SchemaWriteGuard schemaWriteGuard, TransactionEventHandlers transactionEventHandlers,
            IndexingService.Monitor indexingServiceMonitor, FileSystemAbstraction fs,
            StoreUpgrader storeMigrationProcess, TransactionMonitor transactionMonitor,
            KernelHealth kernelHealth, PhysicalLogFile.Monitor physicalLogMonitor,
            TransactionHeaderInformationFactory transactionHeaderInformationFactory,
            StartupStatisticsProvider startupStatistics,
            NodeManager nodeManager, Guard guard,
            IndexConfigStore indexConfigStore, CommitProcessFactory commitProcessFactory,
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
        this.logProvider = logProvider;
        this.propertyKeyTokenHolder = propertyKeyTokens;
        this.labelTokens = labelTokens;
        this.relationshipTypeTokens = relationshipTypeTokens;
        this.locks = lockManager;
        this.schemaWriteGuard = schemaWriteGuard;
        this.transactionEventHandlers = transactionEventHandlers;
        this.indexingServiceMonitor = indexingServiceMonitor;
        this.fs = fs;
        this.storeMigrationProcess = storeMigrationProcess;
        this.transactionMonitor = transactionMonitor;
        this.kernelHealth = kernelHealth;
        this.physicalLogMonitor = physicalLogMonitor;
        this.transactionHeaderInformationFactory = transactionHeaderInformationFactory;
        this.startupStatistics = startupStatistics;
        this.nodeManager = nodeManager;
        this.guard = guard;
        this.indexConfigStore = indexConfigStore;
        this.constraintSemantics = constraintSemantics;
        this.monitors = monitors;
        this.tracers = tracers;

        readOnly = config.get( Configuration.read_only );
        msgLog = logProvider.getLog( getClass() );
        this.storeFactory = sf;
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
        if ( !storeFactory.storeExists() )
        {
            storeFactory.createNeoStore().close();
        }

        indexProvider = dependencyResolver.resolveDependency( SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );

        dependencies.satisfyDependency( lockService );

        // Monitor listeners
        LoggingLogFileMonitor loggingLogMonitor = new LoggingLogFileMonitor( msgLog );
        monitors.addMonitorListener( loggingLogMonitor );
        monitors.addMonitorListener( new RecoveryVisitor.Monitor()
        {
            @Override
            public void transactionRecovered( long txId )
            {
                recoveredCount.incrementAndGet();
            }
        } );

        life.add( new Lifecycle.Delegate( Lifecycles.multiple( indexProviders.values() ) ) );

        // Upgrade the store before we begin
        upgradeStore( storeDir, storeMigrationProcess, indexProvider );

        // Build all modules and their services
        try
        {
            LegacyIndexApplierLookup legacyIndexApplierLookup =
                    dependencies.satisfyDependency( new LegacyIndexApplierLookup.Direct( legacyIndexProviderLookup ) );

            final NeoStoreModule neoStoreModule =
                    buildNeoStore( storeFactory, labelTokens, relationshipTypeTokens, propertyKeyTokenHolder );
            // TODO The only reason this is here is because of the provider-stuff for DiskLayer. Remove when possible:
            this.neoStoreModule = neoStoreModule;

            CacheModule cacheModule = buildCaches(
                    labelTokens, relationshipTypeTokens, propertyKeyTokenHolder );

            IndexingModule indexingModule = buildIndexing( config, scheduler, indexProvider, lockService,
                    tokenNameLookup, logProvider, indexingServiceMonitor,
                    neoStoreModule.neoStore(), cacheModule.updateableSchemaState() );

            StoreLayerModule storeLayerModule = buildStoreLayer( neoStoreModule.neoStore(),
                    propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                    indexingModule.indexingService(), cacheModule.schemaCache(), cacheModule.procedureCache() );

            TransactionLogModule transactionLogModule =
                    buildTransactionLogs( storeDir, config, logProvider, scheduler, indexingModule.labelScanStore(),
                            fs, neoStoreModule.neoStore(), cacheModule.cacheAccess(), indexingModule.indexingService(),
                            indexProviders.values(), legacyIndexApplierLookup );

            buildRecovery( fs, cacheModule.cacheAccess(), indexingModule.indexingService(),
                    indexingModule.labelScanStore(), neoStoreModule.neoStore(),
                    monitors.newMonitor( RecoveryVisitor.Monitor.class ), monitors.newMonitor( Recovery.Monitor.class ),
                    transactionLogModule.logFiles(), transactionLogModule.storeFlusher(), startupStatistics,
                    legacyIndexApplierLookup );

            KernelModule kernelModule = buildKernel( indexingModule.integrityValidator(),
                    transactionLogModule.transactionAppender(), neoStoreModule.neoStore(),
                    transactionLogModule.storeApplier(), indexingModule.indexingService(),
                    indexingModule.indexUpdatesValidator(),
                    storeLayerModule.storeLayer(),
                    cacheModule.updateableSchemaState(), indexingModule.labelScanStore(),
                    indexingModule.schemaIndexProviderMap(), cacheModule.procedureCache() );


            // Do these assignments last so that we can ensure no cyclical dependencies exist
            this.cacheModule = cacheModule;
            this.indexingModule = indexingModule;
            this.storeLayerModule = storeLayerModule;
            this.transactionLogModule = transactionLogModule;
            this.kernelModule = kernelModule;

            dependencies.satisfyDependency( this );
            satisfyDependencies( neoStoreModule, cacheModule, indexingModule, storeLayerModule, transactionLogModule,
                    kernelModule );
        }
        catch ( Throwable e )
        {
            // Something unexpected happened during startup
            msgLog.warn( "Exception occurred while setting up store modules. Attempting to close things down.",
                    e, true );
            try
            { // Close the neostore, so that locks are released properly
                neoStoreModule.neoStore().close();
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
            { // Close the neostore, so that locks are released properly
                neoStoreModule.neoStore().close();
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
        kernelHealth.healed();
    }

    // Startup sequence
    // By doing this sequence of method calls we can ensure that no dependency cycles exist, and get a clearer view
    // of the dependency tree, starting at the bottom
    private void upgradeStore( File storeDir, StoreUpgrader storeMigrationProcess, SchemaIndexProvider indexProvider )
    {
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( pageCache ) );
        storeMigrationProcess
                .addParticipant( indexProvider.storeMigrationParticipant( fs, pageCache, upgradableDatabase ) );
        storeMigrationProcess.migrateIfNeeded( storeDir, indexProvider );
    }

    private NeoStoreModule buildNeoStore( final StoreFactory storeFactory, final LabelTokenHolder
            labelTokens, final RelationshipTypeTokenHolder relationshipTypeTokens,
            final PropertyKeyTokenHolder propertyKeyTokenHolder )
    {
        final NeoStore neoStore = storeFactory.newNeoStore( false );

        life.add( new LifecycleAdapter()
        {
            @Override
            public void start() throws IOException
            {
                // TODO: we should not need it anymore in case if we track ids during recovery,
                // needs to be cleaned up in latest version
                if ( startupStatistics.numberOfRecoveredTransactions() > 0 )
                {
                    neoStore.rebuildIdGenerators();
                }
                neoStoreModule.neoStore().makeStoreOk();

                propertyKeyTokenHolder.setInitialTokens(
                        neoStoreModule.neoStore().getPropertyKeyTokenStore().getTokens( Integer.MAX_VALUE ) );
                relationshipTypeTokens.setInitialTokens(
                        neoStoreModule.neoStore().getRelationshipTypeTokenStore().getTokens( Integer.MAX_VALUE ) );
                labelTokens.setInitialTokens( neoStoreModule.neoStore().getLabelTokenStore().getTokens( Integer
                        .MAX_VALUE ) );

                neoStore.rebuildCountStoreIfNeeded(); // TODO: move this to lifecycle
            }
        } );

        return new NeoStoreModule()
        {
            @Override
            public NeoStore neoStore()
            {
                return neoStore;
            }
        };
    }

    private CacheModule buildCaches( LabelTokenHolder labelTokens, RelationshipTypeTokenHolder relationshipTypeTokens,
            PropertyKeyTokenHolder propertyKeyTokenHolder )
    {
        final UpdateableSchemaState updateableSchemaState = new KernelSchemaStateStore( logProvider );

        final SchemaCache schemaCache = new SchemaCache( constraintSemantics, Collections.<SchemaRule>emptyList() );

        final CacheAccessBackDoor cacheAccess = new BridgingCacheAccess( schemaCache, updateableSchemaState,
                propertyKeyTokenHolder, relationshipTypeTokens, labelTokens );

        final ProcedureCache procedureCache = new ProcedureCache();

        life.add( new LifecycleAdapter()
        {
            @Override
            public void start() throws Throwable
            {
                loadSchemaCache();
            }

            @Override
            public void stop() throws Throwable
            {
            }
        } );

        return new CacheModule()
        {
            @Override
            public SchemaCache schemaCache()
            {
                return schemaCache;
            }

            @Override
            public ProcedureCache procedureCache()
            {
                return procedureCache;
            }

            @Override
            public UpdateableSchemaState updateableSchemaState()
            {
                return updateableSchemaState;
            }

            @Override
            public CacheAccessBackDoor cacheAccess()
            {
                return cacheAccess;
            }
        };
    }

    private IndexingModule buildIndexing( Config config, JobScheduler scheduler, SchemaIndexProvider indexProvider,
            LockService lockService, TokenNameLookup tokenNameLookup,
            LogProvider logProvider, IndexingService.Monitor indexingServiceMonitor,
            NeoStore neoStore, UpdateableSchemaState updateableSchemaState )
    {
        final DefaultSchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( indexProvider );

        final IndexingService indexingService = IndexingService.create(
                new IndexSamplingConfig( config ), scheduler, providerMap,
                new NeoStoreIndexStoreView( lockService, neoStore ), tokenNameLookup, updateableSchemaState,
                toList( new SchemaStorage( neoStore.getSchemaStore() ).allIndexRules() ), logProvider,
                indexingServiceMonitor );
        final IntegrityValidator integrityValidator = new IntegrityValidator( neoStore, indexingService );

        final IndexUpdatesValidator indexUpdatesValidator = dependencies.satisfyDependency(
                new OnlineIndexUpdatesValidator( neoStore, kernelHealth, new PropertyLoader( neoStore ),
                        indexingService, IndexUpdateMode.ONLINE ) );

        // TODO Move to constructor
        final LabelScanStore labelScanStore = dependencyResolver.resolveDependency( LabelScanStoreProvider.class,
                LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore();

        life.add( indexingService );
        life.add( labelScanStore );

        return new IndexingModule()
        {
            @Override
            public IndexingService indexingService()
            {
                return indexingService;
            }

            @Override
            public IndexUpdatesValidator indexUpdatesValidator()
            {
                return indexUpdatesValidator;
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
        };
    }

    private StoreLayerModule buildStoreLayer( NeoStore neoStore,
            PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            IndexingService indexingService,
            SchemaCache schemaCache,
            ProcedureCache procedureCache )
    {
        SchemaStorage schemaStorage = new SchemaStorage( neoStore.getSchemaStore() );
        DiskLayer diskLayer = new DiskLayer( propertyKeyTokenHolder, labelTokens, relationshipTypeTokens, schemaStorage,
                neoStore, indexingService );
        final StoreReadLayer storeLayer = new CacheLayer( diskLayer, schemaCache, procedureCache );

        return new StoreLayerModule()
        {
            @Override
            public StoreReadLayer storeLayer()
            {
                return storeLayer;
            }
        };
    }

    private TransactionLogModule buildTransactionLogs( File storeDir, Config config, LogProvider logProvider,
            JobScheduler scheduler,
            LabelScanStore labelScanStore,
            FileSystemAbstraction fileSystemAbstraction,
            NeoStore neoStore, CacheAccessBackDoor cacheAccess,
            IndexingService indexingService,
            Iterable<IndexImplementation> indexProviders,
            LegacyIndexApplierLookup legacyIndexApplierLookup )
    {
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 1000, 100_000 );
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, PhysicalLogFile.DEFAULT_NAME,
                fileSystemAbstraction );

        final IdOrderingQueue legacyIndexTransactionOrdering = new SynchronizedArrayIdOrderingQueue( 20 );
        final TransactionRepresentationStoreApplier storeApplier = dependencies.satisfyDependency(
                new TransactionRepresentationStoreApplier( indexingService, alwaysCreateNewWriter( labelScanStore ),
                        neoStore, cacheAccess, lockService, legacyIndexApplierLookup, indexConfigStore, kernelHealth,
                        legacyIndexTransactionOrdering ) );

        final PhysicalLogFile logFile = new PhysicalLogFile( fileSystemAbstraction, logFiles,
                config.get( GraphDatabaseSettings.logical_log_rotation_threshold ), neoStore,
                neoStore, physicalLogMonitor, transactionMetadataCache );

        final PhysicalLogFileInformation.LogVersionToTimestamp
                logInformation = new PhysicalLogFileInformation.LogVersionToTimestamp()
        {
            @Override
            public long getTimestampForVersion( long version ) throws IOException
            {
                LogPosition position = LogPosition.start( version );
                try ( ReadableVersionableLogChannel channel = logFile.getReader( position ) )
                {
                    final LogEntryReader<ReadableVersionableLogChannel> reader = new VersionAwareLogEntryReader<>();
                    LogEntry entry;
                    while ( (entry = reader.readLogEntry( channel )) != null )
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
                new PhysicalLogFileInformation( logFiles, transactionMetadataCache, neoStore, logInformation );

        String pruningConf = config.get(
                config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral )
                ? GraphDatabaseFacadeFactory.Configuration.ephemeral_keep_logical_logs
                : GraphDatabaseSettings.keep_logical_logs );

        LogPruneStrategy logPruneStrategy = fromConfigValue( fs, logFileInformation, logFiles, pruningConf );

        final LogPruning logPruning = new LogPruningImpl( logPruneStrategy, logProvider );

        final StoreFlusher storeFlusher = new StoreFlusher( neoStore, indexingService, labelScanStore,
                indexProviders );

        final LogRotation logRotation = new LogRotationImpl( monitors.newMonitor( LogRotation.Monitor.class ),
                logFile, kernelHealth, logProvider );

        final TransactionAppender appender = new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, neoStore, legacyIndexTransactionOrdering, kernelHealth );
        final LogicalTransactionStore logicalTransactionStore =
                new PhysicalLogicalTransactionStore( logFile, transactionMetadataCache );

        int txThreshold = config.get( GraphDatabaseSettings.check_point_interval_tx );
        final CountCommittedTransactionThreshold countCommittedTransactionThreshold =
                new CountCommittedTransactionThreshold( txThreshold );

        long timeMillisThreshold = config.get( GraphDatabaseSettings.check_point_interval_time );
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold( timeMillisThreshold, Clock.SYSTEM_CLOCK );

        CheckPointThreshold threshold =
                CheckPointThresholds.or( countCommittedTransactionThreshold, timeCheckPointThreshold );

        final CheckPointerImpl checkPointer = new CheckPointerImpl( neoStore, threshold, storeFlusher, logPruning,
                appender, kernelHealth, logProvider, tracers.checkPointTracer );

        long recurringPeriod = Math.min( timeMillisThreshold, TimeUnit.SECONDS.toMillis( 10 ) );
        CheckPointScheduler checkPointScheduler = new CheckPointScheduler( checkPointer, scheduler, recurringPeriod );

        life.add( logFile );
        life.add( appender );
        life.add( checkPointer );
        life.add( checkPointScheduler );

        return new TransactionLogModule()
        {
            @Override
            public TransactionRepresentationStoreApplier storeApplier()
            {
                return storeApplier;
            }

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

    private Provider<LabelScanWriter> alwaysCreateNewWriter( final LabelScanStore labelScanStore )
    {
        return new Provider<LabelScanWriter>()
        {
            @Override
            public LabelScanWriter instance()
            {
                return labelScanStore.newWriter();
            }
        };
    }

    private void buildRecovery( final FileSystemAbstraction fileSystemAbstraction, CacheAccessBackDoor cacheAccess,
            IndexingService indexingService, LabelScanStore labelScanStore,
            final NeoStore neoStore, RecoveryVisitor.Monitor recoveryVisitorMonitor, Recovery.Monitor recoveryMonitor,
            final PhysicalLogFiles logFiles, final StoreFlusher storeFlusher,
            final StartupStatisticsProvider startupStatistics,
            LegacyIndexApplierLookup legacyIndexApplierLookup )
    {
        final RecoveryLabelScanWriterProvider labelScanWriters =
                new RecoveryLabelScanWriterProvider( labelScanStore, 1000 );
        final RecoveryLegacyIndexApplierLookup recoveryLegacyIndexApplierLookup = new RecoveryLegacyIndexApplierLookup(
                legacyIndexApplierLookup, 1000 );
        final RecoveryIndexingUpdatesValidator indexUpdatesValidator = new RecoveryIndexingUpdatesValidator( indexingService );
        final TransactionRepresentationStoreApplier storeRecoverer =
                new TransactionRepresentationStoreApplier( indexingService, labelScanWriters, neoStore, cacheAccess,
                        lockService, legacyIndexApplierLookup, indexConfigStore, kernelHealth, IdOrderingQueue.BYPASS );

        RecoveryVisitor recoveryVisitor =
                new RecoveryVisitor( neoStore, storeRecoverer, indexUpdatesValidator, recoveryVisitorMonitor );

        LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        final Visitor<LogVersionedStoreChannel,IOException> logFileRecoverer =
                new LogFileRecoverer( logEntryReader, recoveryVisitor );

        final LatestCheckPointFinder checkPointFinder =
                new LatestCheckPointFinder( logFiles, fileSystemAbstraction, logEntryReader );

        Recovery.SPI spi = new DefaultRecoverySPI( labelScanWriters, recoveryLegacyIndexApplierLookup,
                storeFlusher, logFileRecoverer, logFiles, fileSystemAbstraction, neoStore, checkPointFinder,
                indexUpdatesValidator );

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

    private KernelModule buildKernel( IntegrityValidator integrityValidator, TransactionAppender appender,
            NeoStore neoStore, TransactionRepresentationStoreApplier storeApplier, IndexingService indexingService,
            IndexUpdatesValidator indexUpdatesValidator, StoreReadLayer storeLayer,
            UpdateableSchemaState updateableSchemaState, LabelScanStore labelScanStore,
            SchemaIndexProviderMap schemaIndexProviderMap, ProcedureCache procedureCache )
    {
        final TransactionCommitProcess transactionCommitProcess =
                commitProcessFactory.create( appender, kernelHealth, neoStore, storeApplier,
                        new NeoStoreInjectedTransactionValidator( integrityValidator ), indexUpdatesValidator,
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

        LegacyIndexStore legacyIndexStore = new LegacyIndexStore( config, indexConfigStore, kernelProvider,
                legacyIndexProviderLookup );

        LegacyPropertyTrackers legacyPropertyTrackers = new LegacyPropertyTrackers( propertyKeyTokenHolder,
                nodeManager.getNodePropertyTrackers(), nodeManager.getRelationshipPropertyTrackers(), nodeManager );
        NeoStoreTransactionContextFactory neoStoreTxContextFactory = new NeoStoreTransactionContextFactory( neoStore );

        StatementOperationParts statementOperations = dependencies.satisfyDependency( buildStatementOperations(
                storeLayer, legacyPropertyTrackers, constraintIndexCreator, updateableSchemaState, guard,
                legacyIndexStore ) );

        final TransactionHooks hooks = new TransactionHooks();
        final KernelTransactions kernelTransactions =
                life.add( new KernelTransactions( neoStoreTxContextFactory,
                        neoStore, locks, integrityValidator, constraintIndexCreator, indexingService, labelScanStore,
                        statementOperations, updateableSchemaState, schemaWriteGuard, schemaIndexProviderMap,
                        transactionHeaderInformationFactory, storeLayer, transactionCommitProcess,
                        indexConfigStore, legacyIndexProviderLookup, hooks, constraintSemantics,
                        transactionMonitor, life, procedureCache, tracers ) );

        final Kernel kernel = new Kernel( kernelTransactions, hooks, kernelHealth, transactionMonitor );

        kernel.registerTransactionHook( transactionEventHandlers );

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

    // We do this last to ensure noone is cheating with dependency access
    private void satisfyDependencies( Object... modules )
    {
        for ( Object module : modules )
        {
            for ( Method method : module.getClass().getMethods() )
            {
                if ( !method.getDeclaringClass().equals( Object.class ) )
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

    // Startup sequence done
    private void loadSchemaCache()
    {
        List<SchemaRule> schemaRules = toList( neoStoreModule.neoStore().getSchemaStore().loadAllSchemaRules() );
        cacheModule.schemaCache().load( schemaRules );
    }

    public NeoStore getNeoStore()
    {
        return neoStoreModule.neoStore();
    }

    public IndexingService getIndexService()
    {
        return indexingModule.indexingService();
    }

    public SchemaIndexProvider getIndexProvider()
    {
        return indexProvider;
    }

    public LabelScanStore getLabelScanStore()
    {
        return indexingModule.labelScanStore();
    }

    @Override
    public synchronized void stop()
    {
        if ( !life.isRunning() )
        {
            return;
        }

        StoreFlusher storeFlusher = transactionLogModule.storeFlusher();
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

            // Force all pending store changes to disk.
            storeFlusher.forceEverything();

            //Write new checkpoint in the log only if the kernel is healthy.
            // We cannot throw here since we need to shutdown without exceptions.
            if ( kernelHealth.isHealthy() )
            {
                try
                {
                    checkPointer.forceCheckPoint();
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( e );
                }
            }

            // Shut down all services in here, effectively making the database unusable for anyone who tries.
            life.shutdown();

            // Close the NeoStore
            neoStoreModule.neoStore().close();
            msgLog.info( "NeoStore closed" );
        }
        // After we've released the logFile monitor there might be transactions that wants to commit, but had
        // to wait for the logFile monitor until now. When they finally get it and try to commit they will
        // fail since the logFile no longer works.
    }

    private void awaitAllTransactionsClosed()
    {
        while ( !neoStoreModule.neoStore().closedTransactionIdIsOnParWithOpenedTransactionId() )
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
        return getNeoStore().getStoreId();
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    public long getCreationTime()
    {
        return getNeoStore().getCreationTime();
    }

    public long getRandomIdentifier()
    {
        return getNeoStore().getRandomNumber();
    }

    public long getCurrentLogVersion()
    {
        return getNeoStore().getCurrentLogVersion();
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
    public NeoStore get()
    {
        return neoStoreModule.neoStore();
    }

    public StoreReadLayer getStoreLayer()
    {
        return storeLayerModule.storeLayer();
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
     * Hook that must be called whenever there is an HA mode switch (eg master/slave switch).
     * This must only be called when the database is otherwise inaccessible.
     */
    public void afterModeSwitch()
    {
        loadSchemaCache();

        // Stop all running transactions and get rid of all pooled transactions, as they will otherwise reference
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
