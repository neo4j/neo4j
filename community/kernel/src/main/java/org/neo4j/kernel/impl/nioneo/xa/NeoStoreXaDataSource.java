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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.ConstraintEnforcingEntityOperations;
import org.neo4j.kernel.impl.api.DataIntegrityValidatingStatementOperations;
import org.neo4j.kernel.impl.api.GuardingStatementOperations;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.LegacyIndexApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplier.ProviderLookup;
import org.neo4j.kernel.impl.api.LegacyPropertyTrackers;
import org.neo4j.kernel.impl.api.LockingStatementOperations;
import org.neo4j.kernel.impl.api.SchemaStateConcern;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StateHandlingStatementOperations;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.statistics.StatisticsService;
import org.neo4j.kernel.impl.api.statistics.StatisticsServiceRepository;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipLoader;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStorage;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogFileInformation;
import org.neo4j.kernel.impl.transaction.xaframework.LogPosition;
import org.neo4j.kernel.impl.transaction.xaframework.LogRotationControl;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFileInformation;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategy;
import org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.DEFAULT_HIGH_ID_TRACKING;
import static org.neo4j.kernel.impl.nioneo.xa.CacheLoaders.nodeLoader;
import static org.neo4j.kernel.impl.nioneo.xa.CacheLoaders.relationshipLoader;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogHeaderParser.LOG_HEADER_SIZE;

public class NeoStoreXaDataSource implements NeoStoreProvider, Lifecycle, LogRotationControl, IndexProviders
{
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";
    private KernelTransactions kernelTransactions;

    @SuppressWarnings( "deprecation" )
    public static abstract class Configuration
    {
        public static final Setting<String> keep_logical_logs = GraphDatabaseSettings.keep_logical_logs;
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;
        public static final Setting<File> neo_store = InternalAbstractGraphDatabase.Configuration.neo_store;
    }

    private final Dependencies dependencies = new Dependencies();
    private final StringLogger msgLog;
    private final Logging logging;
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
    private final UpdateableSchemaState updateableSchemaState;
    private final Config config;
    private final LockService lockService;

    private final IndexingService.Monitor indexingServiceMonitor;
    private final FileSystemAbstraction fs;
    private final StoreUpgrader storeMigrationProcess;
    private final TransactionMonitor transactionMonitor;
    private final KernelHealth kernelHealth;
    private final TxIdGenerator txIdGenerator;
    private final TransactionHeaderInformationFactory transactionHeaderInformationFactory;
    private final StartupStatisticsProvider startupStatistics;
    private final Caches cacheProvider;
    private final NodeManager nodeManager;
    private final CommitProcessFactory commitProcessFactory;

    private LifeSupport life;
    private KernelAPI kernel;
    private NeoStore neoStore;
    private IndexingService indexingService;
    private SchemaIndexProvider indexProvider;
    private NeoStoreFileListing fileListing;
    private File storeDir;
    private boolean readOnly;
    private AutoLoadingCache<NodeImpl> nodeCache;
    private AutoLoadingCache<RelationshipImpl> relationshipCache;
    private SchemaCache schemaCache;
    private LabelScanStore labelScanStore;

    private StoreReadLayer storeLayer;
    private LogFile logFile;

    private final AtomicInteger recoveredCount = new AtomicInteger();
    private final Guard guard;

    // Legacy index
    private IndexConfigStore indexConfigStore;
    private final Map<String, IndexImplementation> indexProviders = new HashMap<>();
    private final ProviderLookup legacyIndexProviderLookup;

    private enum Diagnostics implements DiagnosticsExtractor<NeoStoreXaDataSource>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logVersions( log );
            }
        },
        NEO_STORE_ID_USAGE( "Id usage:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logIdUsage( log );
            }
        },
        PERSISTENCE_WINDOW_POOL_STATS( "Persistence Window Pool stats:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                // TODO
//                source.neoStore.logAllWindowPoolStats( log );
            }

            @Override
            boolean applicable( DiagnosticsPhase phase )
            {
                return phase.isExplicitlyRequested();
            }
        };
        private final String message;

        private Diagnostics( String message )
        {
            this.message = message;
        }

        @Override
        public void dumpDiagnostics( final NeoStoreXaDataSource source, DiagnosticsPhase phase, StringLogger log )
        {
            if ( applicable( phase ) )
            {
                log.logLongMessage( message, new Visitor<StringLogger.LineLogger, RuntimeException>()
                {
                    @Override
                    public boolean visit( StringLogger.LineLogger logger )
                    {
                        dump( source, logger );
                        return false;
                    }
                }, true );
            }
        }

        boolean applicable( DiagnosticsPhase phase )
        {
            return phase.isInitialization() || phase.isExplicitlyRequested();
        }

        abstract void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log );
    }

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
     *
     * Note that the tremendous number of dependencies for this class, clearly, is an architecture smell. It is part
     * of the ongoing work on introducing the Kernel API, where components that were previously spread throughout the
     * core API are now slowly accumulating in the Kernel implementation. Over time, these components should be
     * refactored into bigger components that wrap the very granular things we depend on here.
     */
    public NeoStoreXaDataSource( Config config, StoreFactory sf, StringLogger stringLogger, JobScheduler scheduler,
                                 Logging logging, UpdateableSchemaState updateableSchemaState,
                                 TokenNameLookup tokenNameLookup, DependencyResolver dependencyResolver,
                                 PropertyKeyTokenHolder propertyKeyTokens, LabelTokenHolder labelTokens,
                                 RelationshipTypeTokenHolder relationshipTypeTokens, Locks lockManager,
                                 SchemaWriteGuard schemaWriteGuard, TransactionEventHandlers transactionEventHandlers,
                                 IndexingService.Monitor indexingServiceMonitor, FileSystemAbstraction fs,
                                 StoreUpgrader storeMigrationProcess, TransactionMonitor transactionMonitor,
                                 KernelHealth kernelHealth, TxIdGenerator txIdGenerator,
                                 TransactionHeaderInformationFactory transactionHeaderInformationFactory,
                                 StartupStatisticsProvider startupStatistics,
                                 Caches cacheProvider, NodeManager nodeManager, Guard guard,
                                 IndexConfigStore indexConfigStore, CommitProcessFactory commitProcessFactory )
    {
        this.config = config;
        this.tokenNameLookup = tokenNameLookup;
        this.dependencyResolver = dependencyResolver;
        this.scheduler = scheduler;
        this.logging = logging;
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
        this.txIdGenerator = txIdGenerator;
        this.transactionHeaderInformationFactory = transactionHeaderInformationFactory;
        this.startupStatistics = startupStatistics;
        this.cacheProvider = cacheProvider;
        this.nodeManager = nodeManager;
        this.guard = guard;
        this.indexConfigStore = indexConfigStore;

        readOnly = config.get( Configuration.read_only );
        msgLog = stringLogger;
        this.storeFactory = sf;
        this.updateableSchemaState = updateableSchemaState;
        this.lockService = new ReentrantLockService();
        this.legacyIndexProviderLookup = new LegacyIndexApplier.ProviderLookup()
        {
            @Override
            public IndexImplementation lookup( String name )
            {
                assert name != null : "Null provider name supplied";
                IndexImplementation provider = indexProviders.get( name );
                if ( provider == null )
                {
                    throw new IllegalArgumentException( "No index provider '" + name +
                            "' found. Maybe the intended provider (or one more of its dependencies) " +
                            "aren't on the classpath or it failed to load." );
                }
                return provider;
            }

            @Override
            public Iterable<IndexImplementation> providers()
            {
                return indexProviders.values();
            }
        };

        this.commitProcessFactory = commitProcessFactory;
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
        life = new LifeSupport();
        readOnly = config.get( Configuration.read_only );
        storeDir = config.get( Configuration.store_dir );
        File store = config.get( Configuration.neo_store );
        if ( !storeFactory.storeExists() )
        {
            storeFactory.createNeoStore().close();
        }
        indexProvider = dependencyResolver.resolveDependency( SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );
        storeMigrationProcess.addParticipant( indexProvider.storeMigrationParticipant() );
        // TODO: Build a real provider map
        final DefaultSchemaIndexProviderMap providerMap = new DefaultSchemaIndexProviderMap( indexProvider );
        storeMigrationProcess.migrateIfNeeded( store.getParentFile() );
        neoStore = dependencies.satisfyDependency( storeFactory.newNeoStore( false ) );
        dependencies.satisfyDependency( TransactionIdStore.class, neoStore );

        schemaCache = new SchemaCache( Collections.<SchemaRule>emptyList() );

        nodeCache = new AutoLoadingCache<>( cacheProvider.node(),
                nodeLoader( neoStore.getNodeStore() ) );
        relationshipCache = new AutoLoadingCache<>( cacheProvider.relationship(),
                relationshipLoader( neoStore.getRelationshipStore() ) );
        RelationshipLoader relationshipLoader = new RelationshipLoader( relationshipCache, new RelationshipChainLoader(
                neoStore ) );
        PersistenceCache persistenceCache = new PersistenceCache( nodeCache, relationshipCache, nodeManager,
                relationshipLoader, propertyKeyTokenHolder, relationshipTypeTokens, labelTokens );
        CacheAccessBackDoor cacheAccess = new BridgingCacheAccess( schemaCache, updateableSchemaState,
                persistenceCache );
        try
        {
            indexingService = new IndexingService( scheduler, providerMap, new NeoStoreIndexStoreView(
                    lockService, neoStore ), tokenNameLookup, updateableSchemaState, indexRuleLoader(), logging,
                    indexingServiceMonitor ); // TODO 2.2-future What index rules should be
            final IntegrityValidator integrityValidator = new IntegrityValidator( neoStore, indexingService );
            labelScanStore = dependencyResolver.resolveDependency( LabelScanStoreProvider.class,
                    LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore();
            fileListing = new NeoStoreFileListing( storeDir, labelScanStore, indexingService,
                    legacyIndexProviderLookup );
            Provider<NeoStore> neoStoreProvider = new Provider<NeoStore>()
            {
                @Override
                public NeoStore instance()
                {
                    return getNeoStore();
                }
            };

            if(config.get( GraphDatabaseSettings.cache_type ).equals( CacheLayer.EXPERIMENTAL_OFF ))
            {
                storeLayer = new DiskLayer( propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                        new SchemaStorage( neoStore.getSchemaStore() ), neoStoreProvider, indexingService );
            }
            else
            {
                storeLayer = new CacheLayer( new DiskLayer( propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                        new SchemaStorage( neoStore.getSchemaStore() ), neoStoreProvider, indexingService ),
                        persistenceCache, indexingService, schemaCache
                );
            }

            LegacyPropertyTrackers legacyPropertyTrackers = new LegacyPropertyTrackers( propertyKeyTokenHolder,
                    nodeManager.getNodePropertyTrackers(), nodeManager.getRelationshipPropertyTrackers(), nodeManager );
            StatisticsService statisticsService =
                    new StatisticsServiceRepository( fs, config, storeLayer, scheduler ).loadStatistics();
            final NeoStoreTransactionContextSupplier neoStoreTransactionContextSupplier =
                    new NeoStoreTransactionContextSupplier( neoStore );

            final TransactionHooks hooks = new TransactionHooks();
            File directory = config.get( GraphDatabaseSettings.store_dir );
            TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 1000, 100_000 );
            PhysicalLogFiles logFiles = new PhysicalLogFiles( directory, PhysicalLogFile.DEFAULT_NAME, fs );

            LogFileInformation logFileInformation = dependencies.satisfyDependency( LogFileInformation.class,
                    new PhysicalLogFileInformation( logFiles, transactionMetadataCache, neoStore,
                            new PhysicalLogFileInformation.SPI()
                            {
                                @Override
                                public long getTimestampForVersion( long version ) throws IOException
                                {
                                    LogPosition position = new LogPosition( version, LOG_HEADER_SIZE );
                                    try ( ReadableLogChannel channel = logFile.getReader( position ) )
                                    {
                                        LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader();
                                        LogEntry entry;
                                        while ( (entry = reader.readLogEntry( channel )) != null )
                                        {
                                            if ( entry instanceof LogEntryStart )
                                            {
                                                return ((LogEntryStart) entry).getTimeWritten();
                                            }
                                        }
                                    }
                                    return -1;
                                }
                            }) );
            
            LogPruneStrategy logPruneStrategy = LogPruneStrategyFactory.fromConfigValue( fs, logFileInformation,
                    logFiles, neoStore, config.get( GraphDatabaseSettings.keep_logical_logs ) );

            final TransactionRepresentationStoreApplier storeApplier = dependencies.satisfyDependency(
                    new TransactionRepresentationStoreApplier(
                            indexingService, labelScanStore, neoStore,
                            cacheAccess, lockService, legacyIndexProviderLookup, indexConfigStore,
                            DEFAULT_HIGH_ID_TRACKING ) );

            LoggingLogFileMonitor logMonitor = new LoggingLogFileMonitor( logging.getMessagesLog( getClass() ) );
            RecoveryVisitor recoveryVisitor = new RecoveryVisitor( neoStore,
                    storeApplier, recoveredCount, logMonitor );
            Visitor<ReadableLogChannel, IOException> logFileRecoverer =
                    new LogFileRecoverer( new VersionAwareLogEntryReader(), recoveryVisitor );
            logFile = dependencies.satisfyDependency( new PhysicalLogFile( fs, logFiles,
                    config.get( GraphDatabaseSettings.logical_log_rotation_threshold ), logPruneStrategy, neoStore,
                    neoStore, logMonitor, this, transactionMetadataCache, logFileRecoverer ) );

            final LogicalTransactionStore logicalTransactionStore = dependencies.satisfyDependency(
                    LogicalTransactionStore.class, new PhysicalLogicalTransactionStore( logFile, txIdGenerator,
                            transactionMetadataCache, neoStore, config.get( GraphDatabaseSettings.batched_writes ) ) );

            TransactionCommitProcess transactionCommitProcess = dependencies.satisfyDependency( TransactionCommitProcess.class,
                                        commitProcessFactory.create( logicalTransactionStore, kernelHealth,
                                                neoStore, storeApplier,
                                                new NeoStoreInjectedTransactionValidator( integrityValidator ), false ));

            /*
             * This is used by legacy indexes and constraint indexes whenever a transaction is to be spawned
             * from within an existing transaction. It smells, and we should look over alternatives when time permits.
             */
            Provider<KernelAPI> kernelProvider = new Provider<KernelAPI>()
            {
                @Override
                public KernelAPI instance()
                {
                    return kernel;
                }
            };

            ConstraintIndexCreator constraintIndexCreator = new ConstraintIndexCreator( kernelProvider, indexingService);

            LegacyIndexStore legacyIndexStore = new LegacyIndexStore( config, indexConfigStore, kernelProvider,
                    legacyIndexProviderLookup );

            StatementOperationParts statementOperations = buildStatementOperations( storeLayer, legacyPropertyTrackers,
                    constraintIndexCreator, updateableSchemaState, guard, legacyIndexStore );

            kernelTransactions = life.add(new KernelTransactions( neoStoreTransactionContextSupplier,
                    neoStore, locks, integrityValidator, constraintIndexCreator, indexingService, labelScanStore,
                    statementOperations, updateableSchemaState, schemaWriteGuard, providerMap,
                    transactionHeaderInformationFactory, persistenceCache, storeLayer, transactionCommitProcess, indexConfigStore,
                    legacyIndexProviderLookup, hooks, transactionMonitor, life, readOnly ));

            kernel = new Kernel( statisticsService, kernelTransactions, hooks, kernelHealth, transactionMonitor );

            life.add( logFile );
            life.add( logicalTransactionStore );
            life.add( new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    startupStatistics.setNumberOfRecoveredTransactions( recoveredCount.get() );
                    recoveredCount.set( 0 );
                    loadSchemaCache();
                }
            } );
            life.add( statisticsService );
            life.add( new LifecycleAdapter()
            {
                @Override
                public void start()
                {
                    neoStore.makeStoreOk();
                }
            } );
            life.add( indexingService );
            life.add( labelScanStore );

            kernel.registerTransactionHook( transactionEventHandlers );
            neoStore.setRecoveredStatus( true );
            try
            {
                life.start();
            }
            finally
            {
                neoStore.setRecoveredStatus( false );
            }

            propertyKeyTokenHolder.addTokens( neoStore.getPropertyKeyTokenStore().getTokens( Integer.MAX_VALUE ) );
            relationshipTypeTokens.addTokens( neoStore.getRelationshipTypeTokenStore().getTokens( Integer.MAX_VALUE ) );
            labelTokens.addTokens( neoStore.getLabelTokenStore().getTokens( Integer.MAX_VALUE ) );
        }
        catch ( Throwable e )
        { // Something unexpected happened during startup
            try
            { // Close the neostore, so that locks are released properly
                neoStore.close();
            }
            catch ( Exception closeException )
            {
                msgLog.logMessage( "Couldn't close neostore after startup failure" );
            }
            throw Exceptions.launderedException( e );
        }
    }

    private void loadSchemaCache()
    {
        schemaCache.clear();
        for ( SchemaRule schemaRule : loop( neoStore.getSchemaStore().loadAllSchemaRules() ) )
        {
            schemaCache.addSchemaRule( schemaRule );
        }
    }

    private Iterable<IndexRule> indexRuleLoader()
    {
        return new Iterable<IndexRule>()
        {
            @Override
            public Iterator<IndexRule> iterator()
            {
                return new SchemaStorage( neoStore.getSchemaStore() ).allIndexRules();
            }
        };
    }

    // TODO 2.2-future: In TransactionFactory (now gone) was (#onRecoveryComplete)
    //    forceEverything();
    //    neoStore.makeStoreOk();
    //    neoStore.setVersion( xaContainer.getLogicalLog().getHighestLogVersion() );
    public NeoStore getNeoStore()
    {
        return neoStore;
    }

    public IndexingService getIndexService()
    {
        return indexingService;
    }

    public SchemaIndexProvider getIndexProvider()
    {
        return indexProvider;
    }

    public LabelScanStore getLabelScanStore()
    {
        return labelScanStore;
    }

    public LockService getLockService()
    {
        return lockService;
    }

    @Override
    public void stop()
    {
        if ( !readOnly )
        {
            forceEverything();
        }
        life.shutdown();
        neoStore.close();
        msgLog.info( "NeoStore closed" );
    }

    @Override
    public void forceEverything()
    {
        indexingService.flushAll();
        labelScanStore.force();
        for ( IndexImplementation index : indexProviders.values() )
        {
            index.force();
        }
        neoStore.flush();
    }

    @Override
    public void shutdown()
    { // We do our own internal life management:
      // start() does life.init() and life.start(),
      // stop() does life.stop() and life.shutdown().
    }

    public StoreId getStoreId()
    {
        return neoStore.getStoreId();
    }

    public String getStoreDir()
    {
        return storeDir.getPath();
    }

    public long getCreationTime()
    {
        return neoStore.getCreationTime();
    }

    public long getRandomIdentifier()
    {
        return neoStore.getRandomNumber();
    }

    public long getCurrentLogVersion()
    {
        return neoStore.getCurrentLogVersion();
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public KernelAPI getKernel()
    {
        return kernel;
    }

    public boolean setRecovered( boolean recovered )
    {
        boolean currentValue = neoStore.isInRecoveryMode();
        neoStore.setRecoveredStatus( true );
        return currentValue;
    }

    public ResourceIterator<File> listStoreFiles() throws IOException
    {
        return fileListing.listStoreFiles();
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        manager.registerAll( Diagnostics.class, this );
    }

    @Override
    public NeoStore evaluate()
    {
        return neoStore;
    }

    public StoreReadLayer getStoreLayer()
    {
        return storeLayer;
    }

    @Override
    public void awaitAllTransactionsClosed()
    {
        // TODO 2.2-future what if this will never happen?
        while ( !neoStore.closedTransactionIdIsOnParWithCommittedTransactionId() )
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                break;
            }
        }
    }

    public Cache<NodeImpl> getNodeCache()
    {
        return nodeCache;
    }

    public Cache<RelationshipImpl> getRelationshipCache()
    {
        return relationshipCache;
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
        // Bottom layer: Read-access to committed data
        StoreReadLayer storeLayer = storeReadLayer;
        // + Transaction state handling
        StateHandlingStatementOperations stateHandlingContext = new StateHandlingStatementOperations( storeLayer,
                legacyPropertyTrackers, constraintIndexCreator,
                legacyIndexStore );
        StatementOperationParts parts = new StatementOperationParts( stateHandlingContext, stateHandlingContext,
                stateHandlingContext, stateHandlingContext, stateHandlingContext, stateHandlingContext,
                new SchemaStateConcern( updateableSchemaState ), null, stateHandlingContext, stateHandlingContext );
        // + Constraints
        ConstraintEnforcingEntityOperations constraintEnforcingEntityOperations = new ConstraintEnforcingEntityOperations(
                parts.entityWriteOperations(), parts.entityReadOperations(), parts.schemaReadOperations() );
        // + Data integrity
        DataIntegrityValidatingStatementOperations dataIntegrityContext = new DataIntegrityValidatingStatementOperations(
                parts.keyWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations() );
        parts = parts.override( null, dataIntegrityContext, constraintEnforcingEntityOperations,
                constraintEnforcingEntityOperations, null, dataIntegrityContext, null, null, null, null );
        // + Locking
        LockingStatementOperations lockingContext = new LockingStatementOperations( parts.entityReadOperations(),
                parts.entityWriteOperations(), parts.schemaReadOperations(), parts.schemaWriteOperations(),
                parts.schemaStateOperations() );
        parts = parts.override( null, null, null, lockingContext, lockingContext, lockingContext, lockingContext,
                lockingContext, null, null );
        // + Guard
        if ( guard != null )
        {
            GuardingStatementOperations guardingOperations = new GuardingStatementOperations(
                    parts.entityWriteOperations(), parts.entityReadOperations(), guard );
            parts = parts.override( null, null, guardingOperations, guardingOperations, null, null, null, null, null, null );
        }

        return parts;
    }

    @Override
    public void registerIndexProvider( String name, IndexImplementation index )
    {
        // TODO 2.2-future why did i need to remove the next line to get HA working? It said it doubled registered
        // TODO 2.2-future the dummy index extension, which i am not sure is true
//        assert !indexProviders.containsKey( name );
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
        kernelTransactions.disposeAll();
    }
}
