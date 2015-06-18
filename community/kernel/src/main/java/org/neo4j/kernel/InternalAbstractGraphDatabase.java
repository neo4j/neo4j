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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.cache.NoCacheProvider;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy.NodeActions;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipActions;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProvider;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.pagecache.PageCacheLifecycle;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.RollingLogMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.helpers.Settings.ANY;
import static org.neo4j.helpers.Settings.NO_DEFAULT;
import static org.neo4j.helpers.Settings.PATH;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.illegalValueMessage;
import static org.neo4j.helpers.Settings.matches;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies.fail;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_LABEL;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_PROPERTY_KEY;

/**
 * Base implementation of GraphDatabaseService. Responsible for creating services, handling dependencies between them,
 * and lifecycle management of these.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public abstract class InternalAbstractGraphDatabase
        implements GraphDatabaseService, GraphDatabaseAPI, SchemaWriteGuard
{

    public interface Dependencies
    {
        /**
         * Allowed to be null. Null means that no external {@link Monitors} was created, let the
         * database create its own monitors instance.
         */
        Monitors monitors();

        /**
         * Allowed to be null. Null means that no external {@link Logging} was created, let the
         * database create its own logging.
         */
        Logging logging();

        Iterable<Class<?>> settingsClasses();

        Iterable<KernelExtensionFactory<?>> kernelExtensions();

        Iterable<CacheProvider> cacheProviders();

        Iterable<QueryEngineProvider> executionEngines();
    }

    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();
    public static CommitProcessFactory defaultCommitProcessFactory = new CommitProcessFactory()
    {
        @Override
        public TransactionCommitProcess create( LogicalTransactionStore logicalTransactionStore,
                                                KernelHealth kernelHealth, NeoStore neoStore,
                                                TransactionRepresentationStoreApplier storeApplier,
                                                NeoStoreInjectedTransactionValidator txValidator,
                                                IndexUpdatesValidator indexUpdatesValidator, Config config )
        {
            if ( config.get( GraphDatabaseSettings.read_only ) )
            {
                return new ReadOnlyTransactionCommitProcess();
            }
            else
            {
                return new TransactionRepresentationCommitProcess( logicalTransactionStore, kernelHealth,
                        neoStore, storeApplier, indexUpdatesValidator );
            }
        }
    };
    protected final DependencyResolver dependencyResolver;
    protected final Config config;
    protected final LifeSupport life = new LifeSupport();
    private final Dependencies dependencies;
    private final Map<String,CacheProvider> cacheProviders;
    protected KernelExtensions kernelExtensions;
    protected File storeDir;
    protected Logging logging;
    protected StoreId storeId;
    protected StringLogger msgLog;
    protected StoreLockerLifecycleAdapter storeLocker;
    protected KernelEventHandlers kernelEventHandlers;
    protected TransactionEventHandlers transactionEventHandlers;
    protected RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    protected PropertyKeyTokenHolder propertyKeyTokenHolder;
    protected LabelTokenHolder labelTokenHolder;
    protected NodeManager nodeManager;
    protected IndexManager indexManager;
    protected Schema schema;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected KernelHealth kernelHealth;
    protected FileSystemAbstraction fileSystem;
    protected Locks lockManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected IndexConfigStore indexStore;
    protected PageCache pageCache;
    protected StoreFactory storeFactory;
    protected DiagnosticsManager diagnosticsManager;
    protected NeoStoreDataSource neoDataSource;
    protected RecoveryVerifier recoveryVerifier;
    protected Guard guard;
    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;
    protected Caches caches;
    protected ThreadToStatementContextBridge threadToTransactionBridge;
    protected BridgingCacheAccess cacheBridge;
    protected JobScheduler jobScheduler;
    protected Monitors monitors;
    protected TransactionCounters transactionMonitor;
    protected Tracers tracers;
    protected AvailabilityGuard availabilityGuard;
    protected long transactionStartTimeout;
    protected StoreUpgrader storeMigrationProcess;
    protected DataSourceManager dataSourceManager;
    private StartupStatisticsProvider startupStatistics;
    private QueryExecutionEngine queryExecutor = QueryEngineProvider.noEngine();

    protected InternalAbstractGraphDatabase( String storeDir, Map<String,String> params, Dependencies dependencies )
    {
        params.put( Configuration.store_dir.name(), storeDir );

        this.dependencyResolver = new DependencyResolverImpl();

        // SPI - provided services
        this.dependencies = dependencies;
        this.cacheProviders = mapCacheProviders( this.dependencies.cacheProviders() );
        config = new Config( params, getSettingsClasses(
                dependencies.settingsClasses(), dependencies.kernelExtensions(), dependencies.cacheProviders() ) );
        this.logging = dependencies.logging();
        this.monitors = dependencies.monitors();

        this.storeDir = config.get( Configuration.store_dir );
        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );
    }

    private Map<String,CacheProvider> mapCacheProviders( Iterable<CacheProvider> cacheProviders )
    {
        Map<String,CacheProvider> map = new HashMap<>();
        for ( CacheProvider provider : cacheProviders )
        {
            map.put( provider.getName(), provider );
        }
        return map;
    }

    protected void run()
    {
        create();
        Throwable error = null;
        try
        {
            enableAvailabilityLogging(); // Done after create to avoid a redundant "database is now unavailable"
            registerRecovery();

            life.start();
        }
        catch ( final Throwable throwable )
        {
            error = new RuntimeException( "Error starting " + getClass().getName() + ", " + storeDir.getAbsolutePath(),
                    throwable );
        }
        finally
        {
            if ( error != null )
            {
                try
                {
                    shutdown();
                }
                catch ( Throwable shutdownError )
                {
                    error = Exceptions.withSuppressed( shutdownError, error );
                }
            }
        }

        if ( error != null )
        {
            throw Exceptions.launderedException( error );
        }
    }

    protected TransactionCounters createTransactionCounters()
    {
        return new TransactionCounters();
    }

    private Tracers createTracers( Config config, StringLogger msgLog )
    {
        String desiredImplementationName = config.get( Configuration.tracer );
        return new Tracers( desiredImplementationName, msgLog );
    }

    protected void createDatabaseAvailability()
    {
        // This is how we lock the entire database to avoid threads using it during lifecycle events
        life.add( new DatabaseAvailability( availabilityGuard, transactionMonitor ) );
    }

    private void enableAvailabilityLogging()
    {
        availabilityGuard.addListener( new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
                msgLog.info( "Database is now ready" );
            }

            @Override
            public void unavailable()
            {
                msgLog.info( "Database is now unavailable" );
            }
        } );
    }

    protected void registerRecovery()
    {
        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( instance instanceof KernelExtensions && to.equals( LifecycleStatus.STARTED ) )
                {
                    InternalAbstractGraphDatabase.this.doAfterRecoveryAndStartup( true );
                }
            }
        } );
    }

    protected void doAfterRecoveryAndStartup( boolean isMaster )
    {
        storeId = neoDataSource.getStoreId();
        KernelDiagnostics.register( diagnosticsManager, InternalAbstractGraphDatabase.this, neoDataSource );
        if ( isMaster )
        {
            new RemoveOrphanConstraintIndexesOnStartup( neoDataSource.getKernel(), logging ).perform();
        }
    }

    protected void create()
    {
        this.kernelExtensions = new KernelExtensions(
                dependencies.kernelExtensions(),
                getDependencyResolver(),
                fail() );

        availabilityGuard = new AvailabilityGuard( Clock.SYSTEM_CLOCK );

        fileSystem = createFileSystemAbstraction();

        // If no logging was passed in from the outside then create logging and register
        // with this life
        if ( this.logging == null )
        {
            this.logging = createLogging();
        }

        // Component monitoring
        if ( this.monitors == null )
        {
            this.monitors = createMonitors();
        }

        transactionMonitor = createTransactionCounters();

        storeMigrationProcess = new StoreUpgrader( createUpgradeConfiguration(), fileSystem,
                monitors.newMonitor( StoreUpgrader.Monitor.class ), logging );

        this.msgLog = logging.getMessagesLog( getClass() );

        config.setLogger( msgLog );

        tracers = createTracers( config, msgLog );

        this.storeLocker = life.add( new StoreLockerLifecycleAdapter(
                new StoreLocker( fileSystem ), storeDir ) );

        new JvmChecker( msgLog, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        String cacheTypeName = config.get( Configuration.cache_type );
        CacheProvider cacheProvider = cacheProviders.get( cacheTypeName );
        if ( cacheProvider == null )
        {
            // Temporarily here to allow experimentally turning off the cache layer entirely. This is different
            // from cache_type=none in that it *completely* bypasses the caching layer.
            if ( config.get( Configuration.cache_type ).equals( CacheLayer.EXPERIMENTAL_OFF ) )
            {
                cacheProvider = new NoCacheProvider();
            }
            else
            {
                throw new IllegalArgumentException( "No provider for cache type '" + cacheTypeName + "'. " +
                                                    "Cache providers are loaded using java service loading where they" +
                                                    " " +
                                                    "register themselves in resource (plain-text) files found on the " +
                                                    "class path under " +
                                                    "META-INF/services/" + CacheProvider.class.getName() +
                                                    ". This missing provider may have " +
                                                    "been caused by either such a missing registration, " +
                                                    "or by the lack of the provider class itself." );

            }
        }

        jobScheduler = life.add( createJobScheduler() );

        pageCache = createPageCache();
        life.add( new PageCacheLifecycle( pageCache ) );

        kernelEventHandlers = new KernelEventHandlers( logging.getMessagesLog( KernelEventHandlers.class ) );

        caches = createCaches();
        diagnosticsManager = life.add( createDiagnosticsManager() );
        monitors.addMonitorListener( new RollingLogMonitor()
        {
            @Override
            public void rolledOver()
            {
                // Add diagnostics at the top of every log file
                diagnosticsManager.dumpAll();
            }
        } );

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        kernelHealth = new KernelHealth( kernelPanicEventGenerator, logging );

        // TODO please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = life.add( new DataSourceManager() );

        createTxHook();

        guard = config.get( Configuration.execution_guard_enabled ) ? new Guard( msgLog ) : null;

        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        StringLogger messagesLog = logging.getMessagesLog( StoreMigrator.class );
        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( messagesLog, System.out );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );
        storeMigrationProcess.addParticipant( new StoreMigrator(
                progressMonitor, fileSystem, upgradableDatabase, config, logging ) );

        propertyKeyTokenHolder = life.add( new PropertyKeyTokenHolder( createPropertyKeyCreator() ) );
        labelTokenHolder = life.add( new LabelTokenHolder( createLabelIdCreator() ) );
        relationshipTypeTokenHolder = life.add( new RelationshipTypeTokenHolder( createRelationshipTypeCreator() ) );

        caches.configure( cacheProvider, config );
        diagnosticsManager.tryAppendProvider( caches.node() );
        diagnosticsManager.tryAppendProvider( caches.relationship() );

        threadToTransactionBridge = life.add( new ThreadToStatementContextBridge() );

        nodeManager = createNodeManager();

        transactionEventHandlers = new TransactionEventHandlers( createNodeActions(), createRelationshipActions(),
                threadToTransactionBridge );

        indexStore = life.add( new IndexConfigStore( this.storeDir, fileSystem ) );

        diagnosticsManager.prependProvider( config );

        extensions = life.add( createKernelData() );

        life.add( kernelExtensions );

        schema = new SchemaImpl( threadToTransactionBridge );

        final LegacyIndexProxy.Lookup indexLookup = createIndexLookup();
        final IndexProvider indexProvider = new IndexProviderImpl( indexLookup, threadToTransactionBridge );
        nodeAutoIndexer = life.add( new NodeAutoIndexerImpl( config, indexProvider, nodeManager ) );
        relAutoIndexer = life.add( new RelationshipAutoIndexerImpl( config, indexProvider, nodeManager ) );
        indexManager = new IndexManagerImpl(
                threadToTransactionBridge, indexProvider, nodeAutoIndexer, relAutoIndexer );

        recoveryVerifier = createRecoveryVerifier();

        // Factories for things that needs to be created later
        storeFactory = createStoreFactory();

        startupStatistics = new StartupStatisticsProvider();

        createNeoDataSource();

        life.add( new MonitorGc( config, msgLog ) );

        life.add( nodeManager );

        createDatabaseAvailability();

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        dataSourceManager.addListener( new DataSourceManager.Listener()
        {
            @Override
            public void registered( NeoStoreDataSource dataSource )
            {
                queryExecutor = QueryEngineProvider.initialize( InternalAbstractGraphDatabase.this,
                                                                dependencies.executionEngines() );
            }

            @Override
            public void unregistered( NeoStoreDataSource dataSource )
            {
                queryExecutor = QueryEngineProvider.noEngine();
            }
        } );
    }

    protected DiagnosticsManager createDiagnosticsManager()
    {
        return new DiagnosticsManager( logging.getMessagesLog( DiagnosticsManager.class ) );
    }

    protected UpgradeConfiguration createUpgradeConfiguration()
    {
        return new ConfigMapUpgradeConfiguration( config );
    }

    protected Neo4jJobScheduler createJobScheduler()
    {
        return new Neo4jJobScheduler( this.toString() );
    }

    protected LegacyIndexProxy.Lookup createIndexLookup()
    {
        return new LegacyIndexProxy.Lookup()
        {
            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return InternalAbstractGraphDatabase.this;
            }
        };
    }

    protected Monitors createMonitors()
    {
        return new Monitors();
    }

    @Override
    public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
    {
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected TokenCreator createRelationshipTypeCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultRelationshipTypeCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    protected TokenCreator createPropertyKeyCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultPropertyTokenCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    protected TokenCreator createLabelIdCreator()
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultLabelIdCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    private NodeManager createNodeManager()
    {
        NodeActions nodeActions = createNodeActions();
        RelationshipActions relationshipLookup = createRelationshipActions();
        return new NodeManager(
                nodeActions,
                relationshipLookup,
                threadToTransactionBridge );
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return availabilityGuard.isAvailable( timeout );
    }

    @Override
    public void shutdown()
    {
        try
        {
            msgLog.info( "Shutdown started" );
            msgLog.flush();
            availabilityGuard.shutdown();
            life.shutdown();
        }
        catch ( LifecycleException throwable )
        {
            msgLog.warn( "Shutdown failed", throwable );
            throw throwable;
        }
    }

    protected StoreFactory createStoreFactory()
    {
        return new StoreFactory( config, idGeneratorFactory, pageCache, fileSystem,
                logging.getMessagesLog( StoreFactory.class ), monitors );
    }

    protected PageCache createPageCache()
    {
        ConfiguringPageCacheFactory pageCacheFactory = new ConfiguringPageCacheFactory(
                fileSystem, config, tracers.pageCacheTracer );
        PageCache pageCache = pageCacheFactory.getOrCreatePageCache();

        if ( config.get( GraphDatabaseSettings.dump_configuration ) )
        {
            pageCacheFactory.dumpConfiguration( logging.getMessagesLog( PageCache.class ) );
        }
        return pageCache;
    }

    protected RecoveryVerifier createRecoveryVerifier()
    {
        return RecoveryVerifier.ALWAYS_VALID;
    }

    protected KernelData createKernelData()
    {
        return new DefaultKernelData( config, this );
    }

    protected Caches createCaches()
    {
        return new DefaultCaches( msgLog, monitors );
    }

    protected RelationshipActions createRelationshipActions()
    {
        return new RelationshipActions()
        {
            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return InternalAbstractGraphDatabase.this;
            }

            @Override
            public void failTransaction()
            {
                threadToTransactionBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToTransactionBridge.assertInUnterminatedTransaction();
            }

            @Override
            public Statement statement()
            {
                return threadToTransactionBridge.instance();
            }

            @Override
            public Node newNodeProxy( long nodeId )
            {
                // only used by relationship already checked as valid in cache
                return nodeManager.newNodeProxyById( nodeId );
            }

            @Override
            public RelationshipType getRelationshipTypeById( int type )
            {
                try
                {
                    return relationshipTypeTokenHolder.getTokenById( type );
                }
                catch ( TokenNotFoundException e )
                {
                    throw new NotFoundException( e );
                }
            }
        };
    }

    protected NodeActions createNodeActions()
    {
        return new NodeActions()
        {
            @Override
            public Statement statement()
            {
                return threadToTransactionBridge.instance();
            }

            @Override
            public GraphDatabaseService getGraphDatabase()
            {
                // TODO This should be wrapped as well
                return InternalAbstractGraphDatabase.this;
            }

            @Override
            public void assertInUnterminatedTransaction()
            {
                threadToTransactionBridge.assertInUnterminatedTransaction();
            }

            @Override
            public void failTransaction()
            {
                threadToTransactionBridge.getKernelTransactionBoundToThisThread( true ).failure();
            }

            @Override
            public Relationship lazyRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxyById( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id )
            {
                return nodeManager.newRelationshipProxy( id );
            }

            @Override
            public Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
            {
                return nodeManager.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
            }
        };
    }

    protected void createTxHook()
    {
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new DefaultIdGeneratorFactory();
    }

    protected Locks createLockManager()
    {
        String key = config.get( Configuration.lock_manager );
        for ( Locks.Factory candidate : Service.load( Locks.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                return candidate.newInstance( ResourceTypes.values() );
            }
            else if ( key.equals( "" ) )
            {
                logging.getMessagesLog( InternalAbstractGraphDatabase.class )
                       .info( "No locking implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( ResourceTypes.values() );
            }
        }

        if ( key.equals( "community" ) )
        {
            return new CommunityLockManger();
        }
        else if ( key.equals( "" ) )
        {
            logging.getMessagesLog( InternalAbstractGraphDatabase.class )
                   .info( "No locking implementation specified, defaulting to 'community'" );
            return new CommunityLockManger();
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }

    protected Logging createLogging()
    {
        return life.add( DefaultLogging.createDefaultLogging( config, monitors ) );
    }

    protected void createNeoDataSource()
    {
        neoDataSource = new NeoStoreDataSource( config,
                storeFactory, logging.getMessagesLog( NeoStoreDataSource.class ), jobScheduler, logging,
                new NonTransactionalTokenNameLookup( labelTokenHolder, propertyKeyTokenHolder ),
                dependencyResolver, propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder,
                lockManager, this, transactionEventHandlers,
                monitors.newMonitor( IndexingService.Monitor.class ), fileSystem,
                storeMigrationProcess, transactionMonitor, kernelHealth,
                monitors.newMonitor( PhysicalLogFile.Monitor.class ),
                createHeaderInformationFactory(), startupStatistics, caches, nodeManager, guard, indexStore,
                getCommitProcessFactory(), pageCache, monitors, tracers );
        dataSourceManager.register( neoDataSource );
    }

    protected CommitProcessFactory getCommitProcessFactory()
    {
        return defaultCommitProcessFactory;
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    public final String getStoreDir()
    {
        return storeDir.getPath();
    }

    @Override
    public StoreId storeId()
    {
        return storeId;
    }

    @Override
    public Transaction beginTx()
    {
        availabilityGuard.checkAvailability( transactionStartTimeout, TransactionFailureException.class );

        KernelAPI kernel = neoDataSource.getKernel();
        TopLevelTransaction topLevelTransaction =
                threadToTransactionBridge.getTopLevelTransactionBoundToThisThread( false );
        if ( topLevelTransaction != null )
        {
            return new PlaceboTransaction( topLevelTransaction );
        }

        try
        {
            KernelTransaction transaction = kernel.newTransaction();
            topLevelTransaction = new TopLevelTransaction( transaction, threadToTransactionBridge );
            threadToTransactionBridge.bindTransactionToCurrentThread( topLevelTransaction );
            return topLevelTransaction;
        }
        catch ( org.neo4j.kernel.api.exceptions.TransactionFailureException e )
        {
            throw new TransactionFailureException( "Failure to begin transaction", e );
        }
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, Collections.<String,Object>emptyMap() );
    }

    @Override
    public Result execute( String query, Map<String, Object> parameters ) throws QueryExecutionException
    {
        availabilityGuard.checkAvailability( transactionStartTimeout, TransactionFailureException.class );
        try
        {
            return queryExecutor.executeQuery( query, parameters, QueryEngineProvider.embeddedSession() );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + getStoreDir() + "]";
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return GlobalGraphOperations.at( this ).getAllNodes();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return GlobalGraphOperations.at( this ).getAllRelationshipTypes();
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.registerKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.registerTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.unregisterKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.unregisterTransactionEventHandler( handler );
    }

    @Override
    public Node createNode()
    {
        try ( Statement statement = threadToTransactionBridge.instance() )
        {
            return nodeManager.newNodeProxyById( statement.dataWriteOperations().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        try ( Statement statement = threadToTransactionBridge.instance() )
        {
            long nodeId = statement.dataWriteOperations().nodeCreate();
            for ( Label label : labels )
            {
                int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label.name() );
                try
                {
                    statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new NotFoundException( "No node with id " + nodeId + " found.", e );
                }
            }
            return nodeManager.newNodeProxyById( nodeId );
        }
        catch ( ConstraintValidationKernelException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node getNodeById( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new NotFoundException( format( "Node %d not found", id ),
                    new EntityNotFoundException( EntityType.NODE, id ) );
        }
        try ( Statement statement = threadToTransactionBridge.instance() )
        {
            if ( !statement.readOperations().nodeExists( id ) )
            {
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            }

            return nodeManager.newNodeProxyById( id );
        }
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( format( "Relationship %d not found", id ),
                    new EntityNotFoundException( EntityType.RELATIONSHIP, id ));
        }
        try ( Statement statement = threadToTransactionBridge.instance() )
        {
            if ( !statement.readOperations().relationshipExists( id ) )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ),
                        new EntityNotFoundException( EntityType.RELATIONSHIP, id ));
            }

            return nodeManager.newRelationshipProxy( id );
        }
    }

    @Override
    public IndexManager index()
    {
        // TODO: txManager.assertInUnterminatedTransaction();
        return indexManager;
    }

    @Override
    public Schema schema()
    {
        threadToTransactionBridge.assertInUnterminatedTransaction();
        return schema;
    }

    // GraphDatabaseSPI implementation - THESE SHOULD EVENTUALLY BE REMOVED! DON'T ADD dependencies on these!
    public Config getConfig()
    {
        return config;
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
                                                   Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                                   Iterable<CacheProvider> cacheProviders )
    {
        List<Class<?>> totalSettingsClasses = new ArrayList<>();

        // Get the list of settings classes for extensions
        for ( KernelExtensionFactory<?> kernelExtension : kernelExtensions )
        {
            if ( kernelExtension.getSettingsClass() != null )
            {
                totalSettingsClasses.add( kernelExtension.getSettingsClass() );
            }
        }

        for ( CacheProvider cacheProvider : cacheProviders )
        {
            if ( cacheProvider.getSettingsClass() != null )
            {
                totalSettingsClasses.add( cacheProvider.getSettingsClass() );
            }
        }

        return totalSettingsClasses;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || !(o instanceof InternalAbstractGraphDatabase) )
        {
            return false;
        }
        InternalAbstractGraphDatabase that = (InternalAbstractGraphDatabase) o;
        return (storeId != null ? storeId.equals( that.storeId ) : that.storeId == null) &&
               storeDir.equals( that.storeDir );
    }

    @Override
    public int hashCode()
    {
        return storeDir.hashCode();
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel, final String key, final Object value )
    {
        return nodesByLabelAndProperty( myLabel, key, value );
    }

    @Override
    public Node findNode( final Label myLabel, final String key, final Object value )
    {
        try ( ResourceIterator<Node> iterator = findNodes( myLabel, key, value ) )
        {
            if ( !iterator.hasNext() )
            {
                return null;
            }
            Node node = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new MultipleFoundException();
            }
            return node;
        }
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel )
    {
        return allNodesWithLabel( myLabel );
    }

    @Override
    public ResourceIterable<Node> findNodesByLabelAndProperty( final Label myLabel, final String key,
                                                               final Object value )
    {
        return new ResourceIterable<Node>()
        {
            @Override
            public ResourceIterator<Node> iterator()
            {
                return nodesByLabelAndProperty( myLabel, key, value );
            }
        };
    }

    private ResourceIterator<Node> nodesByLabelAndProperty( Label myLabel, String key, Object value )
    {
        Statement statement = threadToTransactionBridge.instance();

        ReadOperations readOps = statement.readOperations();
        int propertyId = readOps.propertyKeyGetForName( key );
        int labelId = readOps.labelGetForName( myLabel.name() );

        if ( propertyId == NO_SUCH_PROPERTY_KEY || labelId == NO_SUCH_LABEL )
        {
            statement.close();
            return IteratorUtil.emptyIterator();
        }

        IndexDescriptor descriptor = findAnyIndexByLabelAndProperty( readOps, propertyId, labelId );

        try
        {
            if ( null != descriptor )
            {
                // Ha! We found an index - let's use it to find matching nodes
                return map2nodes( readOps.nodesGetFromIndexLookup( descriptor, value ), statement );
            }
        }
        catch ( IndexNotFoundKernelException e )
        {
            // weird at this point but ignore and fallback to a label scan
        }

        return getNodesByLabelAndPropertyWithoutIndex( propertyId, value, statement, labelId );
    }

    private IndexDescriptor findAnyIndexByLabelAndProperty( ReadOperations readOps, int propertyId, int labelId )
    {
        try
        {
            IndexDescriptor descriptor = readOps.indexesGetForLabelAndPropertyKey( labelId, propertyId );

            if ( readOps.indexGetState( descriptor ) == InternalIndexState.ONLINE )
            {
                // Ha! We found an index - let's use it to find matching nodes
                return descriptor;
            }
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            // If we don't find a matching index rule, we'll scan all nodes and filter manually (below)
        }
        return null;
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex( int propertyId, Object value,
                                                                           Statement statement, int labelId )
    {
        return map2nodes(
                new PropertyValueFilteringNodeIdIterator(
                        statement.readOperations().nodesGetForLabel( labelId ),
                        statement.readOperations(), propertyId, value ), statement );
    }

    private ResourceIterator<Node> allNodesWithLabel( final Label myLabel )
    {
        Statement statement = threadToTransactionBridge.instance();

        int labelId = statement.readOperations().labelGetForName( myLabel.name() );
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            statement.close();
            return emptyIterator();
        }

        final PrimitiveLongIterator nodeIds = statement.readOperations().nodesGetForLabel( labelId );
        return ResourceClosingIterator.newResourceIterator( statement, map( new FunctionFromPrimitiveLong<Node>()
        {
            @Override
            public Node apply( long nodeId )
            {
                return nodeManager.newNodeProxyById( nodeId );
            }
        }, nodeIds ) );
    }

    private ResourceIterator<Node> map2nodes( PrimitiveLongIterator input, Statement statement )
    {
        return ResourceClosingIterator.newResourceIterator( statement, map( new FunctionFromPrimitiveLong<Node>()
        {
            @Override
            public Node apply( long id )
            {
                return getNodeById( id );
            }
        }, input ) );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( threadToTransactionBridge );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl( threadToTransactionBridge );
    }

    public static class Configuration
    {
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> execution_guard_enabled = GraphDatabaseSettings.execution_guard_enabled;
        public static final Setting<String> cache_type = GraphDatabaseSettings.cache_type;
        public static final Setting<Boolean> ephemeral = setting( "ephemeral", Settings.BOOLEAN, Settings.FALSE );
        public static final Setting<String> ephemeral_keep_logical_logs = setting("keep_logical_logs", STRING, "1 files", illegalValueMessage( "must be `true`/`false` or of format '<number><optional unit> <type>' for example `100M size` for " +
                            "limiting logical log space on disk to 100Mb," +
                            " or `200k txs` for limiting the number of transactions to keep to 200 000", matches(ANY)));
        public static final Setting<File> store_dir = GraphDatabaseSettings.store_dir;
        public static final Setting<File> neo_store = GraphDatabaseSettings.neo_store;
        public static final Setting<File> internal_log_location = setting("store.internal_log.location", PATH, NO_DEFAULT );

        // Kept here to have it not be publicly documented.
        public static final Setting<String> lock_manager = setting( "lock_manager", STRING, "" );

        public static final Setting<String> log_configuration_file = setting( "log.configuration", STRING,
                "neo4j-logback.xml" );
        public static final Setting<String> tracer =
                setting( "dbms.tracer", Settings.STRING, (String) null ); // 'null' default.
    }

    private static class PropertyValueFilteringNodeIdIterator extends PrimitiveLongBaseIterator
    {
        private final PrimitiveLongIterator nodesWithLabel;
        private final ReadOperations statement;
        private final int propertyKeyId;
        private final Object value;

        PropertyValueFilteringNodeIdIterator( PrimitiveLongIterator nodesWithLabel, ReadOperations statement,
                                              int propertyKeyId, Object value )
        {
            this.nodesWithLabel = nodesWithLabel;
            this.statement = statement;
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        protected boolean fetchNext()
        {
            for ( boolean hasNext = nodesWithLabel.hasNext(); hasNext; hasNext = nodesWithLabel.hasNext() )
            {
                long nextValue = nodesWithLabel.next();
                try
                {
                    if ( statement.nodeGetProperty( nextValue, propertyKeyId ).valueEquals( value ) )
                    {
                        return next( nextValue );
                    }
                }
                catch ( EntityNotFoundException e )
                {
                    // continue to the next node
                }
            }
            return false;
        }
    }

    protected final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData( Config config, GraphDatabaseAPI graphDb )
        {
            super( config );
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return graphDb;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }
    }

    /**
     * FIXME: This is supposed to be handled by a Dependency Injection framework...
     *
     * @author ceefour
     */
    class DependencyResolverImpl extends DependencyResolver.Adapter
    {
        private <T> T resolveKnownSingleDependency( Class<T> type )
        {
            if ( type.equals( Map.class ) )
            {
                return type.cast( getConfig().getParams() );
            }
            else if ( type.equals( Config.class ) )
            {
                return type.cast( getConfig() );
            }
            else if ( GraphDatabaseService.class.isAssignableFrom( type )
                      && type.isInstance( InternalAbstractGraphDatabase.this ) )
            {
                return type.cast( InternalAbstractGraphDatabase.this );
            }
            else if ( QueryExecutionEngine.class.isAssignableFrom( type ) && type.isInstance( queryExecutor ) )
            {
                return type.cast( queryExecutor );
            }
            else if ( Locks.class.isAssignableFrom( type ) && type.isInstance( lockManager ) )
            {
                // Locks used to ensure pessimistic concurrency control between transactions
                return type.cast( lockManager );
            }
            else if ( LockService.class.isAssignableFrom( type )
                      && type.isInstance( neoDataSource.getLockService() ) )
            {
                // Locks used to control concurrent access to the store files
                return type.cast( neoDataSource.getLockService() );
            }
            else if ( StoreFactory.class.isAssignableFrom( type ) && type.isInstance( storeFactory ) )
            {
                return type.cast( storeFactory );
            }
            else if ( SchemaWriteGuard.class.isAssignableFrom( type ) )
            {
                return type.cast( InternalAbstractGraphDatabase.this );
            }
            else if ( StringLogger.class.isAssignableFrom( type ) && type.isInstance( msgLog ) )
            {
                return type.cast( msgLog );
            }
            else if ( Logging.class.isAssignableFrom( type ) && type.isInstance( logging ) )
            {
                return type.cast( logging );
            }
            else if ( IndexConfigStore.class.isAssignableFrom( type ) && type.isInstance( indexStore ) )
            {
                return type.cast( indexStore );
            }
            else if ( DataSourceManager.class.isAssignableFrom( type ) && type.isInstance( dataSourceManager ) )
            {
                return type.cast( dataSourceManager );
            }
            else if ( FileSystemAbstraction.class.isAssignableFrom( type ) && type.isInstance( fileSystem ) )
            {
                return type.cast( fileSystem );
            }
            else if ( PageCache.class.isAssignableFrom( type ) )
            {
                return type.cast( pageCache );
            }
            else if ( Guard.class.isAssignableFrom( type ) && type.isInstance( guard ) )
            {
                return type.cast( guard );
            }
            else if ( IndexProviders.class.isAssignableFrom( type ) && type.isInstance( neoDataSource ) )
            {
                return type.cast( neoDataSource );
            }
            else if ( KernelData.class.isAssignableFrom( type ) && type.isInstance( extensions ) )
            {
                return type.cast( extensions );
            }
            else if ( KernelExtensions.class.isAssignableFrom( type ) && type.isInstance( kernelExtensions ) )
            {
                return type.cast( kernelExtensions );
            }
            else if ( NodeManager.class.isAssignableFrom( type ) && type.isInstance( nodeManager ) )
            {
                return type.cast( nodeManager );
            }
            else if ( DiagnosticsManager.class.isAssignableFrom( type ) && type.isInstance( diagnosticsManager ) )
            {
                return type.cast( diagnosticsManager );
            }
            else if ( RelationshipTypeTokenHolder.class.isAssignableFrom( type ) &&
                      type.isInstance( relationshipTypeTokenHolder ) )
            {
                return type.cast( relationshipTypeTokenHolder );
            }
            else if ( PropertyKeyTokenHolder.class.isAssignableFrom( type ) &&
                      type.isInstance( propertyKeyTokenHolder ) )
            {
                return type.cast( propertyKeyTokenHolder );
            }
            else if ( LabelTokenHolder.class.isAssignableFrom( type ) && type.isInstance( labelTokenHolder ) )
            {
                return type.cast( labelTokenHolder );
            }
            else if ( KernelPanicEventGenerator.class.isAssignableFrom( type ) )
            {
                return type.cast( kernelPanicEventGenerator );
            }
            else if ( LifeSupport.class.isAssignableFrom( type ) )
            {
                return type.cast( life );
            }
            else if ( Monitors.class.isAssignableFrom( type ) )
            {
                return type.cast( monitors );
            }
            else if ( ThreadToStatementContextBridge.class.isAssignableFrom( type )
                      && type.isInstance( threadToTransactionBridge ) )
            {
                return type.cast( threadToTransactionBridge );
            }
            else if ( CacheAccessBackDoor.class.isAssignableFrom( type ) && type.isInstance( cacheBridge ) )
            {
                return type.cast( cacheBridge );
            }
            else if ( StoreLockerLifecycleAdapter.class.isAssignableFrom( type ) && type.isInstance( storeLocker ) )
            {
                return type.cast( storeLocker );
            }
            else if ( IndexManager.class.equals( type ) && type.isInstance( indexManager ) )
            {
                return type.cast( indexManager );
            }
            else if ( IndexingService.class.isAssignableFrom( type )
                      && type.isInstance( neoDataSource.getIndexService() ) )
            {
                return type.cast( neoDataSource.getIndexService() );
            }
            else if ( JobScheduler.class.isAssignableFrom( type ) && type.isInstance( jobScheduler ) )
            {
                return type.cast( jobScheduler );
            }
            else if ( KernelAPI.class.equals( type ) )
            {
                return type.cast( neoDataSource.getKernel() );
            }
            else if ( LabelScanStore.class.isAssignableFrom( type )
                      && type.isInstance( neoDataSource.getLabelScanStore() ) )
            {
                return type.cast( neoDataSource.getLabelScanStore() );
            }
            else if ( NeoStoreProvider.class.isAssignableFrom( type ) )
            {
                return type.cast( neoDataSource );
            }
            else if ( IdGeneratorFactory.class.isAssignableFrom( type ) )
            {
                return type.cast( idGeneratorFactory );
            }
            else if ( Monitors.class.isAssignableFrom( type ) )
            {
                return type.cast( monitors );
            }
            else if ( TransactionEventHandlers.class.equals( type ) )
            {
                return type.cast( transactionEventHandlers );
            }
            else if ( DependencyResolver.class.equals( type ) )
            {
                return type.cast( DependencyResolverImpl.this );
            }
            else if ( KernelHealth.class.isAssignableFrom( type ) )
            {
                return type.cast( kernelHealth );
            }
            else if ( StoreUpgrader.class.isAssignableFrom( type ) )
            {
                return type.cast( storeMigrationProcess );
            }
            else if ( StoreId.class.isAssignableFrom( type ) )
            {
                return type.cast( storeId );
            }
            else if ( AvailabilityGuard.class.isAssignableFrom( type ) )
            {
                return type.cast( availabilityGuard );
            }
            else if ( StartupStatistics.class.isAssignableFrom( type ) )
            {
                return type.cast( startupStatistics );
            }
            else if ( TransactionMonitor.class.isAssignableFrom( type ) )
            {
                return type.cast( transactionMonitor );
            }
            else if ( PageCacheMonitor.class.isAssignableFrom( type ) )
            {
                return type.cast( tracers.pageCacheTracer );
            }
            else if ( Caches.class.isAssignableFrom( type ) )
            {
                return type.cast( caches );
            }
            return null;
        }

        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
        {
            // Try known single dependencies
            T result = resolveKnownSingleDependency( type );
            if ( result != null )
            {
                return selector.select( type, Iterables.option( result ) );
            }

            // Try neostore datasource
            try
            {
                result = neoDataSource.getDependencyResolver().resolveDependency( type, selector );
                if ( result != null )
                {
                    return selector.select( type, Iterables.option( result ) );
                }
            }
            catch ( IllegalArgumentException e )
            {   // Nah, didn't find it
            }

            // Try with kernel extensions
            return kernelExtensions.resolveDependency( type, selector );
        }
    }
}
