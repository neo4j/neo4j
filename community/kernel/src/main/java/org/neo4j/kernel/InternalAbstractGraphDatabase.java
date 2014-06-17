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
package org.neo4j.kernel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
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
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationChange;
import org.neo4j.kernel.configuration.ConfigurationChangeListener;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.NodeProxy.NodeLookup;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.RelationshipData;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy.RelationshipLookups;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.LegacyIndexProxy;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.DataSourceManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultTxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitorImpl;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
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
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.helpers.Functions.identity;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.setting;
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
         * Allowed to be null. Null means that no external {@link Logging} was created, let the
         * database create its own logging.
         * @return
         */
        Logging logging();

        Iterable<Class<?>> settingsClasses();

        Iterable<KernelExtensionFactory<?>> kernelExtensions();

        Iterable<CacheProvider> cacheProviders();
    }

    public static class Configuration
    {
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> use_memory_mapped_buffers =
                GraphDatabaseSettings.use_memory_mapped_buffers;
        public static final Setting<Boolean> execution_guard_enabled = GraphDatabaseSettings.execution_guard_enabled;
        public static final Setting<String> cache_type = GraphDatabaseSettings.cache_type;
        public static final Setting<Boolean> ephemeral = setting( "ephemeral", Settings.BOOLEAN, Settings.FALSE );
        public static final Setting<File> store_dir = GraphDatabaseSettings.store_dir;
        public static final Setting<File> neo_store = GraphDatabaseSettings.neo_store;

        // Kept here to have it not be publicly documented.
        public static final Setting<String> lock_manager = setting( "lock_manager", STRING, "" );
        public static final Setting<Boolean> statistics_enabled =
                setting("statistics_enabled", Settings.BOOLEAN, Settings.FALSE);
    }

    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    protected final KernelExtensions kernelExtensions;

    protected final DependencyResolver dependencyResolver = new DependencyResolverImpl();
    protected final Config config;

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
    protected IndexManagerImpl indexManager;
    protected Schema schema;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected KernelHealth kernelHealth;
    protected FileSystemAbstraction fileSystem;
    protected Locks lockManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected IndexConfigStore indexStore;
    protected TxIdGenerator txIdGenerator;
    protected StoreFactory storeFactory;
    protected DiagnosticsManager diagnosticsManager;
    protected NeoStoreXaDataSource neoDataSource;
    protected RecoveryVerifier recoveryVerifier;
    protected Guard guard;
    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;
    protected Caches caches;
    protected ThreadToStatementContextBridge threadToTransactionBridge;
    protected BridgingCacheAccess cacheBridge;
    protected JobScheduler jobScheduler;
    protected UpdateableSchemaState updateableSchemaState;
    protected Monitors monitors;
    protected TransactionMonitor transactionMonitor;
    protected final LifeSupport life = new LifeSupport();
    private final Map<String, CacheProvider> cacheProviders;
    protected AvailabilityGuard availabilityGuard;
    protected long accessTimeout;
    protected StoreUpgrader storeMigrationProcess;
    protected TransactionHeaderInformation transactionHeaderInformation;
    private DataSourceManager dataSourceManager;
    private StartupStatisticsProvider startupStatistics;
    private CacheProvider cacheProvider;

    protected InternalAbstractGraphDatabase( String storeDir, Map<String, String> params, Dependencies dependencies )
    {
        params.put( Configuration.store_dir.name(), storeDir );

        // SPI - provided services
        this.cacheProviders = mapCacheProviders( dependencies.cacheProviders() );
        config = new Config( params, getSettingsClasses(
                dependencies.settingsClasses(), dependencies.kernelExtensions(), dependencies.cacheProviders() ) );
        this.logging = dependencies.logging();

        this.kernelExtensions = new KernelExtensions(
                dependencies.kernelExtensions(),
                config,
                getDependencyResolver(),
                fail() );
        this.storeDir = config.get( Configuration.store_dir );
        accessTimeout = 1 * 1000; // TODO make configurable
    }

    private Map<String, CacheProvider> mapCacheProviders( Iterable<CacheProvider> cacheProviders )
    {
        Map<String, CacheProvider> map = new HashMap<>();
        for ( CacheProvider provider : cacheProviders )
        {
            map.put( provider.getName(), provider );
        }
        return map;
    }

    protected void run()
    {
        create();

        try
        {
            registerRecovery();

            life.start();
        }
        catch ( final Throwable throwable )
        {
            StringBuilder msg = new StringBuilder();
            msg.append( "Startup failed" );
            Throwable temporaryThrowable = throwable;
            while ( temporaryThrowable != null )
            {
                msg.append( ": " ).append( temporaryThrowable.getMessage() );
                temporaryThrowable = temporaryThrowable.getCause();
            }

            msgLog.error( msg.toString() );

            shutdown();

            throw new RuntimeException( "Error starting " + getClass().getName() + ", " + storeDir.getAbsolutePath(),
                    throwable );
        }
    }

    protected void createDatabaseAvailability()
    {
        // This is how we lock the entire database to avoid threads using it during lifecycle events
        life.add( new DatabaseAvailability( availabilityGuard, transactionMonitor ) );
    }

    protected void registerRecovery()
    {
        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                // TODO do not explicitly depend on order of start() calls in txManager and XaDatasourceManager
                // use two booleans instead
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
        availabilityGuard = createAvailabilityGuard();

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

        fileSystem = createFileSystemAbstraction();

        // If no logging was passed in from the outside then create logging and register
        // with this life
        if ( this.logging == null )
        {
            this.logging = createLogging();
        }

        // Component monitoring
        this.monitors = createMonitors();

        storeMigrationProcess = new StoreUpgrader( new ConfigMapUpgradeConfiguration( config ), fileSystem,
                monitors.newMonitor( StoreUpgrader.Monitor.class ) );

        // Apply autoconfiguration for memory settings
        AutoConfigurator autoConfigurator = new AutoConfigurator( fileSystem,
                config.get( NeoStoreXaDataSource.Configuration.store_dir ),
                config.get( Configuration.use_memory_mapped_buffers ),
                logging.getConsoleLog( AutoConfigurator.class ) );
        if (config.get( GraphDatabaseSettings.dump_configuration ))
        {
            System.out.println( autoConfigurator.getNiceMemoryInformation() );
        }
        Map<String, String> configParams = config.getParams();
        Map<String, String> autoConfiguration = autoConfigurator.configure();
        for ( Map.Entry<String, String> autoConfig : autoConfiguration.entrySet() )
        {
            // Don't override explicit settings
            String key = autoConfig.getKey();
            if ( !config.getParams().containsKey( key ) )
            {
                configParams.put( key, autoConfig.getValue() );
            }
        }

        config.applyChanges( configParams );

        this.msgLog = logging.getMessagesLog( getClass() );

        config.setLogger( msgLog );

        this.storeLocker = life.add( new StoreLockerLifecycleAdapter(
                new StoreLocker( fileSystem ), storeDir ) );

        new JvmChecker( msgLog, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        // Instantiate all services - some are overridable by subclasses
        boolean readOnly = config.get( Configuration.read_only );

        String cacheTypeName = config.get( Configuration.cache_type );
        cacheProvider = cacheProviders.get( cacheTypeName );
        if ( cacheProvider == null )
        {
            throw new IllegalArgumentException( "No provider for cache type '" + cacheTypeName + "'. " +
                    "Cache providers are loaded using java service loading where they " +
                    "register themselves in resource (plain-text) files found on the class path under " +
                    "META-INF/services/" + CacheProvider.class.getName() + ". This missing provider may have " +
                    "been caused by either such a missing registration, or by the lack of the provider class itself." );
        }

        jobScheduler =
            life.add( new Neo4jJobScheduler( this.toString(), logging.getMessagesLog( Neo4jJobScheduler.class ) ));

        kernelEventHandlers = new KernelEventHandlers(logging.getMessagesLog( KernelEventHandlers.class ));

        caches = createCaches();
        diagnosticsManager = life.add( new DiagnosticsManager( logging.getMessagesLog( DiagnosticsManager.class ) ) );

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        kernelHealth = new KernelHealth( kernelPanicEventGenerator, logging );

        // TODO 2.2-future please fix the bad dependencies instead of doing this. Before the removal of JTA
        // this was the place of the XaDataSourceManager. NeoStoreXaDataSource is create further down than
        // (specifically) KernelExtensions, which creates an interesting out-of-order issue with #doAfterRecovery().
        // Anyways please fix this.
        dataSourceManager = life.add( new DataSourceManager() );

        createTxHook();

        guard = config.get( Configuration.execution_guard_enabled ) ? new Guard( msgLog ) : null;
//        assert guard == null : "Guard not properly implemented for the time being";

        updateableSchemaState = new KernelSchemaStateStore( newSchemaStateMap() );

        txIdGenerator = life.add( createTxIdGenerator() );

        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        storeMigrationProcess.addParticipant( new StoreMigrator(
                new VisibleMigrationProgressMonitor( logging.getMessagesLog( StoreMigrator.class ), System.out ),
                new UpgradableDatabase( new StoreVersionCheck( fileSystem ) ), config ) );

        propertyKeyTokenHolder = life.add( new PropertyKeyTokenHolder( createPropertyKeyCreator() ) );
        labelTokenHolder = life.add( new LabelTokenHolder( createLabelIdCreator() ) );
        relationshipTypeTokenHolder = life.add( new RelationshipTypeTokenHolder( createRelationshipTypeCreator() ) );

        caches.configure( cacheProvider, config );
        Cache<NodeImpl> nodeCache = diagnosticsManager.tryAppendProvider( caches.node() );
        Cache<RelationshipImpl> relCache = diagnosticsManager.tryAppendProvider( caches.relationship() );

        threadToTransactionBridge = life.add( new ThreadToStatementContextBridge() );

        nodeManager = createNodeManager( readOnly, cacheProvider, nodeCache, relCache );

        transactionEventHandlers = new TransactionEventHandlers( createNodeLookup(), createRelationshipLookups(),
                threadToTransactionBridge  );

        indexStore = life.add( new IndexConfigStore( this.storeDir, fileSystem ) );

        diagnosticsManager.prependProvider( config );

        extensions = life.add( createKernelData() );

        life.add( kernelExtensions );

        schema = new SchemaImpl( threadToTransactionBridge );

        LegacyIndexProxy.Lookup indexLookup = createIndexLookup();
        indexManager = new IndexManagerImpl( indexLookup, threadToTransactionBridge );
        nodeAutoIndexer = life.add( new NodeAutoIndexerImpl( config, indexManager, nodeManager ) );
        relAutoIndexer = life.add( new RelationshipAutoIndexerImpl( config, indexManager, nodeManager ) );

        // TODO This cyclic dependency should be resolved
        indexManager.setNodeAutoIndexer( nodeAutoIndexer );
        indexManager.setRelAutoIndexer( relAutoIndexer );

        recoveryVerifier = createRecoveryVerifier();

        // Factories for things that needs to be created later
        storeFactory = createStoreFactory();

        startupStatistics = new StartupStatisticsProvider();

        transactionHeaderInformation = createTransactionHeaderInformation();
        transactionMonitor = new TransactionMonitorImpl();
        createNeoDataSource();

        life.add( new MonitorGc( config, msgLog ) );

        life.add( nodeManager );

        createDatabaseAvailability();

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        // TODO This is probably too coarse-grained and we should have some strategy per user of config instead
        life.add( new ConfigurationChangedRestarter() );
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

            @Override
            public EntityFactory getEntityFactory()
            {
                return nodeManager;
            }
        };
    }

    protected TransactionHeaderInformation createTransactionHeaderInformation()
    {
        return new TransactionHeaderInformation( -1, -1, new byte[0] );
    }

    protected Monitors createMonitors()
    {
        return new Monitors();
    }

    protected AvailabilityGuard createAvailabilityGuard()
    {
        return new AvailabilityGuard( Clock.SYSTEM_CLOCK, 1 );
    }

    @Override
    public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
    {
    }

    private Map<Object, Object> newSchemaStateMap() {
        return new HashMap<>();
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected TokenCreator createRelationshipTypeCreator()
    {
        return new DefaultRelationshipTypeCreator( dataSourceManager, idGeneratorFactory );
    }

    protected TokenCreator createPropertyKeyCreator()
    {
        return new DefaultPropertyTokenCreator( dataSourceManager, idGeneratorFactory );
    }

    protected TokenCreator createLabelIdCreator()
    {
        return new DefaultLabelIdCreator( dataSourceManager, idGeneratorFactory );
    }

    private NodeManager createNodeManager( final boolean readOnly, final CacheProvider cacheType,
                                           Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        NodeLookup nodeLookup = createNodeLookup();
        RelationshipLookups relationshipLookup = createRelationshipLookups();
        return new NodeManager(
                logging.getMessagesLog( NodeManager.class ), nodeLookup, relationshipLookup,
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
        return new StoreFactory( config, idGeneratorFactory, createWindowPoolFactory(), fileSystem,
                logging.getMessagesLog( StoreFactory.class ) );
    }

    protected DefaultWindowPoolFactory createWindowPoolFactory()
    {
        return new DefaultWindowPoolFactory();
    }

    protected RecoveryVerifier createRecoveryVerifier()
    {
        return RecoveryVerifier.ALWAYS_VALID;
    }

    protected KernelData createKernelData()
    {
        return new DefaultKernelData( config, this );
    }

    protected TxIdGenerator createTxIdGenerator()
    {
        // TODO 2.2-future move to after neostore data source is created
        return new DefaultTxIdGenerator( new Provider<TransactionIdStore>()
        {
            @Override
            public TransactionIdStore instance()
            {
                return neoDataSource.evaluate();
            }
        } );
    }

    protected Caches createCaches()
    {
        return new DefaultCaches( msgLog, monitors );
    }

    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return new RelationshipProxy.RelationshipLookups()
        {
            private final ThreadLocal<RelationshipData> relationshipData = new ThreadLocal<RelationshipData>()
            {
                @Override
                protected RelationshipData initialValue()
                {
                    return new RelationshipData();
                }
            };

            private final RelationshipVisitor<RuntimeException> visitor =
                    new RelationshipVisitor<RuntimeException>()
            {
                @Override
                public void visit( long relId, long startNode, long endNode, int type )
                {
                    relationshipData.get().set( startNode, endNode, type );
                }
            };

            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return InternalAbstractGraphDatabase.this;
            }

            @Override
            public Node newNodeProxy( long nodeId )
            {
                // only used by relationship already checked as valid in cache
                return nodeManager.newNodeProxyById( nodeId );
            }

            @Override
            public RelationshipData getRelationshipData( long relationshipId )
            {
                try ( Statement statement = threadToTransactionBridge.instance() )
                {
                    statement.readOperations().relationshipVisit( relationshipId, visitor );
                    return relationshipData.get();
                }
                catch ( EntityNotFoundException e )
                {
                    throw new NotFoundException( e );
                }
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

    protected NodeProxy.NodeLookup createNodeLookup()
    {
        return new NodeProxy.NodeLookup()
        {
            @Override
            public GraphDatabaseService getGraphDatabase()
            {
                // TODO This should be wrapped as well
                return InternalAbstractGraphDatabase.this;
            }

            @Override
            public NodeManager getNodeManager()
            {
                return nodeManager;
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
        for ( Locks.Factory candidate : Service.load(Locks.Factory.class) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if( candidateId.equals( key ))
            {
                return candidate.newInstance( ResourceTypes.values() );
            }
            else if(key.equals( "" ))
            {
                logging.getMessagesLog( InternalAbstractGraphDatabase.class )
                        .info( "No locking implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( ResourceTypes.values() );
            }
        }

        if( key.equals( "community" ) )
        {
            return new CommunityLockManger();
        }
        else if(key.equals( "" ))
        {
            logging.getMessagesLog( InternalAbstractGraphDatabase.class )
                    .info( "No locking implementation specified, defaulting to 'community'" );
            return new CommunityLockManger();
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }

    protected Logging createLogging()
    {
        return life.add( DefaultLogging.createDefaultLogging( config ) );
    }

    protected void createNeoDataSource()
    {
        neoDataSource = new NeoStoreXaDataSource( config,
                storeFactory, logging.getMessagesLog( NeoStoreXaDataSource.class ), jobScheduler, logging,
                updateableSchemaState, new NonTransactionalTokenNameLookup( labelTokenHolder, propertyKeyTokenHolder ),
                dependencyResolver, propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder,
                lockManager, this, transactionEventHandlers,
                monitors.newMonitor( IndexingService.Monitor.class ), fileSystem, createTranslationFactory(),
                storeMigrationProcess, transactionMonitor, kernelHealth, txIdGenerator,
                transactionHeaderInformation, startupStatistics, caches, nodeManager, guard, indexStore );
        dataSourceManager.register( neoDataSource );
    }

    protected Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>> createTranslationFactory()
    {
        return new Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>>()
        {
            @Override
            public Function<List<LogEntry>, List<LogEntry>> apply( NeoStore neoStore )
            {
                return identity();
            }
        };
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
        if ( !availabilityGuard.isAvailable( accessTimeout ) )
        {
            throw new TransactionFailureException( "Database is currently not available. "
                    + availabilityGuard.describeWhoIsBlocking() );
        }

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
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
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
        catch ( ReadOnlyDatabaseKernelException e )
        {
            throw new ReadOnlyDbException();
        }
    }

    @Override
    public Node getNodeById( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new NotFoundException( format( "Node %d not found", id ) );
        }
        threadToTransactionBridge.assertInTransaction();
        try (Statement statement = threadToTransactionBridge.instance())
        {
            if ( !statement.readOperations().nodeExists( id ) )
            {
                throw new NotFoundException(format( "Node %d not found", id ));
            }

            return nodeManager.newNodeProxyById( id );
        }
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( format( "Relationship %d not found", id));
        }
        threadToTransactionBridge.assertInTransaction();
        try (Statement statement = threadToTransactionBridge.instance())
        {
            if ( !statement.readOperations().relationshipExists( id ) )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ) );
            }

            return nodeManager.newRelationshipProxyById( id );
        }
    }

    @Override
    public IndexManager index()
    {
        // TODO: txManager.assertInTransaction();
        return indexManager;
    }

    @Override
    public Schema schema()
    {
        threadToTransactionBridge.assertInTransaction();
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
            else if( StoreFactory.class.isAssignableFrom( type ) && type.isInstance( storeFactory ) )
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
            else if ( TxIdGenerator.class.isAssignableFrom( type ) && type.isInstance( txIdGenerator ) )
            {
                return type.cast( txIdGenerator );
            }
            else if ( DiagnosticsManager.class.isAssignableFrom( type ) && type.isInstance( diagnosticsManager ) )
            {
                return type.cast( diagnosticsManager );
            }
            else if ( RelationshipTypeTokenHolder.class.isAssignableFrom( type ) && type.isInstance( relationshipTypeTokenHolder ) )
            {
                return type.cast( relationshipTypeTokenHolder );
            }
            else if ( PropertyKeyTokenHolder.class.isAssignableFrom( type ) && type.isInstance( propertyKeyTokenHolder ) )
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
            else if ( IndexManager.class.equals( type )&& type.isInstance( indexManager )  )
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
            else if( KernelAPI.class.equals( type ))
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
            else if( TransactionEventHandlers.class.equals( type ))
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
            else if ( AvailabilityGuard.class.isAssignableFrom( type ) )
            {
                return type.cast( availabilityGuard );
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

            // Try with kernel extensions
            return kernelExtensions.resolveDependency( type, selector );
        }
    }

    class ConfigurationChangedRestarter extends LifecycleAdapter
    {
        private final ConfigurationChangeListener listener = new ConfigurationChangeListener()
        {
            Executor executor = Executors.newSingleThreadExecutor( new DaemonThreadFactory( "Database configuration " +
                    "restart" ) );

            @Override
            public void notifyConfigurationChanges( final Iterable<ConfigurationChange> change )
            {
                executor.execute( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        // Restart
                        try
                        {
                            life.stop();
                            life.start();

                            msgLog.logMessage( "Database restarted with the following configuration changes:" +
                                    change );
                        }
                        catch ( LifecycleException e )
                        {
                            msgLog.logMessage( "Could not restart database", e );
                        }
                    }
                } );
            }
        };

        @Override
        public void start() throws Throwable
        {
            config.addConfigurationChangeListener( listener );
        }

        @Override
        public void stop() throws Throwable
        {
            config.removeConfigurationChangeListener( listener );
        }
    }

    @Override
    public ResourceIterable<Node> findNodesByLabelAndProperty( final Label myLabel, final String key, final Object value )
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
        IndexDescriptor descriptor = findUniqueIndexByLabelAndProperty( readOps, labelId, propertyId );

        if ( null == descriptor )
        {
            descriptor = findRegularIndexByLabelAndProperty( readOps, labelId, propertyId );
        }
        return descriptor;
    }

    private IndexDescriptor findUniqueIndexByLabelAndProperty( ReadOperations readOps, int labelId, int propertyId )
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

    private IndexDescriptor findRegularIndexByLabelAndProperty( ReadOperations readOps, int labelId, int propertyId )
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

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription(threadToTransactionBridge);
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl(threadToTransactionBridge);
    }
}
