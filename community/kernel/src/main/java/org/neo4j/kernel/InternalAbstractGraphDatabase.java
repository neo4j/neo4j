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
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DatabaseShutdownException;
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
import org.neo4j.helpers.Factory;
import org.neo4j.helpers.FunctionFromPrimitiveLong;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
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
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.cleanup.CleanupIfOutsideTransaction;
import org.neo4j.kernel.impl.cleanup.CleanupService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.ReadOnlyNodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.Transactor;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.NodeAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerImpl;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.ReadOnlyTxManager;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.TransactionManagerProvider;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.AbstractPrimitiveLongIterator;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
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
import org.neo4j.kernel.logging.LogbackWeakDependency;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;

import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_LABEL;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
import static org.neo4j.kernel.logging.LogbackWeakDependency.DEFAULT_TO_CLASSIC;

/**
 * Base implementation of GraphDatabaseService. Responsible for creating services, handling dependencies between them,
 * and lifecycle management of these.
 *
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public abstract class InternalAbstractGraphDatabase
        extends AbstractGraphDatabase implements GraphDatabaseService, GraphDatabaseAPI, SchemaWriteGuard
{
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
        public static final Setting<File> logical_log = GraphDatabaseSettings.logical_log;
    }

    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    protected File storeDir;
    protected Map<String, String> params;
    private final TransactionInterceptorProviders transactionInterceptorProviders;
    protected StoreId storeId;
    private final TransactionBuilder defaultTxBuilder = new TransactionBuilderImpl( this, ForceMode.forced );

    protected final KernelExtensions kernelExtensions;

    protected Config config;

    protected DependencyResolver dependencyResolver;
    protected Logging logging;
    protected StringLogger msgLog;
    protected StoreLockerLifecycleAdapter storeLocker;
    protected KernelEventHandlers kernelEventHandlers;
    protected TransactionEventHandlers transactionEventHandlers;
    protected RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    protected NodeManager nodeManager;
    protected IndexManagerImpl indexManager;
    protected Schema schema;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected RemoteTxHook txHook;
    protected FileSystemAbstraction fileSystem;
    protected XaDataSourceManager xaDataSourceManager;
    protected LockManager lockManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected NioNeoDbPersistenceSource persistenceSource;
    protected TxEventSyncHookFactory syncHook;
    protected PersistenceManager persistenceManager;
    protected PropertyKeyTokenHolder propertyKeyTokenHolder;
    protected LabelTokenHolder labelTokenHolder;
    protected IndexStore indexStore;
    protected AbstractTransactionManager txManager;
    protected TxIdGenerator txIdGenerator;
    protected StoreFactory storeFactory;
    protected XaFactory xaFactory;
    protected DiagnosticsManager diagnosticsManager;
    protected NeoStoreXaDataSource neoDataSource;
    protected RecoveryVerifier recoveryVerifier;
    protected Guard guard;
    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;
    protected Caches caches;

    protected TransactionStateFactory stateFactory;
    protected ThreadToStatementContextBridge statementContextProvider;
    protected BridgingCacheAccess cacheBridge;
    protected JobScheduler jobScheduler;
    protected UpdateableSchemaState updateableSchemaState;
    protected CleanupService cleanupService;

    protected Monitors monitors;

    protected final LifeSupport life = new LifeSupport();
    private final Map<String, CacheProvider> cacheProviders;
    protected AvailabilityGuard availabilityGuard;
    protected long accessTimeout;

    protected InternalAbstractGraphDatabase( String storeDir, Map<String, String> params,
                                             Iterable<Class<?>> settingsClasses,
                                             Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                             Iterable<CacheProvider> cacheProviders,
                                             Iterable<TransactionInterceptorProvider> transactionInterceptorProviders )
    {
        this.params = params;

        dependencyResolver = new DependencyResolverImpl();

        // Setup configuration
        params.put( Configuration.store_dir.name(), storeDir );

        // SPI - provided services
        this.cacheProviders = mapCacheProviders( cacheProviders );
        config = new Config( params, getSettingsClasses( settingsClasses, kernelExtensions, cacheProviders ) );

        this.kernelExtensions = new KernelExtensions( kernelExtensions, config, getDependencyResolver(),
                UnsatisfiedDependencyStrategies.fail() );
        this.transactionInterceptorProviders = new TransactionInterceptorProviders( transactionInterceptorProviders,
                dependencyResolver );

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

            Throwable recoveryError = txManager.getRecoveryError();
            if ( recoveryError != null )
            {
                throw recoveryError;
            }
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
        life.add( new DatabaseAvailability( txManager, availabilityGuard ) );
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
        if ( txManager.getRecoveryError() != null )
        {   // If recovery failed then there's no point in going any further here. The database startup will fail.
            return;
        }

        if ( txManager instanceof TxManager )
        {
            @SuppressWarnings("deprecation")
            NeoStoreXaDataSource neoStoreDataSource = xaDataSourceManager.getNeoStoreDataSource();
            storeId = neoStoreDataSource.getStoreId();
            KernelDiagnostics.register( diagnosticsManager, InternalAbstractGraphDatabase.this, neoStoreDataSource );
            if ( isMaster )
            {
                new RemoveOrphanConstraintIndexesOnStartup( new Transactor( txManager, persistenceManager ), logging )
                        .perform();
            }
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

        // Create logger
        this.logging = createLogging();

        // Component monitoring
        this.monitors = new Monitors();

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
            if ( !params.containsKey( key ) )
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
        CacheProvider cacheProvider = cacheProviders.get( cacheTypeName );
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

        xaDataSourceManager = life.add( createXaDataSourceManager() );

        txHook = createTxHook();

        guard = config.get( Configuration.execution_guard_enabled ) ? new Guard( msgLog ) : null;

        stateFactory = createTransactionStateFactory();
        
        Factory<byte[]> xidGlobalIdFactory = createXidGlobalIdFactory();

        updateableSchemaState = new KernelSchemaStateStore( newSchemaStateMap() );

        if ( readOnly )
        {
            txManager = new ReadOnlyTxManager( xaDataSourceManager, xidGlobalIdFactory,
                    logging.getMessagesLog( ReadOnlyTxManager.class ) );
        }
        else
        {
            String serviceName = config.get( GraphDatabaseSettings.tx_manager_impl );
            if ( GraphDatabaseSettings.tx_manager_impl.getDefaultValue().equals( serviceName ) )
            {
                txManager = new TxManager( this.storeDir, xaDataSourceManager, kernelPanicEventGenerator,
                        logging.getMessagesLog( TxManager.class ), fileSystem, stateFactory,
                        xidGlobalIdFactory, monitors );
            }
            else
            {
                TransactionManagerProvider provider;
                provider = Service.load( TransactionManagerProvider.class, serviceName );
                txManager = provider.loadTransactionManager( this.storeDir.getPath(), xaDataSourceManager,
                        kernelPanicEventGenerator, txHook, logging.getMessagesLog( AbstractTransactionManager.class ),
                        fileSystem, stateFactory );
            }
        }
        life.add( txManager );

        cleanupService = life.add( createCleanupService() );

        transactionEventHandlers = new TransactionEventHandlers( txManager );

        txIdGenerator = life.add( createTxIdGenerator() );

        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        persistenceSource = life.add( new NioNeoDbPersistenceSource( xaDataSourceManager ) );

        syncHook = new DefaultTxEventSyncHookFactory();

        persistenceManager = new PersistenceManager( logging.getMessagesLog( PersistenceManager.class ), txManager,
                persistenceSource, syncHook );

        propertyKeyTokenHolder = life.add( new PropertyKeyTokenHolder( txManager, persistenceManager, persistenceSource, createPropertyKeyCreator() ) );
        labelTokenHolder = life.add( new LabelTokenHolder( txManager, persistenceManager, persistenceSource, createLabelIdCreator() ) );
        relationshipTypeTokenHolder = life.add( new RelationshipTypeTokenHolder( txManager,
                persistenceManager, persistenceSource, createRelationshipTypeCreator() ) );

        caches.configure( cacheProvider, config );
        Cache<NodeImpl> nodeCache = diagnosticsManager.tryAppendProvider( caches.node() );
        Cache<RelationshipImpl> relCache = diagnosticsManager.tryAppendProvider( caches.relationship() );

        statementContextProvider = life.add( new ThreadToStatementContextBridge( persistenceManager ) );

        nodeManager = guard != null ?
                createGuardedNodeManager( readOnly, cacheProvider, nodeCache, relCache ) :
                createNodeManager( readOnly, cacheProvider, nodeCache, relCache );

        stateFactory.setDependencies( lockManager, nodeManager, txHook, txIdGenerator );

        indexStore = life.add( new IndexStore( this.storeDir, fileSystem ) );

        diagnosticsManager.prependProvider( config );

        // Config can auto-configure memory mapping settings and what not, so reassign params
        // after we've instantiated Config.
        params = config.getParams();

        extensions = life.add( createKernelData() );

        life.add( kernelExtensions );

        schema = new SchemaImpl( statementContextProvider );

        indexManager = new IndexManagerImpl( config, indexStore, xaDataSourceManager, txManager, this );
        nodeAutoIndexer = life.add( new NodeAutoIndexerImpl( config, indexManager, nodeManager ) );
        relAutoIndexer = life.add( new RelationshipAutoIndexerImpl( config, indexManager, nodeManager ) );

        // TODO This cyclic dependency should be resolved
        indexManager.setNodeAutoIndexer( nodeAutoIndexer );
        indexManager.setRelAutoIndexer( relAutoIndexer );

        recoveryVerifier = createRecoveryVerifier();

        // Factories for things that needs to be created later
        storeFactory = createStoreFactory();
        String keepLogicalLogsConfig = config.get( GraphDatabaseSettings.keep_logical_logs );
        xaFactory = new XaFactory( config, txIdGenerator, txManager, fileSystem,
                monitors, logging, recoveryVerifier, LogPruneStrategies.fromConfigValue(
                fileSystem, keepLogicalLogsConfig ) );

        createNeoDataSource();

        life.add( new MonitorGc( config, msgLog ) );

        life.add( nodeManager );

        createDatabaseAvailability();

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        // TODO This is probably too coarse-grained and we should have some strategy per user of config instead
        life.add( new ConfigurationChangedRestarter() );
    }

    protected Factory<byte[]> createXidGlobalIdFactory()
    {
        return new Factory<byte[]>()
        {
            @Override
            public byte[] newInstance()
            {
                return getNewGlobalId( DEFAULT_SEED, MASTER_ID_REPRESENTING_NO_MASTER );
            }
        };
    }

    protected AvailabilityGuard createAvailabilityGuard()
    {
        return new AvailabilityGuard( Clock.SYSTEM_CLOCK, 1 );
    }

    public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
    {
    }

    protected CleanupService createCleanupService()
    {
        return CleanupService.create( jobScheduler, logging, new CleanupIfOutsideTransaction( txManager ) );
    }

    private Map<Object, Object> newSchemaStateMap() {
        return new HashMap<>();
    }

    protected TransactionStateFactory createTransactionStateFactory()
    {
        return new TransactionStateFactory( logging );
    }

    protected XaDataSourceManager createXaDataSourceManager()
    {
        return new XaDataSourceManager( logging.getMessagesLog( XaDataSourceManager.class ) );
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected TokenCreator createRelationshipTypeCreator()
    {
        return new DefaultRelationshipTypeCreator( logging );
    }

    protected TokenCreator createPropertyKeyCreator()
    {
        return new DefaultPropertyTokenCreator( logging );
    }

    protected TokenCreator createLabelIdCreator()
    {
        return new DefaultLabelIdCreator( logging );
    }

    private NodeManager createNodeManager( final boolean readOnly, final CacheProvider cacheType,
                                           Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( logging.getMessagesLog( NodeManager.class ), this, txManager, persistenceManager,
                    persistenceSource, relationshipTypeTokenHolder, cacheType, propertyKeyTokenHolder, labelTokenHolder,
                    createNodeLookup(), createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager,
                    statementContextProvider );
        }

        return new NodeManager(
                logging.getMessagesLog( NodeManager.class ), this, txManager, persistenceManager,
                persistenceSource, relationshipTypeTokenHolder, cacheType, propertyKeyTokenHolder, labelTokenHolder,
                createNodeLookup(), createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager,
                statementContextProvider );
    }

    private NodeManager createGuardedNodeManager( final boolean readOnly, final CacheProvider cacheType,
                                                  Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( logging.getMessagesLog( NodeManager.class ), this, txManager, persistenceManager,
                    persistenceSource, relationshipTypeTokenHolder, cacheType, propertyKeyTokenHolder, labelTokenHolder, createNodeLookup(),
                    createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager, statementContextProvider )
            {
                @Override
                public Node getNodeByIdOrNull( final long nodeId )
                {
                    guard.check();
                    return super.getNodeByIdOrNull( nodeId );
                }

                @Override
                public NodeImpl getNodeForProxy( final long nodeId, final LockType lock )
                {
                    guard.check();
                    return super.getNodeForProxy( nodeId, lock );
                }

                @Override
                public RelationshipImpl getRelationshipForProxy( final long relId )
                {
                    guard.check();
                    return super.getRelationshipForProxy( relId );
                }

                @Override
                protected Relationship getRelationshipByIdOrNull( final long relId )
                {
                    guard.check();
                    return super.getRelationshipByIdOrNull( relId );
                }

                @Override
                public Node createNode()
                {
                    guard.check();
                    return super.createNode();
                }

                @Override
                public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode,
                                                        Node endNode, long relationshipTypeId )
                {
                    guard.check();
                    return super.createRelationship( startNodeProxy, startNode, endNode, relationshipTypeId );
                }
            };
        }

        return new NodeManager( logging.getMessagesLog( NodeManager.class ), this, txManager, persistenceManager,
                persistenceSource, relationshipTypeTokenHolder, cacheType, propertyKeyTokenHolder, labelTokenHolder, createNodeLookup(),
                createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager, statementContextProvider )
        {
            @Override
            public Node getNodeByIdOrNull( final long nodeId )
            {
                guard.check();
                return super.getNodeByIdOrNull( nodeId );
            }

            @Override
            public NodeImpl getNodeForProxy( final long nodeId, final LockType lock )
            {
                guard.check();
                return super.getNodeForProxy( nodeId, lock );
            }

            @Override
            public RelationshipImpl getRelationshipForProxy( final long relId )
            {
                guard.check();
                return super.getRelationshipForProxy( relId );
            }

            @Override
            protected Relationship getRelationshipByIdOrNull( final long relId )
            {
                guard.check();
                return super.getRelationshipByIdOrNull( relId );
            }

            @Override
            public Node createNode()
            {
                guard.check();
                return super.createNode();
            }

            @Override
            public Relationship createRelationship( Node startNodeProxy, NodeImpl startNode,
                                                    Node endNode, long relationshipTypeId )
            {
                guard.check();
                return super.createRelationship( startNodeProxy, startNode, endNode, relationshipTypeId );
            }
        };
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
        return new StoreFactory( config, idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem,
                logging.getMessagesLog( StoreFactory.class ), txHook );
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
        return TxIdGenerator.DEFAULT;
    }

    protected Caches createCaches()
    {
        return new DefaultCaches( msgLog, monitors );
    }

    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return new RelationshipProxy.RelationshipLookups()
        {
            @Override
            public RelationshipImpl lookupRelationship( long relationshipId )
            {
                assertDatabaseRunning();
                return nodeManager.getRelationshipForProxy( relationshipId );
            }

            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                return InternalAbstractGraphDatabase.this;
            }

            @Override
            public NodeManager getNodeManager()
            {
                return nodeManager;
            }

            @Override
            public Node newNodeProxy( long nodeId )
            {
                // only used by relationship already checked as valid in cache
                return nodeManager.newNodeProxyById( nodeId );
            }
        };
    }

    protected NodeProxy.NodeLookup createNodeLookup()
    {
        return new NodeProxy.NodeLookup()
        {
            @Override
            public NodeImpl lookup( long nodeId )
            {
                assertDatabaseRunning();
                return nodeManager.getNodeForProxy( nodeId, null );
            }

            @Override
            public NodeImpl lookup( long nodeId, LockType lock )
            {
                assertDatabaseRunning();
                return nodeManager.getNodeForProxy( nodeId, lock );
            }

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

    // This is here until we've moved all operations into the kernel, which handles this check on it's own.
    private void assertDatabaseRunning()
    {
        if( life.isRunning() )
        {
            return;
        }
        throw new DatabaseShutdownException();
    }

    protected RemoteTxHook createTxHook()
    {
        return new DefaultTxHook();
    }

    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new DefaultIdGeneratorFactory();
    }

    protected LockManager createLockManager()
    {
        return new LockManagerImpl( new RagManager() );
    }

    protected Logging createLogging()
    {
        return life.add( new LogbackWeakDependency().tryLoadLogbackService( config, DEFAULT_TO_CLASSIC ) );
    }

    protected void createNeoDataSource()
    {
        // Create DataSource
        neoDataSource = new NeoStoreXaDataSource( config,
                storeFactory, logging.getMessagesLog( NeoStoreXaDataSource.class ),
                xaFactory, stateFactory, transactionInterceptorProviders, jobScheduler, logging,
                updateableSchemaState, new NonTransactionalTokenNameLookup( labelTokenHolder, propertyKeyTokenHolder ),
                dependencyResolver, txManager, propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder,
                persistenceManager, lockManager, this );
        xaDataSourceManager.registerDataSource( neoDataSource );
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
        return tx().begin();
    }

    protected Transaction beginTx( ForceMode forceMode )
    {
        if ( !availabilityGuard.isAvailable( accessTimeout ) )
        {
            throw new TransactionFailureException( "Database is currently not available. "
                    + availabilityGuard.describeWhoIsBlocking() );
        }

        try
        {
            if ( transactionRunning() )
            {
                return new PlaceboTransaction( persistenceManager, txManager, txManager.getTransactionState() );
            }

            txManager.begin( forceMode );
            return new TopLevelTransaction( persistenceManager, txManager, txManager.getTransactionState() );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( "Couldn't get transaction", e );
        }
        catch ( NotSupportedException e )
        {
            throw new TransactionFailureException( "Couldn't begin transaction", e );
        }
    }

    @Override
    public boolean transactionRunning()
    {
        try
        {
            return txManager.getTransaction() != null;
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException(
                    "Unable to get transaction.", e );
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
        try ( Statement statement = statementContextProvider.instance() )
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
        try ( Statement statement = statementContextProvider.instance() )
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
        return nodeManager.getNodeById( id );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( format( "Relationship %d not found", id));
        }
        return nodeManager.getRelationshipById( id );
    }

    @Override
    public TransactionBuilder tx()
    {
        return defaultTxBuilder;
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
        txManager.assertInTransaction();
        return schema;
    }

    // GraphDatabaseSPI implementation - THESE SHOULD EVENTUALLY BE REMOVED! DON'T ADD dependencies on these!
    public Config getConfig()
    {
        return config;
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
                                                   Iterable<KernelExtensionFactory<?>> kernelExtensions, Iterable
            <CacheProvider> cacheProviders )
    {
        List<Class<?>> totalSettingsClasses = new ArrayList<>();

        // Add given settings classes
        Iterables.addAll( totalSettingsClasses, settingsClasses );

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

    private class DefaultTxEventSyncHookFactory implements TxEventSyncHookFactory
    {
        @Override
        public TransactionEventsSyncHook create()
        {
            return transactionEventHandlers.hasHandlers() ?
                    new TransactionEventsSyncHook( transactionEventHandlers, txManager ) : null;
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
            else if ( TransactionManager.class.isAssignableFrom( type ) && type.isInstance( txManager ) )
            {
                return type.cast( txManager );
            }
            else if ( LockManager.class.isAssignableFrom( type ) && type.isInstance( lockManager ) )
            {
                return type.cast( lockManager );
            }
            else if( StoreFactory.class.isAssignableFrom( type ) && type.isInstance( storeFactory ) )
            {
                return type.cast( storeFactory );
            }
            else if ( StringLogger.class.isAssignableFrom( type ) && type.isInstance( msgLog ) )
            {
                return type.cast( msgLog );
            }
            else if ( Logging.class.isAssignableFrom( type ) && type.isInstance( logging ) )
            {
                return type.cast( logging );
            }
            else if ( IndexStore.class.isAssignableFrom( type ) && type.isInstance( indexStore ) )
            {
                return type.cast( indexStore );
            }
            else if ( XaFactory.class.isAssignableFrom( type ) && type.isInstance( xaFactory ) )
            {
                return type.cast( xaFactory );
            }
            else if ( XaDataSourceManager.class.isAssignableFrom( type ) && type.isInstance( xaDataSourceManager ) )
            {
                return type.cast( xaDataSourceManager );
            }
            else if ( FileSystemAbstraction.class.isAssignableFrom( type ) && type.isInstance( fileSystem ) )
            {
                return type.cast( fileSystem );
            }
            else if ( Guard.class.isAssignableFrom( type ) && type.isInstance( guard ) )
            {
                return type.cast( guard );
            }
            else if ( IndexProviders.class.isAssignableFrom( type ) && type.isInstance( indexManager ) )
            {
                return type.cast( indexManager );
            }
            else if ( KernelData.class.isAssignableFrom( type ) && type.isInstance( extensions ) )
            {
                return type.cast( extensions );
            }
            else if ( TransactionInterceptorProviders.class.isAssignableFrom( type )
                    && type.isInstance( transactionInterceptorProviders ) )
            {
                return type.cast( transactionInterceptorProviders );
            }
            else if ( KernelExtensions.class.isAssignableFrom( type ) && type.isInstance( kernelExtensions ) )
            {
                return type.cast( kernelExtensions );
            }
            else if ( NodeManager.class.isAssignableFrom( type ) && type.isInstance( nodeManager ) )
            {
                return type.cast( nodeManager );
            }
            else if ( TransactionStateFactory.class.isAssignableFrom( type ) && type.isInstance( stateFactory ) )
            {
                return type.cast( stateFactory );
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
                return (T) monitors;
            }
            else if ( PersistenceManager.class.isAssignableFrom( type ) && type.isInstance( persistenceManager ) )
            {
                return type.cast( persistenceManager );
            }
            else if ( ThreadToStatementContextBridge.class.isAssignableFrom( type )
                    && type.isInstance( statementContextProvider ) )
            {
                return type.cast( statementContextProvider );
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
            else if ( CleanupService.class.equals( type ) )
            {
                return type.cast( cleanupService );
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
            else if ( RemoteTxHook.class.isAssignableFrom( type ) )
            {
                return type.cast( txHook );
            }
            else if ( DependencyResolver.class.equals( type ) )
            {
                return type.cast( DependencyResolverImpl.this );
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
        Statement statement = statementContextProvider.instance();

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
        return cleanupService.resourceIterator( map( new FunctionFromPrimitiveLong<Node>()
        {
            @Override
            public Node apply( long id )
            {
                return getNodeById( id );
            }
        }, input ), statement );
    }

    private static class PropertyValueFilteringNodeIdIterator extends AbstractPrimitiveLongIterator
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
            computeNext();
        }

        @Override
        protected void computeNext()
        {
            for ( boolean hasNext = nodesWithLabel.hasNext(); hasNext; hasNext = nodesWithLabel.hasNext() )
            {
                long nextValue = nodesWithLabel.next();
                try
                {
                    if ( statement.nodeGetProperty( nextValue, propertyKeyId ).valueEquals( value ) )
                    {
                        next( nextValue );
                        return;
                    }
                }
                catch ( EntityNotFoundException e )
                {
                    // continue to the next node
                }
            }
            endReached();
        }
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription(statementContextProvider);
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl(statementContextProvider);
    }
}
