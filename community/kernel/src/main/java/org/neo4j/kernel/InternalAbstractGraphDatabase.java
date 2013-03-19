/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.slf4j.impl.StaticLoggerBinder.getSingleton;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import ch.qos.logback.classic.LoggerContext;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.IndexProviderKernelExtensionFactory;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.LruMap;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationChange;
import org.neo4j.kernel.configuration.ConfigurationChangeListener;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.Kernel;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.SchemaCache;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.DefaultPropertyKeyCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.KeyCreator;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.core.ReadOnlyNodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.ReadOnlyTxManager;
import org.neo4j.kernel.impl.transaction.TransactionManagerProvider;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
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
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * Base implementation of GraphDatabaseService. Responsible for creating services, handling dependencies between them,
 * and lifecycle management of these.
 */
public abstract class InternalAbstractGraphDatabase
        extends AbstractGraphDatabase implements GraphDatabaseService, GraphDatabaseAPI
{
    public static class Configuration
    {
        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> use_memory_mapped_buffers =
                GraphDatabaseSettings.use_memory_mapped_buffers;
        public static final Setting<Boolean> execution_guard_enabled = GraphDatabaseSettings.execution_guard_enabled;
        public static final GraphDatabaseSettings.CacheTypeSetting cache_type = GraphDatabaseSettings.cache_type;
        public static final Setting<Boolean> load_kernel_extensions = GraphDatabaseSettings.load_kernel_extensions;
        public static final Setting<Boolean> ephemeral = new GraphDatabaseSetting.BooleanSetting(
                Settings.setting("ephemeral", Settings.BOOLEAN, Settings.FALSE ) );

        public static final Setting<File> store_dir = GraphDatabaseSettings.store_dir;
        public static final Setting<File> neo_store = GraphDatabaseSettings.neo_store;
        public static final Setting<File> logical_log = GraphDatabaseSettings.logical_log;
    }

    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    protected File storeDir;
    protected Map<String, String> params;
    private final TransactionInterceptorProviders transactionInterceptorProviders;
    private final KernelExtensions kernelExtensions;
    protected StoreId storeId;
    private final TransactionBuilder defaultTxBuilder = new TransactionBuilderImpl( this, ForceMode.forced );

    protected Config config;

    protected DependencyResolver dependencyResolver;
    protected Logging logging;
    protected StringLogger msgLog;
    protected StoreLockerLifecycleAdapter storeLocker;
    protected KernelEventHandlers kernelEventHandlers;
    protected TransactionEventHandlers transactionEventHandlers;
    protected RelationshipTypeHolder relationshipTypeHolder;
    protected NodeManager nodeManager;
    protected IndexManagerImpl indexManager;
    protected Schema schema;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected TxHook txHook;
    protected FileSystemAbstraction fileSystem;
    protected XaDataSourceManager xaDataSourceManager;
    protected LockManager lockManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected KeyCreator relationshipTypeCreator;
    protected NioNeoDbPersistenceSource persistenceSource;
    protected TxEventSyncHookFactory syncHook;
    protected PersistenceManager persistenceManager;
    protected PropertyIndexManager propertyIndexManager;
    protected IndexStore indexStore;
    protected LogBufferFactory logBufferFactory;
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
    protected KernelAPI kernelAPI;
    protected ThreadToStatementContextBridge statementContextProvider;
    protected BridgingCacheAccess cacheBridge;
    protected JobScheduler jobScheduler;
    protected UpdateableSchemaState updateableSchemaState;

    protected final LifeSupport life = new LifeSupport();
    private final Map<String,CacheProvider> cacheProviders;

    protected InternalAbstractGraphDatabase( String storeDir, Map<String, String> params,
                                             Iterable<Class<?>> settingsClasses,
                                             Iterable<IndexProvider> indexProviders,
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

        // Convert IndexProviders into KernelExtensionFactories
        // Remove this when the deprecated IndexProvider is removed
        Iterable<KernelExtensionFactory<?>> indexProviderKernelExtensions = Iterables.map( new Function<IndexProvider, KernelExtensionFactory<?>>()
        {
            @Override
            public KernelExtensionFactory<?> apply( IndexProvider from )
            {
                return new IndexProviderKernelExtensionFactory( from );
            }
        }, indexProviders );

        kernelExtensions = Iterables.concat( kernelExtensions, indexProviderKernelExtensions );

        this.kernelExtensions = new KernelExtensions( kernelExtensions, config, dependencyResolver );
        this.transactionInterceptorProviders = new TransactionInterceptorProviders( transactionInterceptorProviders,
                dependencyResolver );
        
        this.storeDir = config.get( Configuration.store_dir );
    }

    private Map<String, CacheProvider> mapCacheProviders( Iterable<CacheProvider> cacheProviders )
    {
        Map<String, CacheProvider> map = new HashMap<String, CacheProvider>();
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

            if ( txManager.getRecoveryError() != null )
            {
                throw txManager.getRecoveryError();
            }
        }
        catch ( Throwable throwable )
        {
            msgLog.logMessage( "Startup failed", throwable );

            shutdown();

            throw new RuntimeException( throwable );
        }
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
                if ( instance instanceof KernelExtensions && to.equals( LifecycleStatus.STARTED ) && txManager
                        instanceof TxManager )
                {
                    InternalAbstractGraphDatabase.this.doAfterRecoveryAndStartup();
                }
            }
        } );
    }

    protected void doAfterRecoveryAndStartup()
    {
        NeoStoreXaDataSource neoStoreDataSource = xaDataSourceManager.getNeoStoreDataSource();
        storeId = neoStoreDataSource.getStoreId();
        KernelDiagnostics.register( diagnosticsManager, InternalAbstractGraphDatabase.this,
                neoStoreDataSource );
    }

    protected void create()
    {
        fileSystem = createFileSystemAbstraction();

        // Create logger
        this.logging = createLogging();

        // Apply autoconfiguration for memory settings
        AutoConfigurator autoConfigurator = new AutoConfigurator( fileSystem,
                config.get( NeoStoreXaDataSource.Configuration.store_dir ),
                GraphDatabaseSettings.UseMemoryMappedBuffers.shouldMemoryMap(
                        config.get( Configuration.use_memory_mapped_buffers ) ),
                config.get( GraphDatabaseSettings.dump_configuration ) );
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

        this.msgLog = logging.getLogger( getClass() );

        config.setLogger( msgLog );

        this.storeLocker = life.add(new StoreLockerLifecycleAdapter(
                new StoreLocker( config, fileSystem, msgLog ), storeDir ));

        new JvmChecker(msgLog, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();

        // Instantiate all services - some are overridable by subclasses
        boolean readOnly = config.get( Configuration.read_only );

        String cacheTypeName = config.get( Configuration.cache_type );
        CacheProvider cacheProvider = cacheProviders.get( cacheTypeName );
        if ( cacheProvider == null )
        {
            throw new IllegalArgumentException( "No cache type '" + cacheTypeName + "'" );
        }

        jobScheduler = life.add( new Neo4jJobScheduler());

        kernelEventHandlers = new KernelEventHandlers();

        caches = createCaches();
        diagnosticsManager = life.add( new DiagnosticsManager( logging.getLogger( DiagnosticsManager.class ) ) );

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        xaDataSourceManager = life.add( createXaDataSourceManager() );

        txHook = createTxHook();

        guard = config.get( Configuration.execution_guard_enabled ) ? new Guard( msgLog ) : null;

        stateFactory = createTransactionStateFactory();

        int queryCacheSize = config.get(GraphDatabaseSettings.query_cache_size);
        updateableSchemaState = new KernelSchemaStateStore( new LruMap<Object, Object>(queryCacheSize) );

        if ( readOnly )
        {
            StringLogger roTxManagerLogging = logging.getLogger(ReadOnlyTxManager.class);
            txManager = new ReadOnlyTxManager( xaDataSourceManager, updateableSchemaState, roTxManagerLogging);
        }
        else
        {
            String serviceName = config.get( GraphDatabaseSettings.tx_manager_impl );
            if ( GraphDatabaseSettings.tx_manager_impl.getDefaultValue().equals( serviceName ) )
            {
                txManager = new TxManager( this.storeDir, xaDataSourceManager, kernelPanicEventGenerator,
                        logging.getLogger( TxManager.class ), fileSystem, stateFactory );
            }
            else
            {
                TransactionManagerProvider provider;
                provider = Service.load( TransactionManagerProvider.class, serviceName );
                if ( provider == null )
                {
                    throw new IllegalStateException( "Unknown transaction manager implementation: "
                            + serviceName );
                }
                txManager = provider.loadTransactionManager( this.storeDir.getPath(), xaDataSourceManager,
                        kernelPanicEventGenerator, txHook, logging.getLogger( AbstractTransactionManager.class ),
                        fileSystem, stateFactory );
            }
        }
        life.add( txManager );

        transactionEventHandlers = new TransactionEventHandlers( txManager );

        txIdGenerator = life.add( createTxIdGenerator() );

        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        relationshipTypeCreator = createRelationshipTypeCreator();

        persistenceSource = life.add(new NioNeoDbPersistenceSource(xaDataSourceManager));

        syncHook = new DefaultTxEventSyncHookFactory();

        persistenceManager = new PersistenceManager( logging.getLogger( PersistenceManager.class ), txManager,
                persistenceSource, syncHook );
        
        KeyCreator propertyIndexCreator = createPropertyKeyCreator();

        propertyIndexManager = life.add( new PropertyIndexManager( txManager, persistenceManager, persistenceSource, propertyIndexCreator ) );

        relationshipTypeHolder = new RelationshipTypeHolder( txManager,
                persistenceManager, persistenceSource, relationshipTypeCreator );

        caches.configure( cacheProvider, config );
        Cache<NodeImpl> nodeCache = diagnosticsManager.tryAppendProvider( caches.node() );
        Cache<RelationshipImpl> relCache = diagnosticsManager.tryAppendProvider( caches.relationship() );
        
        SchemaCache schemaCache = new SchemaCache( Collections.<SchemaRule>emptyList() );

        kernelAPI = life.add( new Kernel( txManager, propertyIndexManager, persistenceManager,
                xaDataSourceManager, lockManager, schemaCache, updateableSchemaState, dependencyResolver ) );
        // XXX: Circular dependency, temporary during transition to KernelAPI - TxManager should not depend on KernelAPI
        txManager.setKernel(kernelAPI);

        statementContextProvider = life.add( new ThreadToStatementContextBridge( kernelAPI, txManager,
                xaDataSourceManager ) );

        nodeManager = guard != null ?
                createGuardedNodeManager( readOnly, cacheProvider, nodeCache, relCache ) :
                createNodeManager( readOnly, cacheProvider, nodeCache, relCache );

        life.add( nodeManager );
        stateFactory.setDependencies( lockManager, nodeManager, txHook, txIdGenerator );

        indexStore = life.add( new IndexStore( this.storeDir, fileSystem ) );

        diagnosticsManager.prependProvider( config );

        // Config can auto-configure memory mapping settings and what not, so reassign params
        // after we've instantiated Config.
        params = config.getParams();

        /*
         *  LogBufferFactory needs access to the parameters so it has to be added after the default and
         *  user supplied configurations are consolidated
         */

        logBufferFactory = new DefaultLogBufferFactory();

        extensions = life.add( createKernelData() );

        if ( config.get( Configuration.load_kernel_extensions ) )
        {
            life.add( kernelExtensions );
        }

        schema = new SchemaImpl( statementContextProvider, propertyIndexManager );

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
        xaFactory = new XaFactory( config, txIdGenerator, txManager, logBufferFactory, fileSystem,
                logging, recoveryVerifier, LogPruneStrategies.fromConfigValue(
                fileSystem, keepLogicalLogsConfig ) );
        
        cacheBridge = new BridgingCacheAccess( nodeManager, schemaCache );

        createNeoDataSource();

        life.add( new MonitorGc( config, msgLog ) );

        // This is how we lock the entire database to avoid threads using it during lifecycle events
        life.add( new DatabaseAvailability() );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        // TODO This is probably too coarse-grained and we should have some strategy per user of config instead
        life.add( new ConfigurationChangedRestarter() );
    }

    protected TransactionStateFactory createTransactionStateFactory()
    {
        return new TransactionStateFactory( logging );
    }

    protected XaDataSourceManager createXaDataSourceManager()
    {
        return new XaDataSourceManager( logging.getLogger( XaDataSourceManager.class ) );
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected KeyCreator createRelationshipTypeCreator()
    {
        return new DefaultRelationshipTypeCreator( logging );
    }
    
    protected KeyCreator createPropertyKeyCreator()
    {
        return new DefaultPropertyKeyCreator( logging );
    }

    private NodeManager createNodeManager( final boolean readOnly, final CacheProvider cacheType,
                                           Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( config, logging.getLogger( NodeManager.class ), this, txManager, persistenceManager,
                    persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                    createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager, statementContextProvider );
        }

        return new NodeManager( config, logging.getLogger( NodeManager.class ), this, txManager, persistenceManager,
                persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                createRelationshipLookups(), nodeCache, relCache, xaDataSourceManager, statementContextProvider );
    }

    private NodeManager createGuardedNodeManager( final boolean readOnly, final CacheProvider cacheType,
                                                  Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( config, logging.getLogger( NodeManager.class ), this, txManager, persistenceManager,
                    persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
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
                public RelationshipImpl getRelationshipForProxy( final long relId, final LockType lock )
                {
                    guard.check();
                    return super.getRelationshipForProxy( relId, lock );
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
                public Relationship createRelationship( final Node startNodeProxy, final NodeImpl startNode,
                                                        final Node endNode, final RelationshipType type )
                {
                    guard.check();
                    return super.createRelationship( startNodeProxy, startNode, endNode, type );
                }
            };
        }

        return new NodeManager( config, logging.getLogger( NodeManager.class ), this, txManager, persistenceManager,
                persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
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
            public RelationshipImpl getRelationshipForProxy( final long relId, final LockType lock )
            {
                guard.check();
                return super.getRelationshipForProxy( relId, lock );
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
            public Relationship createRelationship( final Node startNodeProxy, final NodeImpl startNode,
                                                    final Node endNode, final RelationshipType type )
            {
                guard.check();
                return super.createRelationship( startNodeProxy, startNode, endNode, type );
            }
        };
    }

    @Override
    public void shutdown()
    {
        try
        {
            life.shutdown();
        }
        catch ( LifecycleException throwable )
        {
            msgLog.logMessage( "Shutdown failed", throwable );
        }
    }

    protected StoreFactory createStoreFactory()
    {
        return new StoreFactory( config, idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem,
                logging.getLogger( StoreFactory.class ), txHook );
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
        return new DefaultCaches( msgLog );
    }

    protected RelationshipProxy.RelationshipLookups createRelationshipLookups()
    {
        return new RelationshipProxy.RelationshipLookups()
        {
            @Override
            public Node lookupNode( long nodeId )
            {
                // TODO: add CAS check here for requests not in tx to guard against shutdown
                return nodeManager.getNodeById( nodeId );
            }

            @Override
            public RelationshipImpl lookupRelationship( long relationshipId )
            {
                // TODO: add CAS check here for requests not in tx to guard against shutdown
                return nodeManager.getRelationshipForProxy( relationshipId, null );
            }

            @Override
            public RelationshipImpl lookupRelationship( long relationshipId, LockType lock )
            {
                return nodeManager.getRelationshipForProxy( relationshipId, lock );
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
                // TODO: add CAS check here for requests not in tx to guard against shutdown
                return nodeManager.getNodeForProxy( nodeId, null );
            }

            @Override
            public NodeImpl lookup( long nodeId, LockType lock )
            {
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

    protected TxHook createTxHook()
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
        Logging logging;
        try
        {
            getClass().getClassLoader().loadClass( "ch.qos.logback.classic.LoggerContext" );
            logging = new LogbackService( config, (LoggerContext) getSingleton().getLoggerFactory() );
        }
        catch ( ClassNotFoundException e )
        {
            logging = new ClassicLoggingService( config );
        }
        return life.add( logging );
    }

    protected void createNeoDataSource()
    {
        // Create DataSource
        try
        {
            // TODO IO stuff should be done in lifecycle. Refactor!
            neoDataSource = new NeoStoreXaDataSource( config,
                    storeFactory, lockManager, logging.getLogger( NeoStoreXaDataSource.class ),
                    xaFactory, stateFactory, cacheBridge,
                    transactionInterceptorProviders, jobScheduler, logging, updateableSchemaState, dependencyResolver );
            xaDataSourceManager.registerDataSource( neoDataSource );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Could not create Neo XA datasource", e );
        }
    }

    @Override
    public final String getStoreDir()
    {
        return storeDir.getPath();
    }

    @Override
    public StoreId getStoreId()
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
        try
        {
            if ( transactionRunning() )
            {
                return new PlaceboTransaction( txManager, txManager.getTransactionState() );
            }

            txManager.begin();
            return new TopLevelTransaction( txManager, txManager.getTransactionState() );
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

    protected boolean isEphemeral()
    {
        return false;
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
        return nodeManager.createNode();
    }

    @Override
    public Node createNode( Label... labels )
    {
        return nodeManager.createNode( labels );
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
            throw new NotFoundException( format("Relationship %d not found", id));
        }
        return nodeManager.getRelationshipById( id );
    }

    @Override
    public Node getReferenceNode()
    {
        return nodeManager.getReferenceNode();
    }

    @Override
    public TransactionBuilder tx()
    {
        return defaultTxBuilder;
    }

    @Override
    public Guard getGuard()
    {
        return guard;
    }

    @Override
    public KernelData getKernelData()
    {
        return extensions;
    }

    @Override
    public IndexManager index()
    {
        return indexManager;
    }

    @Override
    public Schema schema()
    {
        return schema;
    }

    // GraphDatabaseSPI implementation - THESE SHOULD EVENTUALLY BE REMOVED! DON'T ADD dependencies on these!
    public Config getConfig()
    {
        return config;
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public LockManager getLockManager()
    {
        return lockManager;
    }

    @Override
    public XaDataSourceManager getXaDataSourceManager()
    {
        return xaDataSourceManager;
    }

    @Override
    public TransactionManager getTxManager()
    {
        return txManager;
    }

    @Override
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return relationshipTypeHolder;
    }

    @Override
    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return diagnosticsManager;
    }

    @Override
    public PersistenceSource getPersistenceSource()
    {
        return persistenceSource;
    }

    @Override
    public final StringLogger getMessageLog()
    {
        return msgLog;
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return txIdGenerator;
    }

    @Override
    public KernelPanicEventGenerator getKernelPanicGenerator()
    {
        return kernelPanicEventGenerator;
    }

    private Iterable<Class<?>> getSettingsClasses( Iterable<Class<?>> settingsClasses,
                                                   Iterable<KernelExtensionFactory<?>> kernelExtensions, Iterable
            <CacheProvider> cacheProviders )
    {
        List<Class<?>> totalSettingsClasses = new ArrayList<Class<?>>();

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

        if ( getStoreId() != null ? !getStoreId().equals( that.getStoreId() ) : that.getStoreId() != null )
        {
            return false;
        }
        if ( !storeDir.equals( that.storeDir ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return storeDir.hashCode();
    }

    protected class DefaultKernelData extends KernelData implements Lifecycle
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
        public void init()
                throws Throwable
        {
        }

        @Override
        public void start()
                throws Throwable
        {
        }

        @Override
        public void stop()
                throws Throwable
        {
        }

        @Override
        public void shutdown()
        {
            super.shutdown();
        }
    }

    private class DefaultTxEventSyncHookFactory implements TxEventSyncHookFactory
    {
        @Override
        public TransactionEventsSyncHook create()
        {
            return transactionEventHandlers.hasHandlers() ?
                   new TransactionEventsSyncHook( transactionEventHandlers, txManager) : null;
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
                return (T) getConfig().getParams();
            }
            else if ( type.equals( Config.class ) )
            {
                return (T) getConfig();
            }
            else if ( GraphDatabaseService.class.isAssignableFrom( type ) )
            {
                return (T) InternalAbstractGraphDatabase.this;
            }
            else if ( TransactionManager.class.isAssignableFrom( type ) )
            {
                return (T) txManager;
            }
            else if ( LockManager.class.isAssignableFrom( type ) )
            {
                return (T) lockManager;
            }
            else if( StoreFactory.class.isAssignableFrom( type ) )
            {
                return (T) storeFactory;
            }
            else if ( StringLogger.class.isAssignableFrom( type ) )
            {
                return (T) msgLog;
            }
            else if ( Logging.class.isAssignableFrom( type ) )
            {
                return (T) logging;
            }
            else if ( IndexStore.class.isAssignableFrom( type ) )
            {
                return (T) indexStore;
            }
            else if ( XaFactory.class.isAssignableFrom( type ) )
            {
                return (T) xaFactory;
            }
            else if ( XaDataSourceManager.class.isAssignableFrom( type ) )
            {
                return (T) xaDataSourceManager;
            }
            else if ( FileSystemAbstraction.class.isAssignableFrom( type ) )
            {
                return (T) fileSystem;
            }
            else if ( Guard.class.isAssignableFrom( type ) )
            {
                return (T) guard;
            }
            else if ( IndexProviders.class.isAssignableFrom( type ) )
            {
                return (T) indexManager;
            }
            else if ( KernelData.class.isAssignableFrom( type ) )
            {
                return (T) extensions;
            }
            else if ( Logging.class.isAssignableFrom( type ) )
            {
                return (T) logging;
            }
            else if ( TransactionInterceptorProviders.class.isAssignableFrom( type ) )
            {
                return (T) transactionInterceptorProviders;
            }
            else if ( KernelExtensions.class.isAssignableFrom( type ) )
            {
                return (T) kernelExtensions;
            }
            else if ( NodeManager.class.isAssignableFrom( type ) )
            {
                return (T) nodeManager;
            }
            else if ( TransactionStateFactory.class.isAssignableFrom( type ) )
            {
                return (T) stateFactory;
            }
            else if ( TxIdGenerator.class.isAssignableFrom( type ) )
            {
                return (T) txIdGenerator;
            }
            else if ( DiagnosticsManager.class.isAssignableFrom( type ) )
            {
                return (T) diagnosticsManager;
            }
            else if ( PropertyIndexManager.class.isAssignableFrom( type ) )
            {
                return (T) propertyIndexManager;
            }
            else if ( PersistenceManager.class.isAssignableFrom( type ) )
            {
                return (T) persistenceManager;
            }
            else if ( KernelAPI.class.equals( type ) )
            {
                return (T) kernelAPI;
            }
            else if ( ThreadToStatementContextBridge.class.isAssignableFrom( type ) )
            {
                return (T) statementContextProvider;
            }
            else if ( CacheAccessBackDoor.class.isAssignableFrom( type ) )
            {
                return (T) cacheBridge;
            }
            else if ( StoreLockerLifecycleAdapter.class.isAssignableFrom( type ) )
            {
                return (T) storeLocker;
            }
            else if ( IndexManager.class == type )
            {
                return type.cast( indexManager );
            }
            else if ( IndexingService.class.isAssignableFrom( type ) )
            {
                return (T) neoDataSource.getIndexService();
            }
            else if ( JobScheduler.class.isAssignableFrom( type ) )
            {
                return (T) jobScheduler;
            }
            else if ( DependencyResolver.class.isAssignableFrom( type ) )
            {
                return (T) DependencyResolverImpl.this;
            }
            return null;
        }
        
        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy<T> selector )
        {
            // Try known single dependencies
            T result = resolveKnownSingleDependency( type );
            if ( result != null )
                return selector.select( type, Iterables.option( result ) );
            
            // Try with kernel extensions
            return kernelExtensions.resolveDependency( type, selector );
        }
    }

    class DatabaseStartup
            implements Lifecycle
    {
        @Override
        public void init()
                throws Throwable
        {
        }

        @Override
        public void start()
                throws Throwable
        {
        }

        @Override
        public void stop()
                throws Throwable
        {
        }

        @Override
        public void shutdown()
                throws Throwable
        {
        }
    }

    /**
     * This class handles whether the database as a whole is available to use at all.
     * As it runs as the last service in the lifecycle list, the stop() is called first
     * on stop, shutdown or restart, and thus blocks access to everything else for outsiders.
     */
    class DatabaseAvailability
            implements Lifecycle
    {
        @Override
        public void init()
                throws Throwable
        {
            // TODO: Starting database. Make sure none can access it through lock or CAS
        }

        @Override
        public void start()
                throws Throwable
        {
            // TODO: Starting database. Make sure none can access it through lock or CAS
            msgLog.logMessage( "Started - database is now available" );
        }

        @Override
        public void stop()
                throws Throwable
        {
            // TODO: Starting database. Make sure none can access it through lock or CAS
            msgLog.logMessage( "Stopping - database is now unavailable" );
        }

        @Override
        public void shutdown()
                throws Throwable
        {
            // TODO: Starting database. Make sure none can access it through lock or CAS
        }
    }

    private class ConfigurationChangedRestarter
            extends LifecycleAdapter
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
        public void start()
                throws Throwable
        {
            config.addConfigurationChangeListener( listener );
        }

        @Override
        public void stop()
                throws Throwable
        {
            config.removeConfigurationChangeListener( listener );
        }
    }

    @Override
    public Iterable<Node> findNodesByLabelAndProperty( final Label myLabel, final String propertyName,
                                                       final Object value )
    {
        StatementContext ctx = statementContextProvider.getCtxForReading();

        long propertyId;
        long labelId;
        try
        {
            propertyId = ctx.getPropertyKeyId( propertyName );
            labelId = ctx.getLabelId( myLabel.name() );
        }
        catch ( KernelException e )
        {
            return Iterables.empty();
        }

        try
        {
            IndexRule indexRule = ctx.getIndexRule( labelId, propertyId );
            if(ctx.getIndexState( indexRule ) == InternalIndexState.ONLINE)
            {
                // Ha! We found an index - let's use it to find matching nodes
                return map2nodes( ctx.exactIndexLookup( indexRule.getId(), value ) );
            }
        }
        catch ( SchemaRuleNotFoundException e )
        {
            // If we don't find a matching index rule, we'll scan all nodes and filter manually (below)
        }
        catch ( IndexNotFoundKernelException e )
        {
            // If we don't find a matching index rule, we'll scan all nodes and filter manually (below)
        }
        
        return getNodesByLabelAndPropertyWithoutIndex( propertyName, value, ctx, labelId );
    }

    private Iterable<Node> getNodesByLabelAndPropertyWithoutIndex( final String propertyName, final Object value, StatementContext ctx, long labelId )
    {
        Iterable<Long> nodesWithLabel = ctx.getNodesWithLabel( labelId );
        final NodeProxy.NodeLookup lookup = createNodeLookup();

        Iterable<Long> matches = filter( new Predicate<Long>()
        {
            @Override
            public boolean accept( Long item )
            {
                Object propertyValue = lookup.lookup( item ).getProperty( nodeManager, propertyName, null );
                return propertyValue != null && propertyValue.equals( value );
            }
        }, nodesWithLabel );

        return map2nodes( matches );
    }

    private Iterable<Node> map2nodes( Iterable<Long> input )
    {
        return map( new Function<Long, Node>()
        {
            @Override
            public Node apply( Long id )
            {
                return getNodeById( id );
            }
        }, input );
    }
}
