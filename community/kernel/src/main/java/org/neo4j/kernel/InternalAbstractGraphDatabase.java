/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.helpers.Exceptions.launderedException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationChange;
import org.neo4j.kernel.configuration.ConfigurationChangeListener;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MeasureDoNothing;
import org.neo4j.kernel.impl.cache.MonitorGc;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.DefaultCaches;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.core.ReadOnlyNodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.ReadOnlyTxManager;
import org.neo4j.kernel.impl.transaction.TransactionManagerProvider;
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
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.LogbackService;
import org.neo4j.kernel.logging.Loggers;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * Exposes the methods {@link #getManagementBeans(Class)}() a.s.o.
 */
public abstract class InternalAbstractGraphDatabase
        extends AbstractGraphDatabase implements GraphDatabaseService, GraphDatabaseAPI
{
    public static class Configuration
    {
        public static final GraphDatabaseSetting.BooleanSetting dump_configuration = GraphDatabaseSettings.dump_configuration;
        public static final GraphDatabaseSetting.BooleanSetting read_only = GraphDatabaseSettings.read_only;
        public static final GraphDatabaseSetting.BooleanSetting use_memory_mapped_buffers = GraphDatabaseSettings.use_memory_mapped_buffers;
        public static final GraphDatabaseSetting.BooleanSetting execution_guard_enabled = GraphDatabaseSettings.execution_guard_enabled;
        public static final GraphDatabaseSettings.CacheTypeSetting cache_type = GraphDatabaseSettings.cache_type;
        public static final GraphDatabaseSetting.BooleanSetting load_kernel_extensions = GraphDatabaseSettings.load_kernel_extensions;
        public static final GraphDatabaseSetting.BooleanSetting ephemeral = new GraphDatabaseSetting.BooleanSetting("ephemeral");

        public static final GraphDatabaseSetting.DirectorySetting store_dir = GraphDatabaseSettings.store_dir;
        public static final GraphDatabaseSetting.FileSetting neo_store = GraphDatabaseSettings.neo_store;
        public static final GraphDatabaseSetting.FileSetting logical_log = GraphDatabaseSettings.logical_log;
    }

    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    protected String storeDir;
    protected Map<String, String> params;
    private final Iterable<KernelExtension> kernelExtensions;
    protected StoreId storeId;
    private final TransactionBuilder defaultTxBuilder = new TransactionBuilderImpl( this, ForceMode.forced );

    protected Logging logging;
    protected StringLogger msgLog;
    protected KernelEventHandlers kernelEventHandlers;
    protected TransactionEventHandlers transactionEventHandlers;
    protected RelationshipTypeHolder relationshipTypeHolder;
    protected NodeManager nodeManager;
    protected Iterable<IndexProvider> indexProviders;
    protected IndexManagerImpl indexManager;
    protected Config config;
    protected KernelPanicEventGenerator kernelPanicEventGenerator;
    protected TxHook txHook;
    protected FileSystemAbstraction fileSystem;
    protected XaDataSourceManager xaDataSourceManager;
    protected RagManager ragManager;
    protected LockManager lockManager;
    protected IdGeneratorFactory idGeneratorFactory;
    protected RelationshipTypeCreator relationshipTypeCreator;
    protected LastCommittedTxIdSetter lastCommittedTxIdSetter;
    protected NioNeoDbPersistenceSource persistenceSource;
    protected TxEventSyncHookFactory syncHook;
    protected PersistenceManager persistenceManager;
    protected PropertyIndexManager propertyIndexManager;
    protected LockReleaser lockReleaser;
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

    protected MeasureDoNothing monitorGc;

    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;
    protected Caches caches;

    protected final LifeSupport life = new LifeSupport();
    private final Map<String,CacheProvider> cacheProviders;

    protected InternalAbstractGraphDatabase( String storeDir, Map<String, String> params,
                                             Iterable<IndexProvider> indexProviders,
                                             Iterable<KernelExtension> kernelExtensions,
                                             Iterable<CacheProvider> cacheProviders )
    {
        this.params = params;
        this.cacheProviders = mapCacheProviders( cacheProviders );

        // SPI - provided services
        this.indexProviders = indexProviders;
        this.kernelExtensions = kernelExtensions;

        // Setup configuration
        params.put(Configuration.store_dir.name(), storeDir);
        config = new Config( params, getSettingsClasses() );
        
        this.storeDir = config.get(Configuration.store_dir);
    }

    private Map<String, CacheProvider> mapCacheProviders( Iterable<CacheProvider> cacheProviders )
    {
        Map<String, CacheProvider> map = new HashMap<String, CacheProvider>();
        for ( CacheProvider provider : cacheProviders )
            map.put( provider.getName(), provider );
        return map;
    }

    protected void run()
    {
        create();

        try
        {
            life.start();
        }
        catch( LifecycleException throwable )
        {
            msgLog.logMessage( "Startup failed", throwable );

            shutdown();

//            throw new IllegalStateException( "Startup failed", throwable );
            throw throwable;
        }
    }

    private void create()
    {

        fileSystem = life.add(createFileSystemAbstraction());

        // Create logger
        this.logging = createStringLogger();

        // Apply autoconfiguration for memory settings
        AutoConfigurator autoConfigurator = new AutoConfigurator( fileSystem,
                                                                  config.get( NeoStoreXaDataSource.Configuration.store_dir ),
                                                                  config.get( GraphDatabaseSettings.use_memory_mapped_buffers ),
                                                                  config.get( GraphDatabaseSettings.dump_configuration ) );
        Map<String, String> configParams = config.getParams();
        Map<String,String> autoConfiguration = autoConfigurator.configure( );
        for( Map.Entry<String, String> autoConfig : autoConfiguration.entrySet() )
        {
            // Don't override explicit settings
            if( !params.containsKey( autoConfig.getKey() ) )
            {
            	String key = autoConfig.getKey();
                configParams.put( autoConfig.getKey(), autoConfig.getValue() );
            }
        }

        config.applyChanges( configParams );

        this.msgLog = logging.getLogger( Loggers.NEO4J );
        
        config.setLogger(msgLog);

        // Instantiate all services - some are overridable by subclasses
        boolean readOnly = config.get( Configuration.read_only );

        String cacheTypeName = config.get( Configuration.cache_type );
        CacheProvider cacheProvider = cacheProviders.get( cacheTypeName );
        if ( cacheProvider == null )
            throw new IllegalArgumentException( "No cache type '" + cacheTypeName + "'" );

        kernelEventHandlers = new KernelEventHandlers();

        caches = createCaches();
        diagnosticsManager = life.add(new DiagnosticsManager( logging.getLogger( Loggers.DIAGNOSTICS )) );

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        txHook = createTxHook();

        xaDataSourceManager = life.add( new XaDataSourceManager( logging.getLogger( Loggers.DATASOURCE )) );

        guard = config.get( Configuration.execution_guard_enabled ) ? new Guard( msgLog ) : null;

        xaDataSourceManager = life.add(new XaDataSourceManager(msgLog));

        if (readOnly)
        {
            txManager = new ReadOnlyTxManager(xaDataSourceManager);

        } else
        {
            String serviceName = config.get( GraphDatabaseSettings.tx_manager_impl );
            if ( serviceName == null )
            {
                txManager = new TxManager( this.storeDir, xaDataSourceManager, kernelPanicEventGenerator, txHook, logging.getLogger( Loggers.TXMANAGER ), fileSystem);
            }
            else {
                TransactionManagerProvider provider;
                provider = Service.load(TransactionManagerProvider.class, serviceName);
                if ( provider == null )
                {
                    throw new IllegalStateException( "Unknown transaction manager implementation: "
                            + serviceName );
                }
                txManager = provider.loadTransactionManager( this.storeDir, xaDataSourceManager, kernelPanicEventGenerator, txHook, logging.getLogger( Loggers.TXMANAGER ), fileSystem);
            }
        }
        life.add( txManager );

        transactionEventHandlers = new TransactionEventHandlers(txManager);

        txIdGenerator = life.add( createTxIdGenerator() );

        ragManager = new RagManager(txManager );
        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        relationshipTypeCreator = createRelationshipTypeCreator();

        lastCommittedTxIdSetter = createLastCommittedTxIdSetter();

        persistenceSource = life.add(new NioNeoDbPersistenceSource(xaDataSourceManager));

        syncHook = new DefaultTxEventSyncHookFactory();

        // TODO Cyclic dependency! lockReleaser is null here
        persistenceManager = new PersistenceManager(txManager,
                persistenceSource, syncHook, lockReleaser );

        propertyIndexManager = life.add(new PropertyIndexManager(
                txManager, persistenceManager, persistenceSource));

        lockReleaser = new LockReleaser(lockManager, txManager, nodeManager, propertyIndexManager);
        persistenceManager.setLockReleaser(lockReleaser); // TODO This cyclic dep needs to be refactored

        relationshipTypeHolder = new RelationshipTypeHolder( txManager,
            persistenceManager, persistenceSource, relationshipTypeCreator );

        caches.configure( cacheProvider, config );
        Cache<NodeImpl> nodeCache = diagnosticsManager.tryAppendProvider( caches.node() );
        Cache<RelationshipImpl> relCache = diagnosticsManager.tryAppendProvider( caches.relationship() );

        nodeManager = guard != null ?
                createGuardedNodeManager( readOnly, cacheProvider, nodeCache, relCache ) :
                createNodeManager( readOnly, cacheProvider, nodeCache, relCache );

        life.add( nodeManager );

        lockReleaser.setNodeManager(nodeManager); // TODO Another cyclic dep that needs to be refactored

        indexStore = new IndexStore( this.storeDir, fileSystem);

        diagnosticsManager.prependProvider( config );

        // Config can auto-configure memory mapping settings and what not, so reassign params
        // after we've instantiated Config.
        params = config.getParams();

        /*
         *  LogBufferFactory needs access to the parameters so it has to be added after the default and
         *  user supplied configurations are consolidated
         */

        logBufferFactory = new DefaultLogBufferFactory();

        extensions = life.add(createKernelData());

        if ( config.get( Configuration.load_kernel_extensions ))
        {
            life.add(new DefaultKernelExtensionLoader( extensions ));
        }

        if( indexProviders == null )
        {
            indexProviders = new LegacyIndexIterable();
        }
        indexManager = new IndexManagerImpl(config, indexStore, xaDataSourceManager, txManager, this);
        nodeAutoIndexer = life.add(new NodeAutoIndexerImpl( config, indexManager, nodeManager));
        relAutoIndexer = life.add(new RelationshipAutoIndexerImpl( config, indexManager, nodeManager));

        // TODO This cyclic dependency should be resolved
        indexManager.setNodeAutoIndexer( nodeAutoIndexer );
        indexManager.setRelAutoIndexer( relAutoIndexer );

        recoveryVerifier = createRecoveryVerifier();

        // Factories for things that needs to be created later
        storeFactory = createStoreFactory();
        String keepLogicalLogsConfig = config.get( GraphDatabaseSettings.keep_logical_logs );
        xaFactory = new XaFactory(config, txIdGenerator, txManager, logBufferFactory, fileSystem,
                logging.getLogger( Loggers.XAFACTORY), recoveryVerifier, LogPruneStrategies.fromConfigValue( fileSystem, keepLogicalLogsConfig ) );

        // Create DataSource
        List<Pair<TransactionInterceptorProvider, Object>> providers = new ArrayList<Pair<TransactionInterceptorProvider, Object>>( 2 );
        for ( TransactionInterceptorProvider provider : Service.load( TransactionInterceptorProvider.class ) )
        {
            Object prov = params.get( TransactionInterceptorProvider.class.getSimpleName() + "." + provider.name() );
            if ( prov != null )
            {
                providers.add( Pair.of( provider, prov ) );
            }
        }

        try
        {
            // TODO IO stuff should be done in lifecycle. Refactor!
            neoDataSource = new NeoStoreXaDataSource( config,
                    storeFactory, fileSystem, lockManager, lockReleaser, logging.getLogger( Loggers.DATASOURCE ), xaFactory, providers, new DependencyResolverImpl());
            xaDataSourceManager.registerDataSource( neoDataSource );
        } catch (IOException e)
        {
            throw new IllegalStateException("Could not create Neo XA datasource", e);
        }

        life.add( new StuffToDoAfterRecovery() );

        life.add( new MonitorGc( config, msgLog ) );

        // This is how we lock the entire database to avoid threads using it during lifecycle events
        life.add( new DatabaseAvailability() );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );

        // TODO This is probably too coarse-grained and we should have some strategy per user of config instead
        life.add( new ConfigurationChangedRestarter() );
    }

    protected RelationshipTypeCreator createRelationshipTypeCreator()
    {
        return new DefaultRelationshipTypeCreator();
    }

    private NodeManager createNodeManager( final boolean readOnly, final CacheProvider cacheType,
            Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( config, this, lockManager, lockReleaser, txManager, persistenceManager,
                    persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                    createRelationshipLookups(), nodeCache, relCache );
        }

        return new NodeManager( config, this, lockManager, lockReleaser, txManager, persistenceManager,
                persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                createRelationshipLookups(), nodeCache, relCache );
    }

    private NodeManager createGuardedNodeManager( final boolean readOnly, final CacheProvider cacheType,
            Cache<NodeImpl> nodeCache, Cache<RelationshipImpl> relCache )
    {
        if ( readOnly )
        {
            return new ReadOnlyNodeManager( config, this, lockManager, lockReleaser, txManager, persistenceManager,
                    persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                    createRelationshipLookups(), nodeCache, relCache )
            {
                @Override
                protected Node getNodeByIdOrNull( final long nodeId )
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

        return new NodeManager( config, this, lockManager, lockReleaser, txManager, persistenceManager,
                persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager, createNodeLookup(),
                createRelationshipLookups(), nodeCache, relCache )
        {
            @Override
            protected Node getNodeByIdOrNull( final long nodeId )
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
        catch( LifecycleException throwable )
        {
            msgLog.logMessage( "Shutdown failed", throwable );
        }
    }

    protected StoreFactory createStoreFactory()
    {
        return new StoreFactory(config, idGeneratorFactory, fileSystem, lastCommittedTxIdSetter, logging.getLogger( Loggers.NEOSTORE ), txHook);
    }

    protected RecoveryVerifier createRecoveryVerifier()
    {
        return RecoveryVerifier.ALWAYS_VALID;
    }

    protected KernelData createKernelData()
    {
        return new DefaultKernelData(config, this);
    }

    protected LastCommittedTxIdSetter createLastCommittedTxIdSetter()
    {
        return new DefaultLastCommittedTxIdSetter();
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
        return new LockManager(ragManager);
    }

    protected Logging createStringLogger()
    {
        try
        {
            getClass().getClassLoader().loadClass("ch.qos.logback.classic.LoggerContext");
            return life.add( new LogbackService( config ));
        }
        catch( ClassNotFoundException e )
        {
            return life.add( new ClassicLoggingService(config));
        }
    }

    @Override
	public final String getStoreDir()
    {
        return storeDir;
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

    Transaction beginTx( ForceMode forceMode )
    {
        if ( transactionRunning() )
        {
            return new PlaceboTransaction( txManager );
        }
        Transaction result = null;
        try
        {
            txManager.begin( forceMode );
            result = new TopLevelTransaction( txManager, lockManager, lockReleaser );
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Unable to begin transaction", e );
        }
        return result;
    }

    @Override
    public boolean transactionRunning()
    {
        try
        {
            return txManager.getTransaction() != null;
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                    "Unable to get transaction.", e );
        }
    }

    @Override
    public final <T> T getManagementBean( Class<T> type )
    {
        return getSingleManagementBean( type );
    }

    @Override
	public final <T> T getSingleManagementBean( Class<T> type )
    {
        Iterator<T> beans = getManagementBeans( type ).iterator();
        if ( beans.hasNext() )
        {
            T bean = beans.next();
            if( beans.hasNext() )
            {
                throw new NotFoundException( "More than one management bean for " + type.getName() );
            }
            return bean;
        }
        return null;
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
	public Node getNodeById( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new NotFoundException( "Node[" + id + "]" );
        }
        return nodeManager.getNodeById(id);
    }

    @Override
	public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( "Relationship[" + id + "]" );
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
	public <T> Collection<T> getManagementBeans( Class<T> beanClass )
    {
        KernelExtension<?> jmx = Service.load( KernelExtension.class, "kernel jmx" );
        if ( jmx != null )
        {
            Method getManagementBeans = null;
            Object state = jmx.getState( extensions );
            if ( state != null )
            {
                try
                {
                    getManagementBeans = state.getClass().getMethod( "getManagementBeans", Class.class );
                }
                catch ( Exception e )
                {
                    // getManagementBean will be null
                }
            }
            if ( getManagementBeans != null )
            {
                try
                {
                    @SuppressWarnings( "unchecked" ) Collection<T> result =
                            (Collection<T>) getManagementBeans.invoke( state, beanClass );
                    if( result == null )
                    {
                        return Collections.emptySet();
                    }
                    return result;
                }
                catch ( InvocationTargetException ex )
                {
                    Throwable cause = ex.getTargetException();
                    if ( cause instanceof Error )
                    {
                        throw (Error) cause;
                    }
                    if ( cause instanceof RuntimeException )
                    {
                        throw (RuntimeException) cause;
                    }
                }
                catch ( Exception ignored )
                {
                    // exception thrown below
                }
            }
        }
        throw new UnsupportedOperationException( "Neo4j JMX support not enabled" );
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
	public LockReleaser getLockReleaser()
    {
        return lockReleaser;
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
    
	private List<Class<?>> getSettingsClasses() {
		// Get the list of settings classes for extensions
        List<Class<?>> settingsClasses = new ArrayList<Class<?>>();
        settingsClasses.add( GraphDatabaseSettings.class );
        for( KernelExtension<?> kernelExtension : kernelExtensions )
        {
            Class<?> settingsClass = kernelExtension.getSettingsClass();
            if( settingsClass != null )
            {
                settingsClasses.add( settingsClass );
            }
        }
		return settingsClasses;
	}

    private String canonicalize( String path )
    {
        try
        {
            return new File( path ).getCanonicalFile().getAbsolutePath();
        }
        catch ( IOException e )
        {
            return new File( path ).getAbsolutePath();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if( this == o )
        {
            return true;
        }
        if ( o == null || !(o instanceof InternalAbstractGraphDatabase) )
        {
            return false;
        }

        InternalAbstractGraphDatabase that = (InternalAbstractGraphDatabase) o;

        if( getStoreId() != null ? !getStoreId().equals( that.getStoreId() ) : that.getStoreId() != null )
        {
            return false;
        }
        if( !storeDir.equals( that.storeDir ) )
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
        private final Config config;
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData(Config config, GraphDatabaseAPI graphDb)
        {
            this.config = config;
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public Config getConfig()
        {
            return config;
        }

        @Override
        public Map<String, String> getConfigParams()
        {
            return config.getParams();
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
            throws Throwable
        {
            // TODO This should be refactored so that shutdown does not need logger as input
            shutdown( msgLog );
        }
    }

    private class DefaultKernelExtensionLoader implements Lifecycle
    {
        private final KernelData extensions;

        private Collection<KernelExtension<?>> loaded;

        public DefaultKernelExtensionLoader(KernelData extensions)
        {
            this.extensions = extensions;
        }

        @Override
        public void init()
            throws Throwable
        {
            loaded = extensions.loadExtensionConfigurations( logging.getLogger( Loggers.EXTENSION ), kernelExtensions );
            loadIndexImplementations(indexManager, logging.getLogger( Loggers.INDEX));
        }

        @Override
        public void start()
            throws Throwable
        {
            extensions.loadExtensions( loaded, logging.getLogger( Loggers.EXTENSION ) );
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

        void loadIndexImplementations( IndexManagerImpl indexes, StringLogger msgLog )
        {
            for ( IndexProvider index : indexProviders)
            {
                try
                {
                    indexes.addProvider( index.identifier(), index.load( new DependencyResolverImpl() ) );
                }
                catch ( Throwable cause )
                {
                    msgLog.logMessage( "Failed to load index provider " + index.identifier(), cause );
                    if( isAnUpgradeProblem( cause ) )
                    {
                        throw launderedException( cause );
                    }
                    else
                    {
                        cause.printStackTrace();
                    }
                }
            }
        }

        private boolean isAnUpgradeProblem( Throwable cause )
        {
            while ( cause != null )
            {
                if( cause instanceof Throwable )
                {
                    return true;
                }
                cause = cause.getCause();
            }
            return false;
        }

    }

    private class DefaultTxEventSyncHookFactory implements TxEventSyncHookFactory
    {
        @Override
        public TransactionEventsSyncHook create()
        {
            return transactionEventHandlers.hasHandlers() ?
                   new TransactionEventsSyncHook( nodeManager, transactionEventHandlers, txManager) : null;
        }
    }

    /**
     * FIXME: This is supposed to be handled by a Dependency Injection framework...
     * @author ceefour
     */
    class DependencyResolverImpl
            implements DependencyResolver
    {
        @Override
        public <T> T resolveDependency(Class<T> type)
        {
            if( type.equals( Map.class ) )
            {
                return (T) getConfig().getParams();
            }
            else if( type.equals( Config.class ) )
            {
                return (T) getConfig();
            }
            else if( GraphDatabaseService.class.isAssignableFrom( type ) )
            {
                return (T) InternalAbstractGraphDatabase.this;
            }
            else if( TransactionManager.class.isAssignableFrom( type ) )
            {
                return (T) txManager;
            }
            else if( LockManager.class.isAssignableFrom( type ) )
            {
                return (T) lockManager;
            }
            else if( LockReleaser.class.isAssignableFrom( type ) )
            {
                return (T) lockReleaser;
            }
            else if( StoreFactory.class.isAssignableFrom( type ) )
            {
                return (T) storeFactory;
            }
            else if( StringLogger.class.isAssignableFrom( type ) )
            {
                return (T) msgLog;
            }
            else if( IndexStore.class.isAssignableFrom( type ) )
            {
                return (T) indexStore;
            }
            else if( XaFactory.class.isAssignableFrom( type ) )
            {
                return (T) xaFactory;
            }
            else if( XaDataSourceManager.class.isAssignableFrom( type ) )
            {
                return (T) xaDataSourceManager;
            }
            else if( FileSystemAbstraction.class.isAssignableFrom( type ) )
            {
                return (T) fileSystem;
            }
            else if( Guard.class.isAssignableFrom( type ) )
            {
                return (T) guard;
            }
            else
            {
                throw new IllegalArgumentException( "Could not resolve dependency of type:" + type.getName() );
            }
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

    // TODO Probably change name
    class StuffToDoAfterRecovery implements Lifecycle
    {
        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
            storeId = neoDataSource.getStoreId();
            KernelDiagnostics.register( diagnosticsManager, InternalAbstractGraphDatabase.this,
                    neoDataSource );
        }

        @Override
        public void stop() throws Throwable
        {
        }

        @Override
        public void shutdown() throws Throwable
        {
        }
    }

    private class ConfigurationChangedRestarter
        extends LifecycleAdapter
    {
        private final ConfigurationChangeListener listener = new ConfigurationChangeListener()
                    {
                        Executor executor = Executors.newSingleThreadExecutor( new DaemonThreadFactory( "Database configuration restart" ) );

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

                                        msgLog.logMessage( "Database restarted with the following configuration changes:" + change );
                                    }
                                    catch( LifecycleException e )
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
}
