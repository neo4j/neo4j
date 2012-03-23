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
import java.io.FileInputStream;
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
import java.util.Properties;
import java.util.Set;

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
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.cache.MeasureDoNothing;
import org.neo4j.kernel.impl.cache.MonitorGc;
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
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
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
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.tooling.GlobalGraphOperations;


/**
 * Exposes the methods {@link #getManagementBeans(Class)}() a.s.o.
 */
public abstract class AbstractGraphDatabase
        implements GraphDatabaseService, GraphDatabaseSPI
{

    interface Configuration
    {
        boolean read_only(boolean def);

        NodeManager.CacheType cache_type(NodeManager.CacheType def);

        boolean load_kernel_extensions( boolean def );
    }

    private static final NodeManager.CacheType DEFAULT_CACHE_TYPE = NodeManager.CacheType.soft;
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    protected String storeDir;
    protected Map<String, String> params;
    private StoreId storeId;
    private Transaction placeboTransaction = null;
    private final TransactionBuilder defaultTxBuilder = new TransactionBuilderImpl( this, ForceMode.forced );

    protected StringLogger msgLog;
    protected KernelEventHandlers kernelEventHandlers;
    protected TransactionEventHandlers transactionEventHandlers;
    protected RelationshipTypeHolder relationshipTypeHolder;
    protected NodeManager nodeManager;
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
    
    protected MeasureDoNothing monitorGc;

    protected NodeAutoIndexerImpl nodeAutoIndexer;
    protected RelationshipAutoIndexerImpl relAutoIndexer;
    protected KernelData extensions;

    private final LifeSupport life = new LifeSupport();

    protected AbstractGraphDatabase(String storeDir, Map<String, String> params)
    {
        this.params = params;
        this.storeDir = FileUtils.fixSeparatorsInPath( canonicalize( storeDir ));
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
        // Instantiate all services - some are overridable by subclasses
        this.msgLog = createStringLogger();
        params = new ConfigurationMigrator(msgLog).migrateConfiguration( params );

        Configuration conf = ConfigProxy.config(params, Configuration.class);

        boolean readOnly = conf.read_only(false);

        NodeManager.CacheType cacheType = conf.cache_type(DEFAULT_CACHE_TYPE);

        kernelEventHandlers = new KernelEventHandlers();

        diagnosticsManager = life.add(new DiagnosticsManager( msgLog ));

        kernelPanicEventGenerator = new KernelPanicEventGenerator( kernelEventHandlers );

        txHook = createTxHook();

        fileSystem = life.add(createFileSystemAbstraction());

        xaDataSourceManager = life.add(new XaDataSourceManager(msgLog));

        if (readOnly)
        {
            txManager = new ReadOnlyTxManager(xaDataSourceManager);

        } else
        {
            String serviceName = params.get( Config.TXMANAGER_IMPLEMENTATION );
            if ( serviceName == null )
            {
                txManager = new TxManager( this.storeDir, xaDataSourceManager, kernelPanicEventGenerator, txHook, msgLog, fileSystem);
            }
            else {
                TransactionManagerProvider provider;
                provider = Service.load(TransactionManagerProvider.class, serviceName);
                if ( provider == null )
                {
                    throw new IllegalStateException( "Unknown transaction manager implementation: "
                            + serviceName );
                }
                txManager = provider.loadTransactionManager( this.storeDir, kernelPanicEventGenerator, txHook, msgLog, fileSystem);
            }
        }
        life.add( txManager );

        transactionEventHandlers = new TransactionEventHandlers(txManager);

        txIdGenerator = createTxIdGenerator();

        ragManager = new RagManager(txManager);
        lockManager = createLockManager();

        idGeneratorFactory = createIdGeneratorFactory();

        relationshipTypeCreator = new DefaultRelationshipTypeCreator();

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

        nodeManager = !readOnly ? 
                
                new NodeManager( ConfigProxy.config(params, NodeManager.Configuration.class), this,
                        lockManager, lockReleaser, txManager,
                        persistenceManager, persistenceSource, relationshipTypeHolder, cacheType, propertyIndexManager,
                        createNodeLookup(), createRelationshipLookups(), msgLog, diagnosticsManager ) :
                    
                new ReadOnlyNodeManager( ConfigProxy.config(params, NodeManager.Configuration.class), this,
                        lockManager, lockReleaser,
                        txManager, persistenceManager, persistenceSource,
                        relationshipTypeHolder, cacheType, propertyIndexManager,
                        createNodeLookup(), createRelationshipLookups(), msgLog, diagnosticsManager );
                
        life.add( nodeManager );

        lockReleaser.setNodeManager(nodeManager); // TODO Another cyclic dep that needs to be refactored

        indexStore = new IndexStore( this.storeDir, fileSystem);

        // Default settings that need to be available
        // TODO THIS IS A SMELL - SHOULD BE AVAILABLE THROUGH OTHER MEANS!
        String separator = System.getProperty( "file.separator" );
        String store = this.storeDir + separator + NeoStore.DEFAULT_NAME;
        params.put( "store_dir", this.storeDir );
        params.put( "neo_store", store );
        String logicalLog = this.storeDir + separator + NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
        params.put( "logical_log", logicalLog );
        // END SMELL

        config = new Config( fileSystem, this.storeDir,  params );
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

        if ( conf.load_kernel_extensions(true))
        {
            life.add(new DefaultKernelExtensionLoader( extensions ));
        }

        indexManager = new IndexManagerImpl(config, indexStore, xaDataSourceManager, txManager, this);
        nodeAutoIndexer = life.add(new NodeAutoIndexerImpl( ConfigProxy.config( params, NodeAutoIndexerImpl.Configuration.class ), indexManager, nodeManager));
        relAutoIndexer = life.add(new RelationshipAutoIndexerImpl( ConfigProxy.config( params, RelationshipAutoIndexerImpl.Configuration.class ), indexManager, nodeManager));

        // TODO This cyclic dependency should be resolved
        indexManager.setNodeAutoIndexer( nodeAutoIndexer );
        indexManager.setRelAutoIndexer( relAutoIndexer );

        recoveryVerifier = createRecoveryVerifier();

        // Factories for things that needs to be created later
        storeFactory = createStoreFactory();
        xaFactory = new XaFactory(params, txIdGenerator, txManager, logBufferFactory, fileSystem, msgLog, recoveryVerifier );

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
            neoDataSource = new NeoStoreXaDataSource( ConfigProxy.config( params, NeoStoreXaDataSource.Configuration.class ),
                                                      fileSystem, storeFactory, lockManager, lockReleaser, msgLog, xaFactory,
                                                      providers, new DependencyResolverImpl() );
            xaDataSourceManager.registerDataSource( neoDataSource );
        } catch (IOException e)
        {
            throw new IllegalStateException("Could not create Neo XA datasource", e);
        }

        life.add( new StuffToDoAfterRecovery() );

        life.add( new MonitorGc( ConfigProxy.config( params, MonitorGc.Configuration.class ), msgLog ) );
        
        // This is how we lock the entire database to avoid threads using it during lifecycle events
        life.add( new DatabaseAvailability() );

        // Kernel event handlers should be the very last, i.e. very first to receive shutdown events
        life.add( kernelEventHandlers );
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
        return new StoreFactory(params, idGeneratorFactory, fileSystem, lastCommittedTxIdSetter, msgLog, txHook);
    }

    protected RecoveryVerifier createRecoveryVerifier()
    {
        return CommonFactories.defaultRecoveryVerifier();
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
                return AbstractGraphDatabase.this;
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
                return AbstractGraphDatabase.this;
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
        return new CommonFactories.DefaultIdGeneratorFactory();
    }

    protected LockManager createLockManager()
    {
        return new LockManager(ragManager);
    }

    protected StringLogger createStringLogger()
    {
        final StringLogger stringLogger = StringLogger.logger( this.storeDir );
        life.add( new LifecycleAdapter()
        {
            @Override
            public void shutdown()
                throws Throwable
            {
                stringLogger.close();
            }
        });
        return stringLogger;
    }

    public final String getStoreDir()
    {
        return storeDir;
    }

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
            if ( placeboTransaction == null )
            {
                placeboTransaction = new PlaceboTransaction(
                        txManager );
            }
            return placeboTransaction;
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

    /**
     * Get a single management bean. Delegates to {@link #getSingleManagementBean(Class)}.
     *
     * @deprecated since Neo4j may now have multiple beans implementing the same bean interface, this method has been
     *             deprecated in favor of {@link #getSingleManagementBean(Class)} and {@link #getManagementBeans(Class)}
     *             . Version 1.5 of Neo4j will be the last version to contain this method.
     */
    @Deprecated
    public final <T> T getManagementBean( Class<T> type )
    {
        return getSingleManagementBean( type );
    }

    public final <T> T getSingleManagementBean( Class<T> type )
    {
        Iterator<T> beans = getManagementBeans( type ).iterator();
        if ( beans.hasNext() )
        {
            T bean = beans.next();
            if ( beans.hasNext() )
                throw new NotFoundException( "More than one management bean for " + type.getName() );
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

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.registerKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.registerTransactionEventHandler( handler );
    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.unregisterKernelEventHandler( handler );
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.unregisterTransactionEventHandler( handler );
    }

    /**
     * A non-standard Convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most
     * likely be removed in future releases.
     *
     * @param file the property file to load
     * @return a map containing the properties from the file
     * @throws IllegalArgumentException if file does not exist
     */
    public static Map<String,String> loadConfigurations( String file )
    {
        Properties props = new Properties();
        try
        {
            FileInputStream stream = new FileInputStream( new File( file ) );
            try
            {
                props.load( stream );
            }
            finally
            {
                stream.close();
            }
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException( "Unable to load " + file, e );
        }
        Set<Map.Entry<Object,Object>> entries = props.entrySet();
        Map<String,String> stringProps = new HashMap<String,String>();
        for ( Map.Entry<Object,Object> entry : entries )
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            stringProps.put( key, value );
        }
        return stringProps;
    }

    public Node createNode()
    {
        return nodeManager.createNode();
    }

    public Node getNodeById( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new NotFoundException( "Node[" + id + "]" );
        }
        return nodeManager.getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( "Relationship[" + id + "]" );
        }
        return nodeManager.getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return nodeManager.getReferenceNode();
    }

    public TransactionBuilder tx()
    {
        return defaultTxBuilder;
    }

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
                    if ( result == null ) return Collections.emptySet();
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

    public KernelData getKernelData()
    {
        return extensions;
    }

    public IndexManager index()
    {
        return indexManager;
    }

    // GraphDatabaseSPI implementation - THESE SHOULD EVENTUALLY BE REMOVED! DON'T ADD dependencies on these!
    public Config getConfig()
    {
        return config;
    }

    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    public LockReleaser getLockReleaser()
    {
        return lockReleaser;
    }

    public LockManager getLockManager()
    {
        return lockManager;
    }

    public XaDataSourceManager getXaDataSourceManager()
    {
        return xaDataSourceManager;
    }

    public TransactionManager getTxManager()
    {
        return txManager;
    }

    @Override
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return relationshipTypeHolder;
    }

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

    public final StringLogger getMessageLog()
    {
        return msgLog;
    }

    @Override
    public KernelPanicEventGenerator getKernelPanicGenerator()
    {
        return kernelPanicEventGenerator;
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
        if( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        AbstractGraphDatabase that = (AbstractGraphDatabase) o;

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
        private final GraphDatabaseSPI graphDb;

        public DefaultKernelData(Config config, GraphDatabaseSPI graphDb)
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
            return params;
        }

        @Override
        public GraphDatabaseSPI graphDatabase()
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
            loaded = extensions.loadExtensionConfigurations( msgLog );
            loadIndexImplementations(indexManager, msgLog);
        }

        @Override
        public void start()
            throws Throwable
        {
            extensions.loadExtensions( loaded, msgLog );
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
            for ( IndexProvider index : Service.load( IndexProvider.class ) )
            {
                try
                {
                    indexes.addProvider( index.identifier(), index.load( new DependencyResolverImpl() ) );
                }
                catch ( Throwable cause )
                {
                    msgLog.logMessage( "Failed to load index provider " + index.identifier(), cause );
                    if ( isAnUpgradeProblem( cause ) ) throw launderedException( cause );
                    else cause.printStackTrace();
                }
            }
        }


        private boolean isAnUpgradeProblem( Throwable cause )
        {
            while ( cause != null )
            {
                if ( cause instanceof Throwable ) return true;
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

    class DependencyResolverImpl
            implements DependencyResolver
    {
        @Override
        public <T> T resolveDependency(Class<T> type)
        {
            if (type.equals(Map.class))
                return (T) params;
            else if (GraphDatabaseService.class.isAssignableFrom(type))
                return (T) AbstractGraphDatabase.this;
            else if (TransactionManager.class.isAssignableFrom(type))
                return (T) txManager;
            else if (LockManager.class.isAssignableFrom(type))
                return (T) lockManager;
            else if (LockReleaser.class.isAssignableFrom(type))
                return (T) lockReleaser;
            else if (StoreFactory.class.isAssignableFrom(type))
                return (T) storeFactory;
            else if (StringLogger.class.isAssignableFrom(type))
                return (T) msgLog;
            else if (IndexStore.class.isAssignableFrom(type))
                return (T) indexStore;
            else if (XaFactory.class.isAssignableFrom(type))
                return (T) xaFactory;
            else if (XaDataSourceManager.class.isAssignableFrom(type))
                return (T) xaDataSourceManager;
            else if (FileSystemAbstraction.class.isAssignableFrom(type))
                return (T) fileSystem;
            else
                throw new IllegalArgumentException("Could not resolve dependency of type:"+type.getName());
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
        }

        @Override
        public void stop()
            throws Throwable
        {
            // TODO: Starting database. Make sure none can access it through lock or CAS
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
            KernelDiagnostics.register( diagnosticsManager, AbstractGraphDatabase.this,
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
}
