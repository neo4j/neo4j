/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Logger;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.KernelExtension.Function;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipTypeCreator;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.core.TxEventSyncHookFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.index.IndexXaConnection;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxFinishHook;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

class EmbeddedGraphDbImpl
{
    private static final String KEY_INDEX_PROVIDER = "provider";
    private static final String KERNEL_VERSION = Version.get();

    private static Logger log =
        Logger.getLogger( EmbeddedGraphDbImpl.class.getName() );
    private Transaction placeboTransaction = null;
    private final GraphDbInstance graphDbInstance;
    private final GraphDatabaseService graphDbService;
    private final NodeManager nodeManager;
    private final String storeDir;

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();
    private final KernelPanicEventGenerator kernelPanicEventGenerator =
            new KernelPanicEventGenerator( kernelEventHandlers );

    private KernelExtension.KernelData extensions;
    
    private final IndexStore indexStore;
    private final Map<String, IndexProvider> indexProviders = new HashMap<String, IndexProvider>();
    private final StringLogger msgLog;

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     *
     * @param storeDir the store directory for the db files
     * @param config configuration parameters
     */
    public EmbeddedGraphDbImpl( String storeDir, Map<String, String> inputParams,
            GraphDatabaseService graphDbService, LockManagerFactory lockManagerFactory,
            IdGeneratorFactory idGeneratorFactory, RelationshipTypeCreator relTypeCreator,
            TxIdGeneratorFactory txIdFactory, TxFinishHook finishHook,
            LastCommittedTxIdSetter lastCommittedTxIdSetter )
    {
        this.storeDir = storeDir;
        TxModule txModule = newTxModule( inputParams, finishHook );
        LockManager lockManager = lockManagerFactory.create( txModule );
        LockReleaser lockReleaser = new LockReleaser( lockManager, txModule.getTxManager() );
        final Config config = new Config( graphDbService, storeDir, inputParams,
                kernelPanicEventGenerator, txModule, lockManager, lockReleaser, idGeneratorFactory,
                new SyncHookFactory(), relTypeCreator, txIdFactory.create( txModule.getTxManager() ),
                lastCommittedTxIdSetter );
        graphDbInstance = new GraphDbInstance( storeDir, true, config );
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        this.graphDbService = graphDbService;
        graphDbInstance.start( graphDbService, new KernelExtensionLoader()
        {
            public void load( final Map<Object, Object> params )
            {
                extensions = new KernelExtension.KernelData()
                {
                    @Override
                    public String version()
                    {
                        return KERNEL_VERSION;
                    }

                    @Override
                    public Config getConfig()
                    {
                        return config;
                    }

                    @Override
                    public Map<Object, Object> getConfigParams()
                    {
                        return params;
                    }

                    @Override
                    public GraphDatabaseService graphDatabase()
                    {
                        return EmbeddedGraphDbImpl.this.graphDbService;
                    }
                    
                    protected void loaded( KernelExtension extension )
                    {
                        if ( extension instanceof IndexProvider )
                        {
                            indexProviders.put( extension.getKey(), (IndexProvider) extension );
                        }
                    }
                };
                extensions.startup( msgLog );
            }
        });
        nodeManager = config.getGraphDbModule().getNodeManager();
        this.indexStore = graphDbInstance.getConfig().getIndexStore();
    }

    private TxModule newTxModule( Map<String, String> inputParams, TxFinishHook rollbackHook )
    {
        return Boolean.parseBoolean( inputParams.get( Config.READ_ONLY ) ) ? new TxModule( true,
                kernelPanicEventGenerator ) : new TxModule( this.storeDir,
                kernelPanicEventGenerator, rollbackHook );
    }

    <T> T getManagementBean( Class<T> beanClass )
    {
        KernelExtension jmx = Service.load( KernelExtension.class, "kernel jmx" );
        KernelExtension.Function<T> getBean = null;
        if ( jmx != null && jmx.isLoaded( extensions ) )
        {
            getBean = jmx.function( extensions, "getBean", beanClass, Class.class );
        }
        if ( getBean == null )
        {
            throw new UnsupportedOperationException( "Neo4j JMX support not enabled" );
        }
        return getBean.call( beanClass );
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
        Set<Entry<Object,Object>> entries = props.entrySet();
        Map<String,String> stringProps = new HashMap<String,String>();
        for ( Entry<Object,Object> entry : entries )
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
        if ( id < 0 || id > Integer.MAX_VALUE * 2l )
        {
            throw new NotFoundException( "Node[" + id + "]" );
        }
        return nodeManager.getNodeById( (int) id );
    }

    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > Integer.MAX_VALUE * 2l )
        {
            throw new NotFoundException( "Relationship[" + id + "]" );
        }
        return nodeManager.getRelationshipById( (int) id );
    }

    public Node getReferenceNode()
    {
        return nodeManager.getReferenceNode();
    }

    private boolean inShutdown = false;
    public synchronized void shutdown()
    {
        if ( inShutdown ) return;
        inShutdown = true;
        try
        {
            if ( graphDbInstance.started() )
            {
                sendShutdownEvent();
                extensions.shutdown( msgLog );
            }
            graphDbInstance.shutdown();
        }
        finally
        {
            inShutdown = false;
        }
    }

    private void sendShutdownEvent()
    {
        for ( KernelEventHandler handler : this.kernelEventHandlers )
        {
            handler.beforeShutdown();
        }
    }

    public boolean enableRemoteShell()
    {
        return this.enableRemoteShell( null );
    }

    public boolean enableRemoteShell( final Map<String, Serializable> config )
    {
        KernelExtension shell = Service.load( KernelExtension.class, "shell" );
        Function<Void> enable = null;
        if ( shell != null )
        {
            enable = shell.function( extensions, "enableRemoteShell", void.class, Map.class );
        }
        if ( enable == null )
        {
            log.info( "Shell library not available. Neo4j shell not "
                      + "started. Please add the Neo4j shell jar to the classpath." );
            return false;
        }
        enable.call( config );
        return true;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return graphDbInstance.getRelationshipTypes();
    }

    /**
     * @throws TransactionFailureException if unable to start transaction
     */
    public Transaction beginTx()
    {
        if ( graphDbInstance.transactionRunning() )
        {
            if ( placeboTransaction == null )
            {
                placeboTransaction = new PlaceboTransaction(
                        graphDbInstance.getTransactionManager() );
            }
            return placeboTransaction;
        }
        TransactionManager txManager = graphDbInstance.getTransactionManager();
        Transaction result = null;
        try
        {
            txManager.begin();
            result = new TopLevelTransaction( txManager );
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Unable to begin transaction", e );
        }
        return result;
    }

    /**
     * Returns a non-standard configuration object. Will most likely be removed
     * in future releases.
     *
     * @return a configuration object
     */
    public Config getConfig()
    {
        return graphDbInstance.getConfig();
    }

    @Override
    public String toString()
    {
        return super.toString() + " [" + storeDir + "]";
    }

    public String getStoreDir()
    {
        return storeDir;
    }

    public Iterable<Node> getAllNodes()
    {
        return new Iterable<Node>()
        {
            public Iterator<Node> iterator()
            {
                long highId =
                    (nodeManager.getHighestPossibleIdInUse( Node.class ) & 0xFFFFFFFFL);
                return new AllNodesIterator( highId );
            }
        };
    }

    // TODO: temporary all nodes getter, fix this with better implementation
    // (no NotFoundException to control flow)
    private class AllNodesIterator implements Iterator<Node>
    {
        private final long highId;
        private long currentNodeId = 0;
        private Node currentNode = null;

        AllNodesIterator( long highId )
        {
            this.highId = highId;
        }

        public synchronized boolean hasNext()
        {
            while ( currentNode == null && currentNodeId <= highId )
            {
                try
                {
                    currentNode = getNodeById( currentNodeId++ );
                }
                catch ( NotFoundException e )
                {
                    // ok we try next
                }
            }
            return currentNode != null;
        }

        public synchronized Node next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }

            Node nextNode = currentNode;
            currentNode = null;
            return nextNode;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        this.transactionEventHandlers.add( handler );
        return handler;
    }

    <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return unregisterHandler( this.transactionEventHandlers, handler );
    }

    KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        if ( this.kernelEventHandlers.contains( handler ) )
        {
            return handler;
        }

        // Some algo for putting it in the right place
        for ( KernelEventHandler registeredHandler : this.kernelEventHandlers )
        {
            KernelEventHandler.ExecutionOrder order =
                    handler.orderComparedTo( registeredHandler );
            int index = this.kernelEventHandlers.indexOf( registeredHandler );
            if ( order == KernelEventHandler.ExecutionOrder.BEFORE )
            {
                this.kernelEventHandlers.add( index, handler );
                return handler;
            }
            else if ( order == KernelEventHandler.ExecutionOrder.AFTER )
            {
                this.kernelEventHandlers.add( index + 1, handler );
                return handler;
            }
        }

        this.kernelEventHandlers.add( handler );
        return handler;
    }

    KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        return unregisterHandler( this.kernelEventHandlers, handler );
    }

    private <T> T unregisterHandler( Collection<?> setOfHandlers, T handler )
    {
        if ( !setOfHandlers.remove( handler ) )
        {
            throw new IllegalStateException( handler + " isn't registered" );
        }
        return handler;
    }

    private class SyncHookFactory implements TxEventSyncHookFactory
    {
        public TransactionEventsSyncHook create()
        {
            return transactionEventHandlers.isEmpty() ? null :
                    new TransactionEventsSyncHook(
                            nodeManager, transactionEventHandlers,
                            getConfig().getTxModule().getTxManager() );
        }
    }

    private Pair<Map<String, String>, Boolean> findIndexConfig( Class<? extends PropertyContainer> cls,
            String indexName, Map<String, String> suppliedConfig, Map<?, ?> dbConfig )
    {
        // 1. Check stored config (has this index been created previously?)
        Map<String, String> storedConfig = indexStore.get( cls, indexName );
//        userConfig = userConfig != null ? defaultsFiller.fill( userConfig ) : null;
        Map<String, String> configToUse = null;
        IndexProvider indexProvider = null;
        
        // 2. Check config supplied by the user for this method call
        if ( configToUse == null )
        {
            configToUse = suppliedConfig;
        }
        
        // 3. Check db config properties for provider
        if ( configToUse == null )
        {
            String provider = null;
            if ( dbConfig != null )
            {
                provider = (String) dbConfig.get( "index." + indexName );
                if ( provider == null )
                {
                    provider = (String) dbConfig.get( "index" );
                }
            }
            
            // 4. Default to lucene
            if ( provider == null )
            {
                provider = "lucene";
            }
            indexProvider = getIndexProvider( provider );
            configToUse = indexProvider.fillInDefaults( MapUtil.stringMap( KEY_INDEX_PROVIDER, provider ) );
        }
        else
        {
            indexProvider = getIndexProvider( configToUse.get( KEY_INDEX_PROVIDER ) );
        }
        
        if ( storedConfig != null )
        {
            if ( suppliedConfig != null && !storedConfig.equals( suppliedConfig ) )
            {
                throw new IllegalArgumentException( "Supplied index configuration:\n" +
                        suppliedConfig + "\ndiffer from stored config:\n" + storedConfig +
                        "\nfor '" + indexName + "'" );
            }
            configToUse = storedConfig;
        }
        
        boolean created = indexStore.setIfNecessary( cls, indexName, configToUse );
        return new Pair<Map<String, String>, Boolean>( configToUse, created );
    }
    
    private IndexProvider getIndexProvider( String provider )
    {
        synchronized ( this.indexProviders )
        {
            IndexProvider result = this.indexProviders.get( provider );
            if ( result != null )
            {
                return result;
            }
            throw new IllegalArgumentException( "No index provider '" + provider + "' found" );
//            result = (IndexProvider) Service.load( KernelExtension.class, provider );
//            this.indexProviders.put( provider, result );
//            return result;
        }
    }
    
    private Map<String, String> getOrCreateIndexConfig( Class<? extends PropertyContainer> cls,
            String indexName, Map<String, String> suppliedConfig )
    {
        Pair<Map<String, String>, Boolean> result = findIndexConfig( cls,
                indexName, suppliedConfig, getConfig().getParams() );
        if ( result.other() )
        {
            IndexCreatorThread creator = new IndexCreatorThread( cls, indexName, result.first() );
            creator.start();
            try
            {
                creator.join();
                if ( creator.exception != null )
                {
                    throw new TransactionFailureException( "Index creation failed for " + indexName +
                            ", " + result.first(), creator.exception );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        return result.first();
    }
    
    Index<Node> nodeIndex( String indexName, Map<String, String> configForCreation )
    {
        Map<String, String> config = getOrCreateIndexConfig( Node.class, indexName, configForCreation );
        return getIndexProvider( config.get( KEY_INDEX_PROVIDER ) ).nodeIndex( indexName, config );
    }
    
    RelationshipIndex relationshipIndex( String indexName, Map<String, String> configForCreation )
    {
        Map<String, String> config = getOrCreateIndexConfig( Relationship.class, indexName, configForCreation );
        return getIndexProvider( config.get( KEY_INDEX_PROVIDER ) ).relationshipIndex( indexName, config );
    }
    
    private class IndexCreatorThread extends Thread
    {
        private final String indexName;
        private final Map<String, String> config;
        private Exception exception;
        private final Class<? extends PropertyContainer> cls;

        IndexCreatorThread( Class<? extends PropertyContainer> cls, String indexName,
                Map<String, String> config )
        {
            this.cls = cls;
            this.indexName = indexName;
            this.config = config;
        }
        
        @Override
        public void run()
        {
            String provider = config.get( KEY_INDEX_PROVIDER );
            String dataSourceName = getIndexProvider( provider ).getDataSourceName();
            XaDataSource dataSource = getConfig().getTxModule().getXaDataSourceManager().getXaDataSource( dataSourceName );
            IndexXaConnection connection = (IndexXaConnection) dataSource.getXaConnection();
            Transaction tx = beginTx();
            try
            {
                javax.transaction.Transaction javaxTx = getConfig().getTxModule().getTxManager().getTransaction();
                javaxTx.enlistResource( connection.getXaResource() );
                connection.createIndex( cls, indexName, config );
                tx.success();
            }
            catch ( Exception e )
            {
                this.exception = e;
            }
            finally
            {
                tx.finish();
            }
        }
    }
}
