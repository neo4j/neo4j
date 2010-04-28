/*
 * Copyright (c) 2002-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.ShellService.ShellNotAvailableException;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionEventsSyncHook;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TransactionFailureException;
import org.neo4j.kernel.manage.Neo4jJmx;

class EmbeddedGraphDbImpl
{
    private static final String KERNEL_VERSION = Version.get();

    private static Logger log =
        Logger.getLogger( EmbeddedGraphDbImpl.class.getName() );
    private static final AtomicInteger INSTANCE_ID_COUNTER = new AtomicInteger();
    private ShellService shellService;
    private Transaction placeboTransaction = null;
    private final GraphDbInstance graphDbInstance;
    private final GraphDatabaseService graphDbService;
    private final NodeManager nodeManager;
    private final String storeDir;
    private final int instanceId = INSTANCE_ID_COUNTER.getAndIncrement();

    private final List<KernelEventHandler> kernelEventHandlers =
            new CopyOnWriteArrayList<KernelEventHandler>();
    private final Collection<TransactionEventHandler<?>> transactionEventHandlers =
            new CopyOnWriteArraySet<TransactionEventHandler<?>>();
    private final KernelPanicEventGenerator kernelPanicEventGenerator =
            new KernelPanicEventGenerator( kernelEventHandlers );

    private final Runnable jmxShutdownHook;

    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     *
     * @param storeDir the store directory for the Neo4j db files
     */
    public EmbeddedGraphDbImpl( String storeDir, GraphDatabaseService graphDbService )
    {
        this.storeDir = storeDir;
        graphDbInstance = new GraphDbInstance( storeDir, true );
        Map<Object, Object> params = graphDbInstance.start( graphDbService,
                kernelPanicEventGenerator );
        nodeManager =
            graphDbInstance.getConfig().getGraphDbModule().getNodeManager();
        this.graphDbService = graphDbService;
        jmxShutdownHook = initJMX( params );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     *
     * @param storeDir the store directory for the db files
     * @param config configuration parameters
     */
    public EmbeddedGraphDbImpl( String storeDir, Map<String, String> config,
        GraphDatabaseService graphDbService )
    {
        this.storeDir = storeDir;
        graphDbInstance = new GraphDbInstance( storeDir, true );
        Map<Object, Object> params = graphDbInstance.start( graphDbService,
                config, kernelPanicEventGenerator );
        nodeManager =
            graphDbInstance.getConfig().getGraphDbModule().getNodeManager();
        this.graphDbService = graphDbService;
        jmxShutdownHook = initJMX( params );
    }

    private Runnable initJMX( final Map<Object, Object> params )
    {
        return Neo4jJmx.initJMX( new Neo4jJmx.Creator(
                instanceId, KERNEL_VERSION,
                (NeoStoreXaDataSource) graphDbInstance.getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( "nioneodb" ) )
        {
            @Override
            protected void create( Neo4jJmx.Factory jmx )
            {
                jmx.createDynamicConfigurationMBean( params );
                jmx.createPrimitiveMBean( nodeManager );
                jmx.createCacheMBean( nodeManager );
                // jmx.createLockManagerMBean();
                jmx.createTransactionManagerMBean( getConfig().getTxModule() );
                jmx.createMemoryMappingMBean( getConfig().getTxModule() );
                // jmx.createXaManagerMBean();
            }
        } );
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

    public void shutdown()
    {
        if ( graphDbInstance.started() )
        {
            sendShutdownEvent();
        }

        jmxShutdownHook.run();

        if ( this.shellService != null )
        {
            try
            {
                this.shellService.shutdown();
                this.shellService = null;
            }
            catch ( Throwable t )
            {
                log.warning( "Error shutting down shell server: " + t );
            }
        }
        graphDbInstance.shutdown();
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

    public boolean enableRemoteShell(
        final Map<String,Serializable> initialProperties )
    {
        if ( shellService != null )
        {
            throw new IllegalStateException( "Shell already enabled" );
        }

        Map<String,Serializable> properties = initialProperties;
        if ( properties == null )
        {
            properties = Collections.emptyMap();
        }
        try
        {
            shellService = new ShellService( this.graphDbService, properties );
            return true;
        }
        catch ( RemoteException e )
        {
            throw new IllegalStateException( "Can't start remote Neo4j shell",
                e );
        }
        catch ( ShellNotAvailableException e )
        {
            log.info( "Shell library not available. Neo4j shell not "
                + "started. Please add the Neo4j shell jar to the classpath." );
            e.printStackTrace();
            return false;
        }
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
                placeboTransaction =
                    new PlaceboTransaction( graphDbInstance
                        .getTransactionManager() );
            }
            return placeboTransaction;
        }
        TransactionManager txManager = graphDbInstance.getTransactionManager();
        Transaction result = null;
        try
        {
            txManager.begin();
            result = new TransactionImpl( txManager );
            registerTransactionEventHookIfNeeded( txManager, result );
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Unable to begin transaction", e );
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void registerTransactionEventHookIfNeeded(
            TransactionManager txManager, Transaction transaction )
            throws SystemException, RollbackException
    {
        if ( !this.transactionEventHandlers.isEmpty() )
        {
            txManager.getTransaction().registerSynchronization(
                    new TransactionEventsSyncHook( this.nodeManager, transaction,
                            this.transactionEventHandlers ) );
        }
    }

    private static class PlaceboTransaction implements Transaction
    {
        private final TransactionManager transactionManager;

        PlaceboTransaction( TransactionManager transactionManager )
        {
            // we should override all so null is ok
            this.transactionManager = transactionManager;
        }

        public void failure()
        {
            try
            {
                transactionManager.getTransaction().setRollbackOnly();
            }
            catch ( Exception e )
            {
                throw new TransactionFailureException(
                    "Failed to mark transaction as rollback only.", e );
            }
        }

        public void success()
        {
        }

        public void finish()
        {
        }
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

    private static class TransactionImpl implements Transaction
    {
        private boolean success = false;

        private final TransactionManager transactionManager;

        TransactionImpl( TransactionManager transactionManager )
        {
            this.transactionManager = transactionManager;
        }

        public void failure()
        {
            this.success = false;
            try
            {
                transactionManager.getTransaction().setRollbackOnly();
            }
            catch ( Exception e )
            {
                throw new TransactionFailureException(
                    "Failed to mark transaction as rollback only.", e );
            }
        }

        public void success()
        {
            success = true;
        }

        public void finish()
        {
            try
            {
                if ( success )
                {
                    if ( transactionManager.getTransaction() != null )
                    {
                        transactionManager.getTransaction().commit();
                    }
                }
                else
                {
                    if ( transactionManager.getTransaction() != null )
                    {
                        transactionManager.getTransaction().rollback();
                    }
                }
            }
            catch ( RollbackException e )
            {
                throw new TransactionFailureException( "Unable to commit transaction", e );
            }
            catch ( Exception e )
            {
                if ( success )
                {
                    throw new TransactionFailureException(
                        "Unable to commit transaction", e );
                }
                else
                {
                    throw new TransactionFailureException(
                        "Unable to rollback transaction", e );
                }
            }
        }
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
}