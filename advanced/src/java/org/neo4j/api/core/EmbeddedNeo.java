/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.api.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;
import javax.transaction.TransactionManager;
import org.neo4j.api.core.NeoJvmInstance.Config;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoShellServer;
import org.neo4j.util.shell.AbstractServer;

/**
 * An implementation of {@link NeoService} that is used to embed Neo in an
 * application. You typically instantiate it by invoking the
 * {@link #EmbeddedNeo(String) single argument constructor} that takes a path to
 * a directory where Neo will store its data files, as such: <code>
 * <pre>
 * NeoService neo = new EmbeddedNeo( &quot;var/neo&quot; );
 * // ... use neo
 * neo.shutdown();
 * </pre>
 * </code> For more information, see {@link NeoService}.
 */
public final class EmbeddedNeo implements NeoService
{
	public static final int DEFAULT_SHELL_PORT = AbstractServer.DEFAULT_PORT;
	public static final String DEFAULT_SHELL_NAME = AbstractServer.DEFAULT_NAME;
	
    private static Logger log = Logger.getLogger( EmbeddedNeo.class.getName() );
    private NeoShellServer shellServer;
    private Transaction placeboTransaction = null;
    private final NeoJvmInstance neoJvmInstance;
    private final NodeManager nodeManager;
    private final String storeDir;

    /**
     * Creates an embedded {@link NeoService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already
     * exist.
     * @param storeDir the store directory for the neo db files
     */
    public EmbeddedNeo( String storeDir )
    {
        this.storeDir = storeDir;
        this.shellServer = null;
        neoJvmInstance = new NeoJvmInstance( storeDir, true );
        neoJvmInstance.start();
        nodeManager = neoJvmInstance.getConfig().getNeoModule()
            .getNodeManager();
    }

    /**
     * A non-standard way of creating an embedded {@link NeoService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    public EmbeddedNeo( String storeDir, Map<String,String> params )
    {
        this.storeDir = storeDir;
        this.shellServer = null;
        neoJvmInstance = new NeoJvmInstance( storeDir, true );
        neoJvmInstance.start( params );
        nodeManager = neoJvmInstance.getConfig().getNeoModule()
            .getNodeManager();
    }

    /**
     * A non-standard Convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most 
     * likely be removed in future releases.
     * @param file the property file to load
     * @return a map containing the properties from the file
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
            throw new RuntimeException( "Unable to load properties file["
                + file + "]", e );
        }
        Set<Entry<Object,Object>> entries = props.entrySet();
        Map<String,String> stringProps = new HashMap<String,String>();
        for ( Entry entry : entries )
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            stringProps.put( key, value );
        }
        return stringProps;
    }

    // private accessor for the remote shell (started with enableRemoteShell())
    private NeoShellServer getShellServer()
    {
        return this.shellServer;
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#createNode()
     */
    public Node createNode()
    {
        return nodeManager.createNode();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getNodeById(long)
     */
    public Node getNodeById( long id )
    {
        return nodeManager.getNodeById( (int) id );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipById(long)
     */
    public Relationship getRelationshipById( long id )
    {
        return nodeManager.getRelationshipById( (int) id );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getReferenceNode()
     */
    public Node getReferenceNode()
    {
        return nodeManager.getReferenceNode();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#shutdown()
     */
    public void shutdown()
    {
        if ( getShellServer() != null )
        {
            try
            {
                getShellServer().shutdown();
            }
            catch ( Throwable t )
            {
                log.warning( "Error shutting down shell server: " + t );
            }
        }
        neoJvmInstance.shutdown();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell()
     */
    public boolean enableRemoteShell()
    {
        return this.enableRemoteShell( null );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell(java.util.Map)
     */
    public boolean enableRemoteShell(
        final Map<String,Serializable> initialProperties )
    {
        Map<String,Serializable> properties = initialProperties;
        if ( properties == null )
        {
            properties = Collections.emptyMap();
        }
        try
        {
            if ( shellDependencyAvailable() )
            {
                this.shellServer = new NeoShellServer( this );
                Object port = properties.get( "port" );
                Object name = properties.get( "name" );
                this.shellServer.makeRemotelyAvailable(
                    port != null ? (Integer) port : DEFAULT_SHELL_PORT,
                    name != null ? (String) name : DEFAULT_SHELL_NAME );
                return true;
            }
            else
            {
                log.info( "Shell library not available. Neo shell not "
                    + "started. Please add the Neo4j shell jar to the "
                    + "classpath." );
                return false;
            }
        }
        catch ( RemoteException e )
        {
            throw new IllegalStateException( "Can't start remote neo shell: "
                + e );
        }
    }

    private boolean shellDependencyAvailable()
    {
        try
        {
            Class.forName( "org.neo4j.util.shell.ShellServer" );
            return true;
        }
        catch ( Throwable t )
        {
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipTypes()
     */
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return neoJvmInstance.getRelationshipTypes();
    }

    public Transaction beginTx()
    {
        if ( neoJvmInstance.transactionRunning() )
        {
            if ( placeboTransaction == null )
            {
                placeboTransaction = new PlaceboTransaction( neoJvmInstance
                    .getTransactionManager() );
            }
            return placeboTransaction;
        }
        TransactionManager txManager = neoJvmInstance.getTransactionManager();
        try
        {
            txManager.begin();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        return new TransactionImpl( txManager );
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
                throw new RuntimeException( e );
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
        return neoJvmInstance.getConfig();
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
                throw new RuntimeException( e );
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
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
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
        return new Iterable<Node>() {
            public Iterator<Node> iterator() {
                long highId = (nodeManager.getHighestPossibleIdInUse(Node.class)
                        & 0xFFFFFFFFL);
                return new AllNodesIterator(highId);
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
}