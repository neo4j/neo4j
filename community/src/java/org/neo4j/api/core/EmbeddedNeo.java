/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version. This program is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
 * General Public License for more details. You should have received a copy of
 * the GNU Affero General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.neo4j.api.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;
import javax.transaction.TransactionManager;
import org.neo4j.api.core.NeoJvmInstance.Config;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoShellServer;

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
    private static Logger log = Logger.getLogger( EmbeddedNeo.class.getName() );
    private NeoShellServer shellServer;
    private Transaction placeboTransaction = null;
    private final NeoJvmInstance neoJvmInstance;
    private final NodeManager nodeManager;

    /**
     * Creates an embedded {@link NeoService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already
     * exist.
     * @param storeDir
     *            the store directory for the neo db files
     */
    public EmbeddedNeo( String storeDir )
    {
        this.shellServer = null;
        neoJvmInstance = new NeoJvmInstance( storeDir, true );
        neoJvmInstance.start();
        nodeManager = neoJvmInstance.getConfig().getNeoModule()
            .getNodeManager();
        Transaction.neo = this;
    }

    public EmbeddedNeo( String storeDir, Map<String,String> params )
    {
        this.shellServer = null;
        neoJvmInstance = new NeoJvmInstance( storeDir, true );
        neoJvmInstance.start( params );
        nodeManager = neoJvmInstance.getConfig().getNeoModule()
            .getNodeManager();
        Transaction.neo = this;
    }
    
    public static Map<String,String> loadConfigurations( String file ) 
    {
        Properties props = new Properties();
        try
        {
            props.load( new FileInputStream( new File( file ) ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to load properties file[" +
                file + "]", e );
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
    public boolean enableRemoteShell( Map<String,Serializable> initialProperties )
    {
        try
        {
            if ( initialProperties == null )
            {
                initialProperties = Collections.emptyMap();
            }
            if ( shellDependencyAvailable() )
            {
                this.shellServer = new NeoShellServer( this );
                Object port = initialProperties.get( "port" );
                Object name = initialProperties.get( "name" );
                this.shellServer.makeRemotelyAvailable(
                    port != null ? (Integer) port : 1337,
                    name != null ? (String) name : "shell" );
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

    /**
     * Returns all relationship types in the underlying store. Relationship
     * types are added to the underlying store the first time they are used in
     * {@link Node#createRelationshipTo}.
     * @return all relationship types in the underlying store
     * @deprecated Might not be needed now that relationship types are {@link
     *             RelationshipType created dynamically}.
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
        return new Transaction( txManager );
    }

    private static class PlaceboTransaction extends Transaction
    {
        private final TransactionManager transactionManager;

        PlaceboTransaction( TransactionManager transactionManager )
        {
            // we should override all so null is ok
            super( null );
            this.transactionManager = transactionManager;
        }

        @Override
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

        @Override
        public void success()
        {
        }

        @Override
        public void finish()
        {
        }
    }

    public Config getConfig()
    {
        return neoJvmInstance.getConfig();
    }
}
