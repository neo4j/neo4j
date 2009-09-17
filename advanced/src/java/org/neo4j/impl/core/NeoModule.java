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
package org.neo4j.impl.core;

import java.util.Map;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.TransactionFailureException;

public class NeoModule
{
    private static Logger log = Logger.getLogger( NeoModule.class.getName() );

    private boolean startIsOk = true;

    private static final int INDEX_COUNT = 2500;

    private final TransactionManager transactionManager;
    private final AdaptiveCacheManager cacheManager;
    private final LockManager lockManager;
    private final IdGenerator idGenerator;
    
    private NodeManager nodeManager;
    
    private boolean readOnly = false;

    public NeoModule( AdaptiveCacheManager cacheManager,
        LockManager lockManager, TransactionManager transactionManager,
        IdGenerator idGenerator )
    {
        this.cacheManager = cacheManager;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.idGenerator = idGenerator;
    }

    public NeoModule( AdaptiveCacheManager cacheManager,
        LockManager lockManager, TransactionManager transactionManager,
        IdGenerator idGenerator, boolean readOnly )
    {
        this.cacheManager = cacheManager;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.idGenerator = idGenerator;
        this.readOnly = readOnly;
    }
    
    public void init()
    {
    }

    public void start( LockReleaser lockReleaser, 
        PersistenceManager persistenceManager, Map<Object,Object> params )
    {
        if ( !startIsOk )
        {
            return;
        }
        boolean useNewCache = true;
        if ( params.containsKey( "use_old_cache" ) && 
            params.get( "use_old_cache" ).equals( "true" ) )
        {
            useNewCache = false;
        }
        if ( !readOnly )
        {
            nodeManager = new NodeManager( cacheManager, lockManager, lockReleaser, 
                transactionManager, persistenceManager, idGenerator, useNewCache );
        }
        else
        {
            nodeManager = new ReadOnlyNodeManager( cacheManager, lockManager, 
                lockReleaser, transactionManager, persistenceManager, 
                idGenerator, useNewCache );
        }
        // load and verify from PS
        RelationshipTypeData relTypes[] = null;
        PropertyIndexData propertyIndexes[] = null;
        beginTx();
        relTypes = persistenceManager.loadAllRelationshipTypes();
        propertyIndexes = persistenceManager.loadPropertyIndexes( 
            INDEX_COUNT );
        commitTx();
        nodeManager.addRawRelationshipTypes( relTypes );
        nodeManager.addPropertyIndexes( propertyIndexes );
        if ( propertyIndexes.length < INDEX_COUNT )
        {
            nodeManager.setHasAllpropertyIndexes( true );
        }
        nodeManager.start( params );
        startIsOk = false;
    }
    
    private void beginTx()
    {
        try
        {
            transactionManager.begin();
        }
        catch ( NotSupportedException e )
        {
            throw new TransactionFailureException( 
                "Unable to begin transaction.", e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( 
                "Unable to begin transaction.", e );
        }
    }
    
    private void commitTx()
    {
        try
        {
            transactionManager.commit();
        }
        catch ( SecurityException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( IllegalStateException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( RollbackException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( HeuristicMixedException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( HeuristicRollbackException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
    }
    
    public void setReferenceNodeId( Integer nodeId )
    {
        nodeManager.setReferenceNodeId( nodeId.intValue() );
        try
        {
            nodeManager.getReferenceNode();
        }
        catch ( NotFoundException e )
        {
            log.warning( "Reference node[" + nodeId + "] not valid." );
        }
    }

    public Integer getCurrentReferenceNodeId()
    {
        try
        {
            return (int) nodeManager.getReferenceNode().getId();
        }
        catch ( NotFoundException e )
        {
            return -1;
        }
    }

    public void createNewReferenceNode()
    {
        Node node = nodeManager.createNode();
        nodeManager.setReferenceNodeId( (int) node.getId() );
    }

    public void reload( Map<Object,Object> params )
    {
        throw new UnsupportedOperationException();
    }

    public void stop()
    {
        nodeManager.clearPropertyIndexes();
        nodeManager.clearCache();
        nodeManager.stop();
    }

    public void destroy()
    {
    }

    public NodeManager getNodeManager()
    {
        return this.nodeManager;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return nodeManager.getRelationshipTypes();
    }
}