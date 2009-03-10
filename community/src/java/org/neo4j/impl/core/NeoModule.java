/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

public class NeoModule
{
    private static Logger log = Logger.getLogger( NeoModule.class.getName() );

    private boolean startIsOk = true;

    private static final int INDEX_COUNT = 2500;

    private final TransactionManager transactionManager;
    private final NodeManager nodeManager;
    private final PersistenceManager persistenceManager;

    public NeoModule( AdaptiveCacheManager cacheManager,
        LockManager lockManager, TransactionManager transactionManager,
        PersistenceManager persistenceManager, IdGenerator idGenerator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        nodeManager = new NodeManager( cacheManager, lockManager,
            transactionManager, persistenceManager, idGenerator );
    }

    public void init()
    {
    }

    public void start( Map<Object,Object> params )
    {
        if ( !startIsOk )
        {
            return;
        }
        // load and verify from PS
        RelationshipTypeData relTypes[] = null;
        PropertyIndexData propertyIndexes[] = null;
        try
        {
            transactionManager.begin();
            relTypes = persistenceManager.loadAllRelationshipTypes();
            propertyIndexes = persistenceManager.loadPropertyIndexes( 
                INDEX_COUNT );
            transactionManager.commit();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            try
            {
                transactionManager.rollback();
            }
            catch ( Exception ee )
            {
                ee.printStackTrace();
                log.severe( "Unable to rollback tx" );
            }
            throw new RuntimeException( "Unable to load all relationships", e );
        }
        nodeManager.addRawRelationshipTypes( relTypes );
        nodeManager.addPropertyIndexes( propertyIndexes );
        if ( propertyIndexes.length < INDEX_COUNT )
        {
            nodeManager.setHasAllpropertyIndexes( true );
        }
        nodeManager.start( params );
        startIsOk = false;
    }

    public int getNodeCacheSize()
    {
        return nodeManager.getNodeMaxCacheSize();
    }

    public int getRelationshipCacheSize()
    {
        return nodeManager.getRelationshipMaxCacheSize();
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
        try
        {
            Node node = nodeManager.createNode();
            nodeManager.setReferenceNodeId( (int) node.getId() );
            log.fine( "Created a new reference node. " + 
                "Current reference node is now " + node );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            log.severe( "Unable to create new reference node." );
        }
    }

    public void reload( Map<Object,Object> params )
    {
        stop();
        start( params );
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

    public LockReleaser getLockReleaser()
    {
        return nodeManager.getLockReleaser();
    }
}