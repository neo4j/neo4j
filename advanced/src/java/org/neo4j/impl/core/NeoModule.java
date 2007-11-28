/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.logging.Logger;
import javax.transaction.TransactionManager;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.LockManager;

/**
 * The Neo module handles valid relationship types and manages the 
 * reference node. Also the Node and Relationship caches sizes can be
 * configured here. 
 * <p>
 * The reference node is a reference point in the node space. See it as a 
 * starting point that can be used to traverse to other nodes in the node 
 * space. Using different reference nodes one can manage many applications 
 * in the same node space.
 */
public class NeoModule
{
	private static Logger log = Logger.getLogger( NeoModule.class.getName() );

	private boolean startIsOk = true;
	
	private static final int INDEX_COUNT = 2500;
	
	private final TransactionManager transactionManager;
	private final NodeManager nodeManager;
	private final NeoConstraintsListener neoConstraintsListener;
	private final PersistenceManager persistenceManager;
	
	public NeoModule( AdaptiveCacheManager cacheManager, LockManager lockManager, 
		TransactionManager transactionManager, LockReleaser lockReleaser, 
		EventManager eventManager, PersistenceManager persistenceManager, 
		IdGenerator idGenerator )
	{
		this.transactionManager = transactionManager;
		this.persistenceManager = persistenceManager;
		this.neoConstraintsListener = new NeoConstraintsListener( 
			transactionManager, eventManager );
		nodeManager = new NodeManager( cacheManager, lockManager, 
			transactionManager, lockReleaser, eventManager, 
			persistenceManager, idGenerator );
	}
	
	public void init()
	{
	}
	
	public void start()
	{
		if ( !startIsOk )
		{
			return;
		}
		// load and verify from PS
		RawRelationshipTypeData relTypes[] = null;
		RawPropertyIndex propertyIndexes[] = null;
		try
		{
			neoConstraintsListener.registerEventListeners();
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
			throw new RuntimeException( "Unable to load all relationships", 
				e );
		}
		// RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		nodeManager.addRawRelationshipTypes( relTypes );
		nodeManager.addPropertyIndexes( propertyIndexes );
		if ( propertyIndexes.length < INDEX_COUNT )
		{
			nodeManager.setHasAllpropertyIndexes( true );
		}
		nodeManager.start();
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
			log.info( "Created a new reference node. " + 
				"Current reference node is now " + node );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			log.severe( "Unable to create new reference node." );
		}
	}

	public void reload()
	{
		stop();
		start();
	}
	
	public void stop()
	{
		nodeManager.clearPropertyIndexes();
		neoConstraintsListener.unregisterEventListeners();
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
	
	public RelationshipType getRelationshipTypeByName( String name )
    {
		return nodeManager.getRelationshipTypeByName( name );
    }

	public void addEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
    {
		nodeManager.addValidRelationshipTypes( relationshipTypes );
    }

	public Iterable<RelationshipType> getRelationshipTypes()
    {
		return nodeManager.getRelationshipTypes();
    }

	public boolean hasRelationshipType( String name )
    {
		return nodeManager.hasRelationshipType( name );
    }

	public RelationshipType registerRelationshipType( String name, 
		boolean create )
    {
		return nodeManager.addValidRelationshipType( name, create );
    }
}
