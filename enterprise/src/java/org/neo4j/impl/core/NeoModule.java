package org.neo4j.impl.core;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.TransactionFactory;

import java.util.logging.Logger;

/**
 * The neo module handles valid relationship types and manages the 
 * reference node. Alose the Node and Relationship caches sizes can be
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
	private Class<? extends RelationshipType> relTypeClass;
	
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
		try
		{
			NeoConstraintsListener.getListener().registerEventListeners();
			TransactionFactory.getUserTransaction().begin();
			relTypes = 
				PersistenceManager.getManager().loadAllRelationshipTypes();
			TransactionFactory.getUserTransaction().commit();
		}
		catch ( Exception e )
		{
			try
			{
				TransactionFactory.getUserTransaction().rollback();
			}
			catch ( Exception ee )
			{
				ee.printStackTrace();
				log.severe( "Unable to rollback tx" );
			}
			throw new RuntimeException( "Unable to load all relationships", 
				e );
		}
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		rth.addRawRelationshipTypes( relTypes );
		if ( relTypeClass != null )
		{
			rth.addValidRelationshipTypes( relTypeClass );
		}
		AdaptiveCacheManager.getManager().start();
		startIsOk = false;
	}
	
	public void setRelationshipTypes( Class<? extends RelationshipType> clazz )
	{
		this.relTypeClass = clazz;
	}

	public int getNodeCacheSize()
	{
		return NodeManager.getManager().getNodeMaxCacheSize();
	}
	
	public int getRelationshipCacheSize()
	{
		return NodeManager.getManager().getRelationshipMaxCacheSize();
	}
	
	public void setReferenceNodeId( Integer nodeId )
	{
		NodeManager.getManager().setReferenceNodeId( nodeId.intValue() );
		try
		{
			NodeManager.getManager().getReferenceNode();
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
			return (int) NodeManager.getManager().getReferenceNode().getId();
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
			Node node = NodeManager.getManager().createNode();
			NodeManager.getManager().setReferenceNodeId( (int) node.getId() );
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
		NeoConstraintsListener.getListener().unregisterEventListeners();
		AdaptiveCacheManager.getManager().stop();
	}
	
	public void destroy()
	{
	}

	public RelationshipType getRelationshipTypeByName( String name )
    {
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		return rth.getRelationshipTypeByName( name );
    }

	public void addEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
    {
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		rth.addValidRelationshipTypes( relationshipTypes );
    }
}
