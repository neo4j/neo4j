package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.cache.AdaptiveCacheManager;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.command.CommandManager;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.command.TransactionCache;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceException;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.IllegalResourceException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockNotFoundException;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionIsolationLevel;


/**
 * Use this class to create and find {@link Node nodes} and to create 
 * {@link Relationship relationships}. 
 * <p>
 * A loaded node (from persistence storage when not found in cache) will start 
 * in first phase only knowing about node id.
 * A relationship may become shallow if one of the nodes aren't in chache. See 
 * {@link NodeImpl} and {@link RelationshipImpl} for more information about 
 * different phases.
 * <p>
 * All public methods must be invoked within a transaction context, failure to 
 * do so will result in an exception. <CODE>createNode</CODE> and 
 * <CODE>createRelationship</CODE> methods will create a command that can 
 * execute and undo the desired operation. The command will be associated with 
 * the transaction and if the transaction fails all the commands participating 
 * in the transaction will be undone.
 * <p>
 * Methods that uses commands will first create a command and verify that
 * we're in a transaction context. To persist operations a pro-active event 
 * is generated and will cause the 
 * {@link org.neo4j.impl.persistence.BusinessLayerMonitor} to persist the 
 * then operation. 
 * If the event fails (false is returned)  the transaction is marked as 
 * rollback only and if the command will be undone.
 */
public class NodeManager
{
	private static Logger log = Logger.getLogger( NodeManager.class.getName() );
	
	private static NodeManager instance = new NodeManager();
	
	private int referenceNodeId = 0;
	
	private LruCache<Integer,Node> nodeCache = 
		new LruCache<Integer,Node>( "NodeCache", 1500 );
	private LruCache<Integer,Relationship> relCache = 
		new LruCache<Integer,Relationship>( "RelationshipCache", 3500 );
	
	private NodeManager()
	{
		AdaptiveCacheManager.getManager().registerCache(
			nodeCache, 0.77f, 0 );
		AdaptiveCacheManager.getManager().registerCache(
			relCache, 0.77f, 0 );
	}
	
	public static NodeManager getManager()
	{
		return instance;
	}
	
	private TransactionCache getTransactionCache()
	{
		return TransactionCache.getCache();
	}
	
	/**
	 * Creates a node, see class javadoc
	 *
	 * @return the created node
	 * @throws CreateException if not in transaction or unable to create node
	 */
	public Node createNode() // throws CreateException
	{
		int id = IdGenerator.getGenerator().nextId( Node.class );
		NodeImpl node = new NodeImpl( id, true );
		NodeCommands nodeCommand = null;
		acquireLock( node, LockType.WRITE );
		try
		{
			nodeCommand = new NodeCommands();
			nodeCommand.setNode( node );
			nodeCommand.initCreate();

			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( nodeCommand );
			if ( !em.generateProActiveEvent( Event.NODE_CREATE, eventData ) )
			{
				setRollbackOnly();
				throw new CreateException( "Unable to create node, " +
					"pro-active event failed." );
			}

			nodeCommand.execute();
			em.generateReActiveEvent( Event.NODE_CREATE, eventData );
			return new NodeProxy( id );
		}
		catch ( ExecuteFailedException e )
		{
			setRollbackOnly();
			if ( nodeCommand != null )
			{
				nodeCommand.undo();
			}
			throw new CreateException( "Failed executing command", e );
		}
		finally
		{
			releaseLock( node, LockType.WRITE );
		}
	}
	
	/**
	 * Create a realtionship, see class javadoc.
	 *
	 * @param startNode the start node if directed or just one of the nodes
	 * two nodes if not directed
	 * @param endNode the end node if directed or just the other node if not
	 * directed
	 * @param type the relationship type
	 * @param directed whether the relationship should be directed
	 * @return the created relationship
	 * @throws IllegalArgumentException If null param or invalid relationship
	 * type
	 * @throws CreateException if not in transaction or unable to create 
	 * relationship
	 */
	public Relationship createRelationship( Node startNode, Node endNode, 
		RelationshipType type )
	{
		if ( startNode == null || endNode == null || type == null )
		{
			throw new IllegalArgumentException( "Null parameter, startNode=" + 
				startNode + ", endNode=" + endNode + ", type=" + type );
		}
		if ( !RelationshipTypeHolder.getHolder().isValidRelationshipType( 
			type ) )
		{
			setRollbackOnly();
			throw new IllegalArgumentException( "Relationship type: " + type 
				+ " not valid" );
		}
		
		NodeImpl firstNode = ( NodeImpl ) getNodeForProxy( 
			(int) startNode.getId() );
		NodeImpl secondNode = ( NodeImpl ) getNodeForProxy( 
			(int) endNode.getId() );
		
		int id = IdGenerator.getGenerator().nextId( Relationship.class );
		RelationshipImpl rel = new RelationshipImpl( id, 
			(int) startNode.getId(), (int) endNode.getId(), type, true );
		RelationshipCommands relationshipCommand = null;
		boolean firstNodeTaken = false;
		boolean secondNodeTaken = false;
		acquireLock( rel, LockType.WRITE );
		try
		{
			acquireLock( firstNode, LockType.WRITE );
			firstNodeTaken = true;
			if ( firstNode.isDeleted() )
			{
				setRollbackOnly();
				throw new CreateException( "" + startNode + 
					" has been deleted in other transaction" );
			}
			acquireLock( secondNode, LockType.WRITE );
			secondNodeTaken = true;
			if ( secondNode.isDeleted() )
			{
				setRollbackOnly();
				throw new CreateException( "" + endNode + 
					" has been deleted in other transaction" );
			}
			relationshipCommand = new RelationshipCommands();
			relationshipCommand.setRelationship( rel );
			relationshipCommand.initCreate();

			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( relationshipCommand );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIP_CREATE, 
				eventData ) )
			{
				setRollbackOnly();
				throw new CreateException( "Unable to create relationship, " +
					"pro-active event failed." );
			}

			relationshipCommand.execute();
			em.generateReActiveEvent( Event.RELATIONSHIP_CREATE, eventData );
			return new RelationshipProxy( id );
		}
		catch ( ExecuteFailedException e )
		{
			setRollbackOnly();
			if ( relationshipCommand != null )
			{
				relationshipCommand.undo();
			}
			throw new CreateException( "Failed executing command", e );
		}
		finally
		{
			boolean releaseFailed = false;
			if ( firstNodeTaken )
			{
				try
				{
					releaseLock( firstNode, LockType.WRITE );
				}
				catch ( Exception e )
				{
					releaseFailed = true;
					e.printStackTrace();
					log.severe( "Failed to release lock" );
				}
			}
			if ( secondNodeTaken )
			{
				try
				{
					releaseLock( secondNode, LockType.WRITE );
				}
				catch ( Exception e )
				{
					releaseFailed = true;
					e.printStackTrace();
					log.severe( "Failed to release lock" );
				}
			}
			releaseLock( rel, LockType.WRITE );
			if ( releaseFailed )
			{
				throw new RuntimeException( "Unable to release locks [" + 
					startNode + "," + endNode + "] in relationship create->" + 
					rel );
			}
		}
	}
	
	/**
	 * Gets a node by id.
	 * <p>
	 * First checks the cache, if the node is in cache it is returned.
	 * If the node isn't in cache it is loaded from persistent storage.
	 * 
	 * @param id the node id
	 * @return the node with node id <CODE>id</CODE>
	 * @throws NotFoundException
	 */
	public Node getNodeById( int nodeId ) throws NotFoundException
	{
		Node node = getTransactionCache().getNode( nodeId ); 
		if ( node != null )
		{
			if ( ( ( NodeImpl ) node ).isDeleted() )
			{
				throw new NotFoundException( 
					"Node[" + nodeId + "] has been deleted (in this tx)" );
			}
			return new NodeProxy( nodeId );
		}
		node = nodeCache.get( nodeId );
		if ( node != null )
		{
			return new NodeProxy( nodeId );
		}
		node = new NodeImpl( nodeId );
		acquireLock( node, LockType.READ );
		try
		{
			if ( nodeCache.get( nodeId ) != null )
			{
				node = nodeCache.get( nodeId );
				return new NodeProxy( nodeId );
			}
			if ( PersistenceManager.getManager().loadLightNode( nodeId ) == 
				null )
			{
				throw new NotFoundException( "Node[" + nodeId + "]" );
			}
			
			// If we get here, loadLightNode didn't throw exception and
			// a node with the given id does exist... good.
			nodeCache.add( nodeId, node );
			return new NodeProxy( nodeId );
		}
		catch ( PersistenceException pe )
		{
			log.severe( "Persistence error while trying to get node #" +
					   nodeId + " by id. " + pe );
			throw new NotFoundException( pe );
		}
		finally
		{
			forceReleaseReadLock( node );
		}
	}
	
	Node getNodeForProxy( int nodeId )
	{
		Node node = getTransactionCache().getNode( nodeId ); 
		if ( node != null )
		{
			return node;
		}
		node = nodeCache.get( nodeId );
		if ( node != null )
		{
			return node;
		}
		node = new NodeImpl( nodeId );
		acquireLock( node, LockType.READ );
		try
		{
			if ( nodeCache.get( nodeId ) != null )
			{
				node = nodeCache.get( nodeId );
				return node;
			}
			if ( PersistenceManager.getManager().loadLightNode( nodeId ) == 
				null )
			{
				throw new RuntimeException( "Node[" + nodeId + "] deleted?" );
			}
			nodeCache.add( nodeId, node );
			return node;
		}
		catch ( PersistenceException pe )
		{
			log.severe( "Persistence error while trying to get node #" +
					   nodeId + " by id. " + pe );
			throw new RuntimeException( "Node deleted?", pe );
		}
		finally
		{
			forceReleaseReadLock( node );
		}
	}
	
	/**
	 * Returns the reference node in this node space. If the node isn't found 
	 * (reference node hasn't been configured properly) a 
	 * {@link NotFoundException} is thrown. See {@link NeoModule} for more 
	 * information
	 *
	 * @return the reference node
	 * @throws NotFoundException
	 */
	public Node getReferenceNode() throws NotFoundException
	{
		if ( referenceNodeId == -1 )
		{
			throw new NotFoundException( "No reference node set" );
		}
		return getNodeById( referenceNodeId );
	}
	
	void setReferenceNodeId( int nodeId )
	{
		this.referenceNodeId = nodeId;
	}
	
	// checks the cache first, if not in cache load it using PM
	Relationship getRelationshipById( int relId ) 
		throws NotFoundException
	{
		Relationship relationship = getTransactionCache().getRelationship( 
			relId ); 
		if ( relationship != null )
		{
			return new RelationshipProxy( relId );
		}
		relationship = relCache.get( relId );
		if ( relationship != null )
		{
			return new RelationshipProxy( relId );
		}
		Relationship dummyRel = new RelationshipImpl( relId );
		acquireLock( dummyRel, LockType.READ );
		try
		{
			if ( relCache.get( relId ) != null )
			{
				relationship = relCache.get( relId );
				return new RelationshipProxy( relId );
			}

			RawRelationshipData data = 
				PersistenceManager.getManager().loadLightRelationship( relId );
			if ( data == null )
			{
				throw new NotFoundException( "Relationship[" + relId + 
					"] not found" );
			}
			RelationshipType type = getRelationshipTypeById( data.getType() );
			if ( type == null )
			{
				throw new NotFoundException( "Relationship[" + data.getId() +
					"] exist but relationship type[" + data.getType() +
					"] not registered." );
			}
			relationship = new RelationshipImpl( relId, data.getFirstNode(), 
				data.getSecondNode(), type, false );
			relCache.add( relId, relationship );
			return new RelationshipProxy( relId );
		}
		catch ( PersistenceException e )
		{
			throw new NotFoundException( "Could not get relationship[" + 
				relId + "].", e );
		}
		finally
		{
			// forceReleaseWriteLock( dummyRel );
			forceReleaseReadLock( dummyRel );
		}
	}
	
	RelationshipType getRelationshipTypeById( int id )
	{
		return RelationshipTypeHolder.getHolder().getRelationshipType( id );
	}
	
	Relationship getRelForProxy( int relId )
	{
		Relationship relationship = getTransactionCache().getRelationship( 
			relId ); 
		if ( relationship != null )
		{
			return relationship;
		}
		relationship = relCache.get( relId );
		if ( relationship != null )
		{
			return relationship;
		}
		Relationship dummyRel = new RelationshipImpl( relId );
		acquireLock( dummyRel, LockType.READ );
		try
		{
			if ( relCache.get( relId ) != null )
			{
				relationship = relCache.get( relId );
				return relationship;
			}
			RawRelationshipData data = 
				PersistenceManager.getManager().loadLightRelationship( relId );
			if ( data == null )
			{
				throw new RuntimeException( "Relationship[" + relId + 
					"] deleted?" );
			}
			RelationshipType type = getRelationshipTypeById( data.getType() );
			if ( type == null )
			{
				throw new NotFoundException( "Relationship[" + data.getId() +
					"] exist but relationship type[" + data.getType() +
					"] not registered." );
			}
			relationship = new RelationshipImpl( relId, data.getFirstNode(), 
				data.getSecondNode(), type, false );
			relCache.add( relId, relationship );
			return relationship;
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( "Relationship deleted?", e );
		}
		catch ( PersistenceException e )
		{
			throw new RuntimeException( "Relationship deleted?", e );
		}
		finally
		{
			forceReleaseReadLock( dummyRel );
		}
	}
	
	void doDeleteNode( NodeImpl node ) // throws DeleteException
	{
		nodeCache.remove( (int) node.getId() );
	}
	
	void doCreateNode( NodeImpl node )//  throws CreateException
	{
		nodeCache.add( (int) node.getId(), node );
	}

	// NOTE: caller responsible for acquiring lock on nodes
	void doDeleteRelationship( RelationshipImpl relationship ) 
	{
		Integer nodeIds[] = relationship.getNodeIds();
		if ( getTransactionCache().getNode( nodeIds[0] ) != null || 
			nodeCache.get( nodeIds[0] ) != null )
		{
			( ( NodeImpl ) getNodeForProxy( nodeIds[0].intValue() ) ).
				removeRelationship( relationship.getType(),
					(int) relationship.getId() );
		}
		if ( getTransactionCache().getNode( nodeIds[1] ) != null || 
			nodeCache.get( nodeIds[1] ) != null )
		{
			( ( NodeImpl ) getNodeForProxy( nodeIds[1].intValue() ) ).
				removeRelationship( relationship.getType(), 
					(int) relationship.getId() );
		}
		relCache.remove( (int) relationship.getId() );
	}

	// NOTE: caller responsible for acquiring lock on nodes
	void doCreateRelationship( RelationshipImpl relationship ) 
	{
		Integer nodeIds[] = relationship.getNodeIds();
		( ( NodeImpl ) getNodeForProxy( nodeIds[0].intValue() ) ).
			addRelationship( relationship.getType(), 
				(int) relationship.getId() );
		( ( NodeImpl ) getNodeForProxy( nodeIds[1].intValue() ) ).
			addRelationship( relationship.getType(), 
				(int) relationship.getId() );
		relCache.add( (int) relationship.getId(), relationship );
	}
	
	Object loadPropertyValue( int id )
	{
		try
		{
			return PersistenceManager.getManager().loadPropertyValue( id );
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			log.severe( "Failed loading property value[" +
				id + "]" );
		}
		return null;
	}
	
	List<Relationship> loadRelationships( NodeImpl node )
	{
		try
		{
			RawRelationshipData rawRels[] = 
				PersistenceManager.getManager().loadRelationships( node );
			List<Relationship> relList = new ArrayList<Relationship>();
			for ( RawRelationshipData rawRel : rawRels )
			{
				int relId = rawRel.getId();
				Relationship rel = relCache.get( relId );
				if ( rel == null )
				{
					RelationshipType type = getRelationshipTypeById( rawRel.getType() ); 
					if ( type == null )
					{
						// relationship with type that hasn't been registered
						continue;
					}
					rel = new RelationshipImpl( relId, 
						rawRel.getFirstNode(), rawRel.getSecondNode(), 
						type, false );
					relCache.add( relId, rel );
				}
				relList.add( rel );
			}
			return relList;
		}
		catch ( Exception e )
		{
			log.severe( "Failed loading relationships for node[" +
				node.getId() + "]" );
			throw new RuntimeException( e );
		}
	}
	
	RawPropertyData[] loadProperties( NodeImpl node )
	{
		try
		{
			RawPropertyData properties[]  = 
				PersistenceManager.getManager().loadProperties( node );
			return properties;
		}
		catch ( Exception e )
		{
			log.severe( "Failed loading properties for node[" +
				node.getId() + "]" );
			throw new RuntimeException( e );
		}
	}
	
	RawPropertyData[] loadProperties( RelationshipImpl relationship )
	{
		try
		{
			RawPropertyData properties[]  = 
				PersistenceManager.getManager().loadProperties( relationship );
			return properties;
		}
		catch ( Exception e )
		{
			log.severe( "Failed loading properties for relationship[" +
				relationship.getId() + "]" );
			throw new RuntimeException( e );
		}
	}
	
	int getNodeMaxCacheSize()
	{
		return nodeCache.maxSize();
	}
	
	int getRelationshipMaxCacheSize()
	{
		return relCache.maxSize();
	}
	
	public void clearCache()
	{
		nodeCache.clear();
		relCache.clear();
	}
	
	private void setRollbackOnly()
	{
		try
		{
			TransactionFactory.getTransactionManager().setRollbackOnly();
		}
		catch ( javax.transaction.SystemException se )
		{
			se.printStackTrace();
			log.severe( "Failed to set transaction rollback only" );
		}
	}

	private void acquireLock( Object resource, LockType lockType )
	{
		try
		{
			if ( lockType == LockType.READ )
			{
				LockManager.getManager().getReadLock( resource );
			}
			else if ( lockType == LockType.WRITE )
			{
				LockManager.getManager().getWriteLock( resource );
			}
			else
			{
				throw new RuntimeException( "Unkown lock type: " + lockType );
			}
		}
		catch ( IllegalResourceException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	private void releaseLock( Object resource, LockType lockType )
	{
		try
		{
			TransactionIsolationLevel level = 
				TransactionFactory.getTransactionIsolationLevel();
			if ( level == TransactionIsolationLevel.READ_COMMITTED )
			{
				if ( lockType == LockType.READ )
				{
					LockManager.getManager().releaseReadLock( resource );
				}
				else if ( lockType == LockType.WRITE )
				{
					CommandManager.getManager().addLockToTransaction( resource, 
						lockType );
				}
				else
				{
					throw new RuntimeException( "Unkown lock type: " + 
						lockType );
				}
			}
			else if ( level == TransactionIsolationLevel.BAD )
			{
				CommandManager.getManager().addLockToTransaction( resource, lockType );
			}
			else
			{
				throw new RuntimeException( 
					"Unkown transaction isolation level, " + level );
			}
		}
		catch ( NotInTransactionException e )
		{
			throw new RuntimeException( 
				"Unable to get transaction isolation level.", e );
		}
		catch ( LockNotFoundException e )
		{
			throw new RuntimeException( 
				"Unable to release locks.", e );
		}
		catch ( IllegalResourceException e )
		{
			throw new RuntimeException( 
				"Unable to release locks.", e );
		}
	}
	
	// only used during creation/loading of nodes/rels
	private void forceReleaseReadLock( Object resource )
	{
		try
		{
			LockManager.getManager().releaseReadLock( resource );
		}
		catch ( LockNotFoundException e )
		{
			throw new RuntimeException( 
				"Unable to release lock.", e );
		}
		catch ( IllegalResourceException e )
		{
			throw new RuntimeException( 
				"Unable to release lock.", e );
		}
	}
	
	public boolean isValidRelationship( Relationship rel )
	{
		try
		{
			return !( ( RelationshipImpl ) getRelForProxy( (int) rel.getId() ) 
				).isDeleted();
		}
		catch ( RuntimeException e )
		{
			return false;
		}
	}
	
	public boolean isValidRelationshipType( RelationshipType type )
	{
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		return rth.isValidRelationshipType( type );
	}

	public void addEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
    {
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		rth.addValidRelationshipTypes( relationshipTypes );
    }

	public RelationshipType registerRelationshipType( String name, 
		boolean create )
    {
		RelationshipTypeHolder rth = RelationshipTypeHolder.getHolder();
		return rth.addValidRelationshipType( name, create );
    }
	
	public int getHighestPossibleIdInUse( Class clazz )
	{
		return IdGenerator.getGenerator().getHighestPossibleIdInUse( clazz );
	}
	
	public int getNumberOfIdsInUse( Class clazz )
	{
		return IdGenerator.getGenerator().getNumberOfIdsInUse( clazz );
	}
}