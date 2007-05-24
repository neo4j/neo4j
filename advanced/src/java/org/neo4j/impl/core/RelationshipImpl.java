package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

import org.neo4j.impl.command.CommandManager;
import org.neo4j.impl.command.ExecuteFailedException;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.transaction.IllegalResourceException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockNotFoundException;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.transaction.TransactionIsolationLevel;


/**
 * This is the implementation of {@link Relationship}. Class 
 * <CODE>Relationship</CODE> has two different states/phases and lazy 
 * initialization to increase performance and decrease memory and resource 
 * In first phase (normal) it might have properties cached. 
 * Second phase (full) the whole relationship with all properties is loaded. 
 * Changes in phases are completely transparent to the user.
 * <p>
 * All public methods must be invoked within a transaction context, 
 * failure to do so will result in an exception. 
 * Modifing methods will create a command that can execute and undo the desired
 * operation. The command will be associated with the transaction and
 * if the transaction fails all the commands participating in the transaction 
 * will be undone.
 * <p>
 * Methods that uses commands will first create a command and verify that
 * we're in a transaction context. To persist operations a pro-active event 
 * is generated and will cause the 
 * {@link com.windh.kernel.persistence.BusinessLayerMonitor} to persist the 
 * then operation. 
 * If the event fails (false is returned)  the transaction is marked as 
 * rollback only and if the command will be undone.
 * <p>
 * This implementaiton of node does not rely on persistence storage to 
 * enforce contraints. This is done by the {@link NeoConstraintsListener}. 
 */
class RelationshipImpl 
	implements Relationship, Comparable
{
	private enum RelationshipPhase { NORMAL, FULL }
	
	private static Logger log = 
		Logger.getLogger( RelationshipImpl.class.getName() );
	private static NodeManager nodeManager = NodeManager.getManager(); 
	
	private int	id = -1;
	private int startNodeId = -1;
	private int endNodeId = -1;
	private RelationshipPhase phase = RelationshipPhase.NORMAL;
	private RelationshipType type = null;
	private Map<String,Property> propertyMap = new HashMap<String,Property>();
	private boolean isDeleted = false;
	
	/**
	 * Dummy constructor for NodeManager to acquire read lock on relationship
	 * when loading from PL.
	 */
	RelationshipImpl( int id )
	{
		// when using this constructor only equals and hashCode methods are 
		// valid
		this.id = id;
		isDeleted = true;
	}
	
	/**
	 * Creates a node in second phase so both nodes are referenced. If  
	 * <CODE>newRel</CODE> is <CODE>true</CODE> the phase will be set to 
	 * full.
	 *
	 * @param id the relationship id
	 * @param startNodeId the start node
	 * @param endNodeId the end node
	 * @param type relationship type
	 * @param directed <CODE>true</CODE> if directed relationship
	 * @param newRel true if this is a new relationship
	 * @throws IllegalArgumentException if null parameter or startNode == endNode
	 */
	RelationshipImpl( int id, int startNodeId, int endNodeId, 
		RelationshipType type, boolean newRel ) 
	{
		if ( type == null )
		{
			throw new IllegalArgumentException( "Null type" );
		}
		if ( startNodeId == endNodeId )
		{
			throw new IllegalArgumentException( "Start node equals end node" );
		}
		
		this.id = id;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
		if ( newRel )
		{
			this.phase = RelationshipPhase.FULL;
		}
		this.type = type;
	}
	
	/**
	 * Returns the relationship id.
	 *
	 * @return the unique relationship id
	 */
	public long getId()
	{
		return this.id;
	}
	
	/**
	 * Returns the two nodes for this relationship. If the node is directed
	 * the starting node is in first and the ending node is last, however
	 * this can only be asumed on {@link Direction directed} nodes.
	 * <p>
	 * If the relationship has a shallow node that node will be loaded and
	 * phase changed to normal.
	 *
	 * @return the two nodes for this relationship
	 */
	public Node[] getNodes()
	{
		try
		{
			return new Node[]
			{ 
				nodeManager.getNodeById( startNodeId ), 
				nodeManager.getNodeById( endNodeId ) 
			};
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	/**
	 * If <CODE>node</CODE> is one of the nodes in this relationship
	 * the other node is returned. If <CODE>node</CODE> is not in this
	 * relationship <CODE>null</CODE> is returned.
	 * <p>
	 * If the relationship has a shallow node that node will be loaded and
	 * phase changed to normal.
	 *
	 * @param node a node in this relationship
	 * @return the other node in this relationship
	 */
	public Node getOtherNode( Node node )
	{
		try
		{
			if ( startNodeId == (int) node.getId() )
			{
				return nodeManager.getNodeById( endNodeId );
			}
			if ( endNodeId == (int) node.getId() )
			{
				return nodeManager.getNodeById( startNodeId );
			}
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( e );
		}
		return null;
	}
	
	public Node getStartNode()
	{
		try
		{
			return nodeManager.getNodeById( startNodeId );
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public Node getEndNode()
	{
		try
		{
			return nodeManager.getNodeById( endNodeId );
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( e );
		}
	}

	/**
	 * Returns the {@link RelationshipType} of this relationship.
	 *
	 * @return the relationship type
	 */
	public RelationshipType getType()
	{
		return type;
	}
	
	/**
	 * Returns the property for <CODE>key</CODE>.
	 * <p>
	 * First the existing cache is checked for property <CODE>key</CODE>
	 * if the property is found it is returned at once. If not the full
	 * relaitonship is loaded and then the cache is checked again.
	 *
	 * @param key the property name
	 * @return the property object
	 * @throws NotFoundException if this property doesn't exist
	 */
	public Object getProperty( String key ) throws NotFoundException
	{
		acquireLock( this, LockType.READ );
		try
		{
			if ( propertyMap.containsKey( key ) )
			{
				return propertyMap.get( key ).getValue();
			}
			ensureFullRelationship();
			if ( propertyMap.containsKey( key ) )
			{
				return propertyMap.get( key ).getValue();
			}
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
		throw new NotFoundException( "" + key + 
			" property not found." );
	}
	
	public Object getProperty( String key, Object defaultValue )
	{
		if ( hasProperty( key ) )
		{
			return getProperty( key );
		}
		return defaultValue;	
	}
	
	/**
	 * Returns all properties on <CODE>this</CODE> relationship. The whole 
	 * relationship will be loaded (if not full phase already) to make sure 
	 * that all properties are present. 
	 *
	 * @return an object array containing all properties.
	 */
	public Iterable<Object> getPropertyValues()
	{
		acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationship();
	
			List<Object> properties = new ArrayList<Object>();
			for ( Property property : propertyMap.values() )
			{
				properties.add( property.getValue() );
			}
			return properties;
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}
	}
	
	/**
	 * Returns all property keys on <CODE>this</CODE> relationship. The whole 
	 * relationship will be loaded (if not full phase already) to make sure 
	 * that all properties are present.
	 *
	 * @return a string array containing all property keys.
	 */
	public Iterable<String> getPropertyKeys()
	{
		acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationship();
			List<String> propertyKeys = new ArrayList<String>();
			for ( String key : propertyMap.keySet() )
			{
				propertyKeys.add( key );
			}
			return propertyKeys;
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
	}

	/** 
	 * Returns true if this relationship has the property <CODE>key</CODE>.
	 * <p>
	 * First the existing cache is checked for property <CODE>key</CODE>
	 * if the property is found it is returned at once. If not the full
	 * relationship is loaded and then the cache is checked again.
	 *
	 * @param key the property name
	 * @return true if <CODE>key</CODE> property exists, else false
	 */
	public boolean hasProperty( String key )
	{
		acquireLock( this, LockType.READ );
		try
		{
			if ( propertyMap.containsKey( key ) )
			{
				return true;
			}
			ensureFullRelationship();
			return propertyMap.containsKey( key );
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
	}
	
	/**
	 * Adds a new property. Throws IllegalValueException if null parameter or 
	 * a property with <CODE>key</CODE> already exists.
	 * <p>
	 * The relationship will enter full phase since we must make sure that no 
	 * property <CODE>key</CODE> already exist.
	 *
	 * @param key the property name
	 * @param value the value of the property
	 * @throws IllegalValueException
	 */
	public void setProperty( String key, Object value ) 
		throws IllegalValueException
	{
		if ( !hasProperty( key ) )
		{
			addProperty( key, value );
		}
		else
		{
			changeProperty( key, value );
		}
	}
	
	void addProperty( String key, Object value ) 
		throws IllegalValueException
	{
		if ( key == null || value == null )
		{
			throw new IllegalValueException( "Null parameter, " +
				"key=" + key + ", " + "value=" + value );
		}
		acquireLock( this, LockType.WRITE );
		RelationshipCommands relationshipCommand = null;
		try
		{
			// must make sure we don't add already existing property
			ensureFullRelationship();
			relationshipCommand = new RelationshipCommands();
			relationshipCommand.setRelationship( this );
			relationshipCommand.initAddProperty( key, 
				new Property( -1, value ) );
			// have to execute command here since full relationship will be 
			// loaded and property already in cache
			relationshipCommand.execute();
			
			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( relationshipCommand );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIP_ADD_PROPERTY, 
				eventData ) )
			{
				setRollbackOnly();
				relationshipCommand.undo();
				throw new IllegalValueException( 
					"Generate pro-active event failed." );
			}

			em.generateReActiveEvent( Event.RELATIONSHIP_ADD_PROPERTY, 
				eventData );
		}
		catch ( ExecuteFailedException e )
		{
			relationshipCommand.undo();
			throw new IllegalValueException( "Failed executing command.", e );
		}
		finally
		{
			releaseLock( this, LockType.WRITE );
		}
	}
	
	/**
	 * Removes the property <CODE>key</CODE>. If null property <CODE>key</CODE> 
	 * or the property doesn't exist a <CODE>NotFoundException</CODE> is 
	 * thrown. 
	 * <p>
	 * If the relationship is in shallow or normal phase the cache is first 
	 * checked and if the property isn't found the relationship enters full 
	 * phase and the cache is checked again.
	 *
	 * @param key the property name
	 * @return the removed property value
	 * @throws NotFoundException
	 */
	public Object removeProperty( String key ) throws NotFoundException
	{
		if ( key == null )
		{
			throw new NotFoundException( "Null parameter." );
		}
		acquireLock( this, LockType.WRITE );
		RelationshipCommands relationshipCommand = null;
		try
		{
			ensureFullRelationship();
			relationshipCommand = new RelationshipCommands();
			relationshipCommand.setRelationship( this );
			relationshipCommand.initRemoveProperty( 
				doGetProperty( key ).getId(), key );
			// have to execute here for RelationshipOperationEventData to be 
			// command also checks that the property really exist
			relationshipCommand.execute();

			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( relationshipCommand );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY, 
				eventData ) )
			{
				setRollbackOnly();
				relationshipCommand.undo();
				throw new NotFoundException( 
					"Generate pro-active event failed." );
			}

			em.generateReActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY, 
				eventData );
			return relationshipCommand.getOldProperty();
		}
		catch ( ExecuteFailedException e )
		{
			relationshipCommand.undo();
			throw new NotFoundException( "Failed executing command.", e );
		}
		finally
		{
			releaseLock( this, LockType.WRITE );
		}
	}
	
	Object changeProperty( String key, Object newValue ) 
		throws IllegalValueException, NotFoundException
	{
		if ( key == null || newValue == null )
		{
			throw new IllegalValueException( "Null parameter, " +
				"key=" + key + ", " + "value=" + newValue );
		}
		acquireLock( this, LockType.WRITE );
		RelationshipCommands relationshipCommand = null;
		try
		{
			// if null or not found make sure full
			ensureFullRelationship();
			relationshipCommand = new RelationshipCommands();
			relationshipCommand.setRelationship( this );
			int propertyId = doGetProperty( key ).getId();
			relationshipCommand.initChangeProperty( propertyId, key, new 
				Property( propertyId, newValue )  );
			// have to execute here for RelationshipOperationEventData to be 
			// command also checks that the property really exist
			relationshipCommand.execute();

			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( relationshipCommand );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIP_CHANGE_PROPERTY, 
				eventData ) )
			{
				setRollbackOnly();
				relationshipCommand.undo();
				throw new IllegalValueException( 
					"Generate pro-active event failed." );
			}

			em.generateReActiveEvent( Event.RELATIONSHIP_CHANGE_PROPERTY, eventData );
			return relationshipCommand.getOldProperty();
		}
		catch ( ExecuteFailedException e )
		{
			setRollbackOnly();
			relationshipCommand.undo();
			throw new IllegalValueException( "Failed executing command.", e );
		}
		finally
		{
			releaseLock( this, LockType.WRITE );
		}
	}

	/**
	 * Deletes this relationship removing it from cache and persistent storage. 
	 * If unable to delete a <CODE>DeleteException</CODE> is thrown.
	 * <p>
	 * The phase is never changed during this operation. The relationship will 
	 * be removed from cache and invoking any method on this relationship after 
	 * delete is invalid. Doing so might result in a checked or runtime 
	 * exception beeing thrown. 
	 *
	 * @throws DeleteException if unable to delete
	 */
	public void delete() //throws DeleteException
	{
		RelationshipCommands relationshipCommand = null;
		Node startNode = null;
		Node endNode = null;
		boolean startNodeLocked = false;
		boolean endNodeLocked = false;
		acquireLock( this, LockType.WRITE );
		try
		{
			startNode = nodeManager.getNodeForProxy( startNodeId ); 
			acquireLock( startNode, LockType.WRITE );
			startNodeLocked = true;
			endNode = nodeManager.getNodeForProxy( endNodeId );
			acquireLock( endNode, LockType.WRITE );
			endNodeLocked = true;
			// no need to load full relationship, all properties will be 
			// deleted when relationship is deleted 
			relationshipCommand = new RelationshipCommands();
			relationshipCommand.setRelationship( this );
			relationshipCommand.initDelete();

			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( relationshipCommand );
			if ( !em.generateProActiveEvent( Event.RELATIONSHIP_DELETE, 
					eventData ) )
			{
				setRollbackOnly();
				throw new DeleteException( 
					"Generate pro-active event failed." );
			}

			// full phase here isn't necessary, if something breaks we still
			// have the relationship as it was in memory and the transaction 
			// will rollback so the full relationship will still be persistent
			relationshipCommand.execute();
			em.generateReActiveEvent( Event.RELATIONSHIP_DELETE, eventData );
		}
		catch ( ExecuteFailedException e )
		{
			setRollbackOnly();
			relationshipCommand.undo();
			throw new DeleteException( "Failed executing command.", e );
		}
		finally
		{
			boolean releaseFailed = false;
			try
			{
				if ( startNodeLocked )
				{
					releaseLock( startNode, LockType.WRITE );
				}
			}
			catch ( Exception e )
			{
				releaseFailed = true;
				e.printStackTrace();
				log.severe( "Failed to release lock" );
			}
			try
			{
				if ( endNodeLocked )
				{
					releaseLock( endNode, LockType.WRITE );
				}
			}
			catch ( Exception e )
			{
				releaseFailed = true;
				e.printStackTrace();
				log.severe( "Failed to release lock" );
			}
			releaseLock( this, LockType.WRITE );
			if ( releaseFailed )
			{
				throw new RuntimeException( "Unable to release locks [" + 
					startNode + "," + endNode + "] in relationship delete->" + 
					this );
			}
		}
	}

	/**
	 * To string method.
	 *
	 * return a string representation of this relationship
	 */
	public String toString()
	{
		return	"Relationship #" + this.getId() + " of type " + type + 
			" between Node[" + startNodeId + "] and Node[" + endNodeId + "]";
	}
	
	/**
	 * If object <CODE>rel</CODE> is a relationship, 0 is returned if 
	 * <CODE>this</CODE> relationship id equals <CODE>rel's</CODE> relationship 
	 * id, 1 if <CODE>this</CODE> relationship id is greater and -1 else.
	 * <p>
	 * If <CODE>rel</CODE> isn't a relationship a ClassCastException will be 
	 * thrown.
	 *
	 * @param rel the relationship to compare this node with
	 * @return 0 if equal id, 1 if this id is greater else -1
	 */
	// TODO: Verify this implementation
	public int compareTo( Object rel )
	{
		Relationship r = (Relationship) rel;
		int ourId = (int) this.getId(), theirId = (int) r.getId();
		
		if ( ourId < theirId )
		{
			return -1;
		}
		else if ( ourId > theirId )
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}

	/**
	 * Returns true if object <CODE>o</CODE> is a relationship with the same id
	 * as <CODE>this</CODE>.
	 *
	 * @param o the object to compare
	 * @return true if equal, else false
	 */
	public boolean equals( Object o )
	{
		// the id check bellow isn't very expensive so this performance 
		// optimization isn't worth it
		// if ( this == o )
		// {
		// 	return true;
		// }

		// verify type and not null, should use Node inteface
		if ( !(o instanceof Relationship) )
		{
			return false;
		}
		
		// The equals contract:
		// o reflexive: x.equals(x)
		// o symmetric: x.equals(y) == y.equals(x)
		// o transitive: ( x.equals(y) && y.equals(z) ) == true 
		//				 then x.equals(z) == true
		// o consistent: the nodeId never changes
		return this.getId() == ((Relationship) o).getId();
		
	}
	
	private volatile int hashCode = 0;
	
	public int hashCode()
	{
		// hashcode contract:
		// 1. must return the same result for the same object consistenlty
		// 2. if two objects are equal they must produce the same hashcode
		// also two distinct object should (not required) produce different
		// hash values, ideally a hash function should distribute any 
		// collection uniformly across all possible hash values
		
		// we have 
		if ( hashCode == 0 )
		{
			// this one is so leet, if you don't understand, that is ok...
			// you're just not on the same elitness level as some of us, 
			// nothing to be ashamed of
			hashCode = 3217 * (int) this.getId();
		}
		return hashCode;

		// or maybe this is enough when we have zillions of nodes?
		// return this.id;
	}

	Integer[] getNodeIds()
	{
		return new Integer[] 
		{ 
			new Integer( startNodeId ), 
			new Integer( endNodeId ) 
		};
	}

	// caller responsible for acquiring lock
	void doAddProperty( String key, Property property ) 
		throws IllegalValueException
	{
		if ( propertyMap.containsKey( key ) )
		{
			throw new IllegalValueException( "Property[" + key + 
				"] already added." );
		}
		propertyMap.put( key, property );
	}
	
	// caller responsible for acquiring lock
	Property doRemoveProperty( String key ) throws NotFoundException
	{
		if ( propertyMap.containsKey( key ) )
		{
			return propertyMap.remove( key );
		}
		throw new NotFoundException( "Property not found: " +	key );
	}

	// caller responsible for acquiring lock
	Property doChangeProperty( String key, Property newValue ) 
		throws IllegalValueException, NotFoundException
	{
		if ( propertyMap.containsKey( key ) )
		{
			Property oldValue  = propertyMap.get( key );
			// propertyMap.put( key, newValue );
			if ( !oldValue.getValue().getClass().equals( 
					newValue.getValue().getClass() ) )
			{
				throw new IllegalValueException( "New value[" + 
					newValue.getValue() + 
					" not same type as old value[" + 
					oldValue.getValue() + "]" );
			}
			propertyMap.put( key, newValue );
			return oldValue;
		}
		throw new NotFoundException( "Property not found: " + key );
	}

	Property doGetProperty( String key ) throws NotFoundException
	{
		if ( propertyMap.containsKey( key ) )
		{
			return propertyMap.get( key );
		}
		throw new NotFoundException( "Property not found: " + key );
	}
	
	private void setRollbackOnly()
	{
		try
		{
			TransactionFactory.getTransactionManager().setRollbackOnly();
		}
		catch ( javax.transaction.SystemException se )
		{
			log.severe( "Failed to set transaction rollback only" );
		}
	}

	private void ensureFullRelationship()
	{
		if ( phase != RelationshipPhase.FULL )
		{
			RawPropertyData[] rawProperties = 
				NodeManager.getManager().loadProperties( this );
			Set<Integer> addedProps = new LinkedHashSet<Integer>();
			Map<String,Property> newPropertyMap = 
				new HashMap<String,Property>();
			for ( RawPropertyData propData : rawProperties )
			{
				int propId = propData.getId();
				assert !addedProps.contains( propId );
				addedProps.add( propId );
				Property property = new Property( propId, 
					propData.getValue() );
				newPropertyMap.put( propData.getKey(), property );
			}
			for ( String key : this.propertyMap.keySet() )
			{
				Property prop = propertyMap.get( key );
				if ( !addedProps.contains( prop.getId() ) )
				{
					newPropertyMap.put( key, prop );
				}
			}
			this.propertyMap = newPropertyMap;
			this.phase = RelationshipPhase.FULL;
		}
	}		

	private void acquireLock( Object resource, LockType lockType )
	{
		try
		{
			// make sure we're in transaction
			TransactionFactory.getTransactionIsolationLevel();
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
		catch ( NotInTransactionException e )
		{
			throw new RuntimeException( 
				"Unable to get transaction isolation level.", e );
		}
		catch ( IllegalResourceException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	private void releaseLock( Object resource, LockType lockType )
	{
		releaseLock( resource, lockType, false );
	}

	private void releaseLock( Object resource, LockType lockType, 
		boolean forceRelease )
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
					if ( forceRelease ) 
					{
						LockManager.getManager().releaseWriteLock( resource );
					}
					else
					{
						CommandManager.getManager().addLockToTransaction( resource, 
							lockType );
					}
				}
				else
				{
					throw new RuntimeException( "Unkown lock type: " + 
						lockType );
				}
			}
			else if ( level == TransactionIsolationLevel.BAD )
			{
				CommandManager.getManager().addLockToTransaction( resource, 
					lockType );
			}
			else
			{
				throw new RuntimeException( 
					"Unkown transaction isolation level, " + level );
			}
		}
		catch ( NotInTransactionException e )
		{
			e.printStackTrace();
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

	boolean isDeleted()
	{
		return isDeleted;
	}
	
	void setIsDeleted( boolean flag )
	{
		isDeleted = flag;
	}

	// TODO: remove this method once we have non corupt persistence
	boolean propertyAlreadyInCache( String key )
	{
		if ( propertyMap.containsKey( key ) )
		{
			return true;
		}
		return false;
	}
}