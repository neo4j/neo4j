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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.transaction.IllegalResourceException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockNotFoundException;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;

class RelationshipImpl 
	implements Relationship, Comparable<Relationship>
{
	private static enum RelationshipPhase { NORMAL, FULL }
	
	private static Logger log = 
		Logger.getLogger( RelationshipImpl.class.getName() );
	private static final NodeManager nodeManager = NodeManager.getManager();
	private static final LockManager lockManager = LockManager.getManager();
	private static final LockReleaser lockReleaser = LockReleaser.getManager();
	
	private final int id;
	private final int startNodeId;
	private final int endNodeId;
	private RelationshipPhase phase = RelationshipPhase.NORMAL;
	private final RelationshipType type;
	private ArrayMap<Integer,Property> propertyMap = null;
		// new ArrayMap<Integer,Property>( 9, false, true );
	private boolean isDeleted = false;
	
	// Dummy constructor for NodeManager to acquire read lock on relationship
	// when loading from PL.
	RelationshipImpl( int id )
	{
		// when using this constructor only equals and hashCode methods are 
		// valid
		this.id = id;
		this.startNodeId = -1;
		this.endNodeId = -1;
		this.type = null;
		isDeleted = true;
	}
	
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
	
	public long getId()
	{
		return this.id;
	}
	
	public Node[] getNodes()
	{
		try
		{
			return new Node[]
			{ 
				new NodeProxy( startNodeId ),
				new NodeProxy( endNodeId )
			};
		}
		catch ( NotFoundException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public Node getOtherNode( Node node )
	{
		if ( startNodeId == (int) node.getId() )
		{
			// return nodeManager.getNodeById( endNodeId );
			return new NodeProxy( endNodeId );
		}
		if ( endNodeId == (int) node.getId() )
		{
			// return nodeManager.getNodeById( startNodeId );
			return new NodeProxy( startNodeId );
		}
		throw new RuntimeException( "Node[" + node.getId() + 
			"] not connected to this relationship[" + getId() + "]" );
	}
	
	public Node getStartNode()
	{
		// return nodeManager.getNodeById( startNodeId );
		return new NodeProxy( startNodeId );
	}
	
	int getStartNodeId()
	{
		return startNodeId;
	}
	
	public Node getEndNode()
	{
		// return nodeManager.getNodeById( endNodeId );
		return new NodeProxy( endNodeId );
	}
	
	int getEndNodeId()
	{
		return endNodeId;
	}

	public RelationshipType getType()
	{
		return type;
	}

	public boolean isType( RelationshipType type )
    {
		return type != null && type.name().equals( this.getType().name() );
    }
	
	public Object getProperty( String key ) throws NotFoundException
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "null key" );
		}
		acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : PropertyIndex.index( key ) )
			{
				Property property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.get( index.getKeyId() );
				}
				if ( property != null )
				{
					return property.getValue();
				}
				
				if ( ensureFullRelationship() )
				{
					property = propertyMap.get( index.getKeyId() );
					if ( property != null )
					{
						return property.getValue();
					}
				}
			}
			if ( !PropertyIndex.hasAll() )
			{
				ensureFullRelationship();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !PropertyIndex.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = 
							PropertyIndex.getIndexFor( keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							return propertyMap.get( 
								indexToCheck.getKeyId() ).getValue();
						}
					}
				}
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
		acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : PropertyIndex.index( key ) )
			{
				Property property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.get( index.getKeyId() );
				}
				if ( property != null )
				{
					return property.getValue();
				}
				
				if ( ensureFullRelationship() )
				{
					property = propertyMap.get( index.getKeyId() );
					if ( property != null )
					{
						return property.getValue();
					}
				}
			}
			if ( !PropertyIndex.hasAll() )
			{
				ensureFullRelationship();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !PropertyIndex.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = 
							PropertyIndex.getIndexFor( keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							return propertyMap.get( 
								indexToCheck.getKeyId() ).getValue();
						}
					}
				}
			}
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
		return defaultValue;	
	}
	
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
	
	public Iterable<String> getPropertyKeys()
	{
		acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationship();
			List<String> propertyKeys = new ArrayList<String>();
			for ( int index : propertyMap.keySet() )
			{
				propertyKeys.add( PropertyIndex.getIndexFor( index ).getKey() );
			}
			return propertyKeys;
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
	}

	public boolean hasProperty( String key )
	{
		acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : PropertyIndex.index( key ) )
			{
				Property property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.get( index.getKeyId() );
				}
				if ( property != null )
				{
					return true;
				}
				if ( ensureFullRelationship() )
				{
					if ( propertyMap.get( index.getKeyId() ) != null )
					{
						return true;
					}
				}
			}
			ensureFullRelationship();
			for ( int keyId : propertyMap.keySet() )
			{
				PropertyIndex indexToCheck = PropertyIndex.getIndexFor( keyId );
				if ( indexToCheck.getKey().equals( key ) )
				{
					return true;
				}
			}
			return false;
		}
		finally
		{
			releaseLock( this, LockType.READ );
		}			
	}
	
	public void setProperty( String key, Object value ) 
		throws IllegalValueException
	{
		if ( key == null || value == null )
		{
			throw new IllegalValueException( "Null parameter, " +
				"key=" + key + ", " + "value=" + value );
		}
		acquireLock( this, LockType.WRITE );
		try
		{
			// must make sure we don't add already existing property
			ensureFullRelationship();
			PropertyIndex index = null;
			Property property = null;
			for ( PropertyIndex cachedIndex : PropertyIndex.index( key ) )
			{
				property = propertyMap.get( cachedIndex.getKeyId() );
				index = cachedIndex;
				if ( property != null )
				{
					break;
				}
			}
			if ( property == null && !PropertyIndex.hasAll() )
			{
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !PropertyIndex.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = PropertyIndex.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							index = indexToCheck;
							property = propertyMap.get( indexToCheck.getKeyId() );
							break;
						}
					}
				}
			}
			if ( index == null )
			{
				index = PropertyIndex.createPropertyIndex( key );
			}
			Event event = Event.RELATIONSHIP_ADD_PROPERTY;
			RelationshipOpData data;
			if ( property != null )
			{
				int propertyId = property.getId();
				data = new RelationshipOpData( this, id, propertyId, index, 
					value );
				event = Event.RELATIONSHIP_CHANGE_PROPERTY;
			}
			else
			{
				data = new RelationshipOpData( this, id, -1, index, value );
			}
			
			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( data );
			if ( !em.generateProActiveEvent( event, eventData ) )
			{
				setRollbackOnly();
				throw new IllegalValueException( 
					"Generate pro-active event failed." );
			}
			if ( event == Event.RELATIONSHIP_ADD_PROPERTY )
			{
				doAddProperty( index, new Property( data.getPropertyId(), 
					value ) );
			}
			else
			{
				doChangeProperty( index, new Property( data.getPropertyId(), 
					value ) );
			}

			em.generateReActiveEvent( event, eventData );
		}
		finally
		{
			releaseLock( this, LockType.WRITE );
		}
	}
	
	public Object removeProperty( String key )
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "Null parameter." );
		}
		acquireLock( this, LockType.WRITE );
		try
		{
			// if not found just return null
			Property property = null;
			for ( PropertyIndex cachedIndex : PropertyIndex.index( key ) )
			{
				property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.remove( cachedIndex.getKeyId() );
				}
				if ( property == null )
				{
					if ( ensureFullRelationship() )
					{
						property = propertyMap.remove( cachedIndex.getKeyId() );
						if ( property != null )
						{
							break;
						}
					}
				}
				else
				{
					break;
				}
			}
			if ( property == null && !PropertyIndex.hasAll() )
			{
				ensureFullRelationship();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !PropertyIndex.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = PropertyIndex.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							property = propertyMap.remove( indexToCheck.getKeyId() );
							break;
						}
					}
				}
			}
			if ( property == null )
			{
				return null;
			}
			RelationshipOpData data = new RelationshipOpData( this, id, 
				property.getId() );
			EventManager em = EventManager.getManager();
			EventData eventData = new EventData( data );
		if ( !em.generateProActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY, 
				eventData ) )
			{
				setRollbackOnly();
				throw new NotFoundException( 
					"Generate pro-active event failed." );
			}

			em.generateReActiveEvent( Event.RELATIONSHIP_REMOVE_PROPERTY, 
				eventData );
			return property.getValue();
		}
		finally
		{
			releaseLock( this, LockType.WRITE );
		}
	}
	
	public void delete() //throws DeleteException
	{
		NodeImpl startNode = null;
		NodeImpl endNode = null;
		boolean startNodeLocked = false;
		boolean endNodeLocked = false;
		acquireLock( this, LockType.WRITE );
		try
		{
			startNode = nodeManager.getLightNode( startNodeId ); 
			if ( startNode != null )
			{
				acquireLock( startNode, LockType.WRITE );
				startNodeLocked = true;
			}
			endNode = nodeManager.getLightNode( endNodeId );
			if ( endNode != null )
			{
				acquireLock( endNode, LockType.WRITE );
				endNodeLocked = true;
			}
			// no need to load full relationship, all properties will be 
			// deleted when relationship is deleted
			
			EventManager em = EventManager.getManager();
			int typeId = RelationshipTypeHolder.getHolder().getIdFor( type ); 
			EventData eventData = new EventData( new RelationshipOpData( this, 
				id, typeId, startNodeId, endNodeId ) );
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
			if ( startNode != null )
			{
				startNode.removeRelationship( type, id );
			}
			if ( endNode != null )
			{
				endNode.removeRelationship( type, id );
			}
			nodeManager.removeRelationshipFromCache( id );
			
			em.generateReActiveEvent( Event.RELATIONSHIP_DELETE, eventData );
		}
		finally
		{
			boolean releaseFailed = false;
			try
			{
				if ( startNodeLocked )
				{
					releaseLock( startNode, LockType.WRITE ); //, level );
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
					releaseLock( endNode, LockType.WRITE ); //, level );
				}
			}
			catch ( Exception e )
			{
				releaseFailed = true;
				e.printStackTrace();
				log.severe( "Failed to release lock" );
			}
			releaseLock( this, LockType.WRITE ); //, level );
			if ( releaseFailed )
			{
				throw new RuntimeException( "Unable to release locks [" + 
					startNode + "," + endNode + "] in relationship delete->" + 
					this );
			}
		}
	}

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
	public int compareTo( Relationship r )
	{
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
	
	public int hashCode()
	{
		return id;
	}

	// caller responsible for acquiring lock
	void doAddProperty( PropertyIndex index, Property property ) 
		throws IllegalValueException
	{
		if ( propertyMap.get( index.getKeyId() ) != null )
		{
			throw new IllegalValueException( "Property[" + index.getKey() + 
				"] already added." );
		}
		propertyMap.put( index.getKeyId(), property );
	}
	
	// caller responsible for acquiring lock
	Property doRemoveProperty( PropertyIndex index ) throws NotFoundException
	{
		Property property = propertyMap.remove( index.getKeyId() );
		if ( property != null )
		{
			return property;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
	}

	// caller responsible for acquiring lock
	Property doChangeProperty( PropertyIndex index, Property newValue ) 
		throws IllegalValueException, NotFoundException
	{
		Property property = propertyMap.get( index.getKeyId() );
		if ( property != null )
		{
			Property oldProperty = new Property( property.getId(), 
				property.getValue() );
			property.setNewValue( newValue.getValue() );
			return oldProperty;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
	}

	Property doGetProperty( PropertyIndex index ) throws NotFoundException
	{
		Property property = propertyMap.get( index.getKeyId() );
		if ( property != null )
		{
			return property;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
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

	private boolean ensureFullRelationship()
	{
		if ( phase != RelationshipPhase.FULL )
		{
			RawPropertyData[] rawProperties = 
				nodeManager.loadProperties( this );
			ArrayIntSet addedProps = new ArrayIntSet();
			ArrayMap<Integer,Property> newPropertyMap = 
				new ArrayMap<Integer,Property>(); 
			for ( RawPropertyData propData : rawProperties )
			{
				int propId = propData.getId();
				assert !addedProps.contains( propId );
				addedProps.add( propId );
				Property property = new Property( propId, 
					propData.getValue() );
				newPropertyMap.put( propData.getIndex(), property );
			}
			if ( propertyMap != null )
			{
				for ( int index : this.propertyMap.keySet() )
				{
					Property prop = propertyMap.get( index );
					if ( !addedProps.contains( prop.getId() ) )
					{
						newPropertyMap.put( index, prop );
					}
				}
			}
			this.propertyMap = newPropertyMap;
			this.phase = RelationshipPhase.FULL;
			return true;
		}
		else
		{
			if ( propertyMap == null )
			{
				propertyMap = 
					new ArrayMap<Integer,Property>( 9, false, true );
			}
		}
		return false;
	}		

	private void acquireLock( Object resource, LockType lockType )
	{
		try
		{
			if ( lockType == LockType.READ )
			{
				lockManager.getReadLock( resource );
			}
			else if ( lockType == LockType.WRITE )
			{
				lockManager.getWriteLock( resource );
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
		try
		{
			if ( lockType == LockType.READ )
			{
				lockManager.releaseReadLock( resource );
			}
			else if ( lockType == LockType.WRITE )
			{
				lockReleaser.addLockToTransaction( resource, lockType );
			}
			else
			{
				throw new RuntimeException( "Unkown lock type: " + 
					lockType );
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
}