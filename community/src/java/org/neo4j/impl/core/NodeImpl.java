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

// Java imports
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;


class NodeImpl implements Node, Comparable<Node>
{
	private static enum NodePhase { 
		EMPTY_PROPERTY, 
		FULL_PROPERTY,
		EMPTY_REL,
		FULL_REL }
	
//	private static Logger log = Logger.getLogger( NodeImpl.class.getName() );
//	private static final LockManager lockManager = LockManager.getManager();
//	private static final LockReleaser lockReleaser = LockReleaser.getManager();
//	private static final NodeManager nodeManager = NodeManager.getManager();
//	private static final TraverserFactory travFactory = 
//		TraverserFactory.getFactory();
	
	private final int id;
	private boolean isDeleted = false;
	private NodePhase nodePropPhase;
	private NodePhase nodeRelPhase;
	private ArrayMap<String,ArrayIntSet> relationshipMap = 
		new ArrayMap<String,ArrayIntSet>(); 
	private ArrayMap<Integer,Property> propertyMap = null; 
	
	private final NodeManager nodeManager; 

	NodeImpl( int id, NodeManager nodeManager )
	{
		this.id = id;
		this.nodeManager = nodeManager;
		this.nodePropPhase = NodePhase.EMPTY_PROPERTY;
		this.nodeRelPhase = NodePhase.EMPTY_REL;
	}
	
	// newNode will only be true for NodeManager.createNode 
	NodeImpl( int id, boolean newNode, NodeManager nodeManager )
	{
		this.id = id;
		this.nodeManager = nodeManager;
		if ( newNode )
		{
			this.nodePropPhase = NodePhase.FULL_PROPERTY;
			this.nodeRelPhase = NodePhase.FULL_REL;
		}
	}
		
	public long getId()
	{
		return this.id;
	}
	
	public Iterable<Relationship> getRelationships()
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationships();
			if ( relationshipMap == null )
			{
				return Collections.emptyList();
			}
			
			Iterator<ArrayIntSet> values = relationshipMap.values().iterator();
			int size = 0;
			while ( values.hasNext() )
			{
				ArrayIntSet relTypeSet = values.next();
				size += relTypeSet.size();
			}
			values = relationshipMap.values().iterator();
			int[] ids = new int[size];
			int position = 0;
			while ( values.hasNext() )
			{
				ArrayIntSet relTypeSet = values.next();
				for ( int relId : relTypeSet.values() )
				{
					ids[position++] = relId;
				}
			}
			return new RelationshipIterator( ids, this, Direction.BOTH, 
				nodeManager );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}
	
	public Iterable<Relationship> getRelationships( Direction dir )
	{
		if ( dir == Direction.BOTH )
		{
			return getRelationships();
		}
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationships();
			if ( relationshipMap == null )
			{
				return Collections.emptyList();
			}
			
			Iterator<ArrayIntSet> values = relationshipMap.values().iterator();
			int size = 0;
			while ( values.hasNext() )
			{
				ArrayIntSet relTypeSet = values.next();
				size += relTypeSet.size();
			}
			values = relationshipMap.values().iterator();
			int[] ids = new int[size];
			int position = 0;
			while ( values.hasNext() )
			{
				ArrayIntSet relTypeSet = values.next();
				for ( int relId : relTypeSet.values() )
				{
					ids[position++] = relId;
				}
			}
			return new RelationshipIterator( ids, this, dir, nodeManager );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}
	
	public Iterable<Relationship> getRelationships( RelationshipType type )
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationships();
			if ( relationshipMap == null )
			{
				return Collections.emptyList();
			}
			ArrayIntSet relationshipSet = relationshipMap.get( type.name() );
			if ( relationshipSet == null )
			{
				return Collections.emptyList();
			}
			int[] ids = new int[relationshipSet.size()];
			int position = 0;
			for ( int relId : relationshipSet.values() )
			{
				ids[position++] = relId;
			}
			return new RelationshipIterator( ids, this, Direction.BOTH, 
				nodeManager );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}

	public Iterable<Relationship> getRelationships( RelationshipType... types )
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationships();
			int size = 0;
			for ( RelationshipType type : types )
			{
				ArrayIntSet relTypeSet = relationshipMap.get( type.name() );
				if ( relTypeSet != null )
				{
					size += relTypeSet.size();
				}
			}
			int[] ids = new int[size];
			int position = 0;
			for ( RelationshipType type : types )
			{
				ArrayIntSet relTypeSet = relationshipMap.get( type.name() );
				if ( relTypeSet != null )
				{
					for ( int relId : relTypeSet.values() )
					{
						ids[position++] = relId;
					}
				}
			}
			return new RelationshipIterator( ids, this, Direction.BOTH, 
				nodeManager );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}
	
	public Relationship getSingleRelationship( RelationshipType type, 
		Direction dir )
	{
		Iterator<Relationship> rels = getRelationships( type, dir ).iterator();
		if ( !rels.hasNext() )
		{
			return null;
		}
		Relationship rel = rels.next();
		if ( rels.hasNext() )
		{
			throw new NotFoundException( "More then one relationship[" + type 
				+ "] found" );
		}
		return rel;
	}

	public Iterable<Relationship> getRelationships( RelationshipType type, 
		Direction dir )
	{
		if ( dir == Direction.BOTH )
		{
			return getRelationships( type );
		}
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullRelationships();
			if ( relationshipMap == null )
			{
				return Collections.emptyList();
			}
			ArrayIntSet relationshipSet = relationshipMap.get( type.name() );
			if ( relationshipSet == null )
			{
				return Collections.emptyList();
			}
			int[] ids = new int[relationshipSet.size()];
			int position = 0;
			for ( int relId : relationshipSet.values() )
			{
				ids[position++] = relId;
			}
			return new RelationshipIterator( ids, this, dir, nodeManager );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}
	

	public void delete() // throws DeleteException
	{
		nodeManager.acquireLock( this, LockType.WRITE );
		try
		{
			EventData eventData = new EventData( new NodeOpData( this, id ) );
			if ( !nodeManager.generateProActiveEvent( Event.NODE_DELETE, 
				eventData ) )
			{
				nodeManager.setRollbackOnly();
				throw new DeleteException( 
					"Generate pro-active event failed, " + 
					"unable to delete " + this );
			}
			// normal node phase here isn't necessary, if something breaks we 
			// still have the node as it was in memory and the transaction will 
			// rollback so the full node will still be persistent
			nodeRelPhase = NodePhase.EMPTY_REL;
			nodePropPhase = NodePhase.EMPTY_PROPERTY;
			relationshipMap = new ArrayMap<String,ArrayIntSet>(); 
			propertyMap = new ArrayMap<Integer,Property>( 9, false, true );
			
			nodeManager.removeNodeFromCache( id );
			nodeManager.generateReActiveEvent( Event.NODE_DELETE, eventData );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.WRITE );
		}
	}
	
	/**
	 * Returns all properties on <CODE>this</CODE> node. The whole node will be 
	 * loaded (if not full phase already) to make sure that all properties are 
	 * present. 
	 *
	 * @return an object array containing all properties.
	 */
	public Iterable<Object> getPropertyValues()
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullProperties();
			List<Object> properties = new ArrayList<Object>();
			for ( Property property : propertyMap.values() )
			{
				properties.add( getPropertyValue( property ) );
			}
			return properties;
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
	}
	
	/**
	 * Returns all property keys on <CODE>this</CODE> node. The whole node will 
	 * be loaded (if not full phase already) to make sure that all properties 
	 * are present.
	 *
	 * @return a string array containing all property keys.
	 */
	public Iterable<String> getPropertyKeys()
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			ensureFullProperties();
			List<String> propertyKeys = new ArrayList<String>();
			for ( int keyId : propertyMap.keySet() )
			{
				propertyKeys.add( nodeManager.getIndexFor( keyId ).getKey() );
			}
			return propertyKeys;
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}			
	}

	public Object getProperty( String key ) 
		throws NotFoundException
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "null key" );
		}
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : nodeManager.index( key ) )
			{
				Property property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.get( index.getKeyId() );
				}
				if ( property != null )
				{
					return getPropertyValue( property );
				}
				
				if ( ensureFullProperties() )
				{
					property = propertyMap.get( index.getKeyId() );
					if ( property != null )
					{
						return getPropertyValue( property );
					}
				}
			}
			if ( !nodeManager.hasAllPropertyIndexes() )
			{
				ensureFullProperties();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !nodeManager.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = nodeManager.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							Property property = propertyMap.get( 
								indexToCheck.getKeyId() );
							return getPropertyValue( property );
						}
					}
				}
			}
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
		throw new NotFoundException( "" + key + " property not found." );
	}
	
	public Object getProperty( String key, Object defaultValue )
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "null key" );
		}
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : nodeManager.index( key ) )
			{
				Property property = null;
				if ( propertyMap != null ) 
				{
					property = propertyMap.get( index.getKeyId() );
				}
				if ( property != null )
				{
					return getPropertyValue( property );
				}
				
				if ( ensureFullProperties() )
				{
					property = propertyMap.get( index.getKeyId() );
					if ( property != null )
					{
						return getPropertyValue( property );
					}
				}
			}
			if ( !nodeManager.hasAllPropertyIndexes() )
			{
				ensureFullProperties();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !nodeManager.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = nodeManager.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							Property property = propertyMap.get( 
								indexToCheck.getKeyId() );
							return getPropertyValue( property );
						}
					}
				}
			}
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
		}
		return defaultValue;
	}

	public boolean hasProperty( String key )
	{
		nodeManager.acquireLock( this, LockType.READ );
		try
		{
			for ( PropertyIndex index : nodeManager.index( key ) )
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
				if ( ensureFullProperties() )
				{
					if ( propertyMap.get( index.getKeyId() ) != null )
					{
						return true;
					}
				}
			}
			ensureFullProperties();
			for ( int keyId : propertyMap.keySet() )
			{
				PropertyIndex indexToCheck = nodeManager.getIndexFor( keyId );
				if ( indexToCheck.getKey().equals( key ) )
				{
					return true;
				}
			}
			return false;
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.READ );
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
		nodeManager.acquireLock( this, LockType.WRITE );
		try
		{
			// must make sure we don't add already existing property
			ensureFullProperties();
			PropertyIndex index = null;
			Property property = null;
			for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
			{
				property = propertyMap.get( cachedIndex.getKeyId() );
				index = cachedIndex;
				if ( property != null )
				{
					break;
				}
			}
			if ( property == null && !nodeManager.hasAllPropertyIndexes() )
			{
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !nodeManager.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = nodeManager.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							index = indexToCheck;
							property = propertyMap.get( 
								indexToCheck.getKeyId() );
							break;
						}
					}
				}
			}
			if ( index == null )
			{
				index = nodeManager.createPropertyIndex( key );
			}
			Event event = Event.NODE_ADD_PROPERTY;
			NodeOpData data;
			if ( property != null )
			{
				int propertyId = property.getId();
				data = new NodeOpData( this, id, propertyId, index, value );
				event = Event.NODE_CHANGE_PROPERTY;
			}
			else
			{
				data = new NodeOpData( this, id, -1, index, value );
			}

			EventData eventData = new EventData( data );
			if ( !nodeManager.generateProActiveEvent( event, eventData ) )
			{
				nodeManager.setRollbackOnly();
				throw new IllegalValueException( 
					"Generate pro-active event failed, " +
					" unable to add property[" + key + "," + value + 
					"] on " + this );
			}
			if ( event == Event.NODE_ADD_PROPERTY )
			{
				doAddProperty( index, new Property( data.getPropertyId(), 
					value ) );
			}
			else
			{
				doChangeProperty( index, new Property( data.getPropertyId(), 
					value ) );
			}
			nodeManager.generateReActiveEvent( event, eventData );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.WRITE );
		}
	}
	
	public Object removeProperty( String key )
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "Null parameter." );
		}
		nodeManager.acquireLock( this, LockType.WRITE );
		try
		{
			Property property = null;
			for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
			{
				property = null;
				if ( propertyMap != null )
				{
					property = propertyMap.remove( cachedIndex.getKeyId() );
				}
				if ( property == null )
				{
					if ( ensureFullProperties() )
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
			if ( property == null && !nodeManager.hasAllPropertyIndexes() )
			{
				ensureFullProperties();
				for ( int keyId : propertyMap.keySet() )
				{
					if ( !nodeManager.hasIndexFor( keyId ) )
					{
						PropertyIndex indexToCheck = nodeManager.getIndexFor( 
							keyId );
						if ( indexToCheck.getKey().equals( key ) )
						{
							property = propertyMap.remove( 
								indexToCheck.getKeyId() );
							break;
						}
					}
				}
			}
			if ( property == null )
			{
				return null;
			}
			NodeOpData data = new NodeOpData( this, id, property.getId() );
			EventData eventData = new EventData( data );
			if ( !nodeManager.generateProActiveEvent( 
				Event.NODE_REMOVE_PROPERTY, eventData ) )
			{
				nodeManager.setRollbackOnly();
				throw new NotFoundException( 
					"Generate pro-active event failed, " +
					"unable to remove property[" + key + "] from " + this );
			}

			nodeManager.generateReActiveEvent( Event.NODE_REMOVE_PROPERTY, 
				eventData );
			return getPropertyValue( property );
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.WRITE );
		}
	}
	
	/**
	 * If object <CODE>node</CODE> is a node, 0 is returned if <CODE>this</CODE>
	 * node id equals <CODE>node's</CODE> node id, 1 if <CODE>this</CODE> 
	 * node id is greater and -1 else.
	 * <p>
	 * If <CODE>node</CODE> isn't a node a ClassCastException will be thrown.
	 *
	 * @param node the node to compare this node with
	 * @return 0 if equal id, 1 if this id is greater else -1
	 */
	public int compareTo( Node n )
	{
		int ourId = (int) this.getId(), theirId = (int) n.getId();
		
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
	 * Returns true if object <CODE>o</CODE> is a node with the same id
	 * as <CODE>this</CODE>.
	 *
	 * @param o the object to compare
	 * @return true if equal, else false
	 */
	public boolean equals( Object o )
	{
		// verify type and not null, should use Node inteface
		if ( !(o instanceof Node) )
		{
			return false;
		}
		
		// The equals contract:
		// o reflexive: x.equals(x)
		// o symmetric: x.equals(y) == y.equals(x)
		// o transitive: ( x.equals(y) && y.equals(z) ) == true 
		//				 then x.equals(z) == true
		// o consistent: the nodeId never changes
		return this.getId() == ((Node) o).getId();
		
	}
	
	public int hashCode()
	{
		return id;
	}
	
	/**
	 * Returns this node's string representation. 
	 * 
	 * @return the string representation of this node
	 */
	public String toString()
	{
		return "Node #" + this.getId();
	}
	
	 // caller is responsible for acquiring lock
	void doAddProperty( PropertyIndex index, Property value )
		throws IllegalValueException
	{
		if ( propertyMap.get( index.getKeyId() ) != null )
		{
			throw new IllegalValueException( "Property[" + index.getKey() + 
				"] already added." );
		}
		propertyMap.put( index.getKeyId(), value );
	}

	 // caller is responsible for acquiring lock
	Property doRemoveProperty( PropertyIndex index ) throws NotFoundException
	{
		Property property = propertyMap.remove( index.getKeyId() );
		if ( property != null )
		{
			return property;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
	}

	 // caller is responsible for acquiring lock
	Property doChangeProperty( PropertyIndex index, Property newValue )
		throws IllegalValueException, NotFoundException
	{
		Property property = propertyMap.get( index.getKeyId() );
		if ( property != null )
		{
			Property oldProperty = new Property( property.getId(), 
				getPropertyValue( property ) );
			property.setNewValue( getPropertyValue( newValue ) );
			return oldProperty;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
	}
	
	private Object getPropertyValue( Property property )
	{
		Object value = property.getValue();
		if ( value == null )
		{
			value = nodeManager.loadPropertyValue( property.getId() );
			property.setNewValue( value );
		}
		return value;
	}
	
	 // caller is responsible for acquiring lock
	Property doGetProperty( PropertyIndex index ) throws NotFoundException
	{
		Property property = propertyMap.get( index.getKeyId() );
		if ( property != null )
		{
			return property;
		}
		throw new NotFoundException( "Property not found: " + index.getKey() );
	}

	 // caller is responsible for acquiring lock
	 // this method is only called when a relationship is created or 
	 // a relationship delete is undone or when the full node is loaded
	void addRelationship( RelationshipType type, int relId ) 
	{
		ArrayIntSet relationshipSet = relationshipMap.get( type.name() );
		if ( relationshipSet == null )
		{
			relationshipSet = new ArrayIntSet();
			relationshipMap.put( type.name(), relationshipSet );
		}
		relationshipSet.add( relId );
	}
	
	 // caller is responsible for acquiring lock
	 // this method is only called when a undo create relationship or
	 // a relationship delete is invoked.
	void removeRelationship( RelationshipType type, int relId )
	{
		ArrayIntSet relationshipSet = relationshipMap.get( type.name() );
		if ( relationshipSet != null )
		{
			if ( !relationshipSet.remove( relId ) )
			{
				if ( ensureFullRelationships() )
				{
					relationshipSet.remove( relId );
				}
			}
			if ( relationshipSet.size() == 0 )
			{
				relationshipMap.remove( type.name() );
			}
		}
		else
		{
			if ( ensureFullRelationships() )
			{
				removeRelationship( type, relId );
			}
		}
	}
	
	boolean internalHasRelationships()
	{
		ensureFullRelationships();
		return ( relationshipMap.size() > 0 );
	}
	
/*	private void setRollbackOnly()
	{
		try
		{
			nodeManager.setRollbackOnly();
			// TransactionFactory.getTransactionManager().setRollbackOnly();
		}
		catch ( javax.transaction.SystemException se )
		{
			se.printStackTrace();
//			log.severe( "Failed to set transaction rollback only" );
		}
	}*/

	private boolean ensureFullProperties()
	{
		if ( nodePropPhase != NodePhase.FULL_PROPERTY )
		{
			RawPropertyData[] rawProperties =
				nodeManager.loadProperties( this );
			ArrayIntSet addedProps = new ArrayIntSet();
			ArrayMap<Integer,Property> newPropertyMap = 
				new ArrayMap<Integer,Property>();
			for ( RawPropertyData propData : rawProperties )
			{
				int propId = propData.getId();
				assert addedProps.add( propId );
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
			nodePropPhase = NodePhase.FULL_PROPERTY;
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
	
	private boolean ensureFullRelationships()
	{
		if ( nodeRelPhase != NodePhase.FULL_REL )
		{
			List<Relationship> fullRelationshipList = 
				nodeManager.loadRelationships( this );
			ArrayIntSet addedRels = new ArrayIntSet();
			ArrayMap<String,ArrayIntSet> newRelationshipMap = 
				new ArrayMap<String,ArrayIntSet>(); 
			for ( Relationship rel : fullRelationshipList )
			{
				int relId = (int) rel.getId();
				addedRels.add( relId );
				RelationshipType type = rel.getType();
				ArrayIntSet relationshipSet = newRelationshipMap.get( 
					type.name() );
				if ( relationshipSet == null )
				{
					relationshipSet = new ArrayIntSet();
					newRelationshipMap.put( type.name(), relationshipSet );
				}
				relationshipSet.add( relId );
			}
			for ( String typeName : this.relationshipMap.keySet() )
			{
				ArrayIntSet relationshipSet = 
					this.relationshipMap.get( typeName );
				for ( Integer relId : relationshipSet.values() )
				{
					if ( !addedRels.contains( relId ) )
					{
						ArrayIntSet newRelationshipSet = 
							newRelationshipMap.get( typeName );
						if ( newRelationshipSet == null )
						{
							newRelationshipSet = new ArrayIntSet();
							newRelationshipMap.put( typeName, 
								newRelationshipSet );
						}
						newRelationshipSet.add( relId );
						addedRels.add( relId );
					}
				}
			}
			this.relationshipMap = newRelationshipMap;
			nodeRelPhase = NodePhase.FULL_REL;
			return true;
		}
		return false;
	}
	
/*	private void acquireLock( Object resource, LockType lockType )
	{
		try
		{
			if ( lockType == LockType.READ )
			{
				nodeManager.getRe
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
		releaseLock( resource, lockType, false );
	}

	private void releaseLock( Object resource, LockType lockType, 
		boolean forceRelease )
	{
		try
		{
			if ( lockType == LockType.READ )
			{
				lockManager.releaseReadLock( resource );
			}
			else if ( lockType == LockType.WRITE )
			{
				if ( forceRelease ) 
				{
					lockManager.releaseWriteLock( resource );
				}
				else
				{
					lockReleaser.addLockToTransaction( resource, 
						lockType );
				}
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
	}*/
	
	boolean isDeleted()
	{
		return isDeleted;
	}
	
	void setIsDeleted( boolean flag )
	{
		isDeleted = flag;
	}
	
	public Relationship createRelationshipTo( Node otherNode, 
		RelationshipType type )
	{
		return nodeManager.createRelationship( this, otherNode, type );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, 
		RelationshipType relationshipType, Direction direction )
	{
		if ( direction == null )
		{
			throw new IllegalArgumentException( "Null direction" );
		}
		if ( relationshipType == null )
		{
			throw new IllegalArgumentException( "Null relationship type" );
		}
		// rest of parameters will be validated in traverser package
		return nodeManager.createTraverser( traversalOrder, this, 
			relationshipType, direction, stopEvaluator, returnableEvaluator );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, 
		RelationshipType firstRelationshipType, Direction firstDirection, 
		RelationshipType secondRelationshipType, Direction secondDirection )
	{
		if ( firstDirection == null || secondDirection == null )
		{
			throw new IllegalArgumentException( "Null direction, " + 
				"firstDirection=" + firstDirection + "secondDirection=" +
				secondDirection );
		}
		if ( firstRelationshipType == null || secondRelationshipType == null )
		{
			throw new IllegalArgumentException( "Null rel type, " + 
				"first=" + firstRelationshipType + "second=" +
				secondRelationshipType );
		}
		// rest of parameters will be validated in traverser package
		RelationshipType[] types = new RelationshipType[2];
		Direction[] dirs = new Direction[2];
		types[0] = firstRelationshipType;
		types[1] = secondRelationshipType;
		dirs[0] = firstDirection;
		dirs[1] = secondDirection;
		return nodeManager.createTraverser( traversalOrder, this, types, dirs, 
			stopEvaluator, returnableEvaluator );
	}

	public Traverser traverse( Order traversalOrder, 
		StopEvaluator stopEvaluator, 
		ReturnableEvaluator returnableEvaluator, 
		Object... relationshipTypesAndDirections )
	{
		int length = relationshipTypesAndDirections.length;
		if ( (length % 2) != 0 || length == 0 )
		{
			throw new IllegalArgumentException( "Variable argument should " + 
				" consist of [RelationshipType,Direction] pairs" ); 
		}
		int elements = relationshipTypesAndDirections.length / 2; 
		RelationshipType[] types = new RelationshipType[ elements ];
		Direction[] dirs = new Direction[ elements ];
		int j = 0;
		for ( int i = 0; i < elements; i++ )
		{
			Object relType = relationshipTypesAndDirections[j++];
			if ( !(relType instanceof RelationshipType) )
			{
				throw new IllegalArgumentException( 
					"Expected RelationshipType at var args pos " + (j - 1) + 
					", found " + relType );
			}
			types[i] = ( RelationshipType ) relType;
			Object direction = relationshipTypesAndDirections[j++];
			if ( !(direction instanceof Direction) )
			{
				throw new IllegalArgumentException( 
					"Expected Direction at var args pos " + (j - 1) + 
					", found " + direction );
			}
			dirs[i] = ( Direction ) direction;
		}
		return nodeManager.createTraverser( traversalOrder, this, types, dirs, 
			stopEvaluator, returnableEvaluator );
	}

	public boolean hasRelationship()
    {
		return getRelationships().iterator().hasNext();
    }

	public boolean hasRelationship( RelationshipType... types )
    {
		return getRelationships( types ).iterator().hasNext();
    }

	public boolean hasRelationship( Direction dir )
    {
		return getRelationships( dir ).iterator().hasNext();
    }

	public boolean hasRelationship( RelationshipType type, Direction dir )
    {
		return getRelationships( type, dir ).iterator().hasNext();
    }
}