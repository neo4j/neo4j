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
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;


class NodeImpl extends NeoPrimitive implements Node, Comparable<Node>
{
	private static enum RelPhase 
    { 
		EMPTY_REL,
		FULL_REL 
    }
	
	private final int id;
	private boolean isDeleted = false;
	private RelPhase relPhase;

    private ArrayMap<String,ArrayIntSet> relationshipMap = null; 
    private ArrayMap<String,ArrayIntSet> cowRelationshipMap = null;
    
	NodeImpl( int id, NodeManager nodeManager )
	{
        super( nodeManager );
		this.id = id;
		this.relPhase = RelPhase.EMPTY_REL;
	}
	
	// newNode will only be true for NodeManager.createNode 
	NodeImpl( int id, boolean newNode, NodeManager nodeManager )
	{
        super( newNode, nodeManager );
		this.id = id;
		if ( newNode )
		{
			this.relPhase = RelPhase.FULL_REL;
		}
	}
		
    protected void changeProperty( int propertyId, Object value )
    {
        nodeManager.nodeChangeProperty( this, propertyId, value );
    }
    
    protected int addProperty( PropertyIndex index, Object value )
    {
        return nodeManager.nodeAddProperty( this, index, value );
    }
    
    protected void removeProperty( int propertyId )
    {
        nodeManager.nodeRemoveProperty( this, propertyId );
    }
    
    protected RawPropertyData[] loadProperties()
    {
        return nodeManager.loadProperties( this );
    }

    public long getId()
	{
		return this.id;
	}
	
	public Iterable<Relationship> getRelationships()
	{
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        Iterator<ArrayIntSet> values = mapToCheck.values().iterator();
        int size = 0;
        while ( values.hasNext() )
        {
            ArrayIntSet relTypeSet = values.next();
            size += relTypeSet.size();
        }
        values = mapToCheck.values().iterator();
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
/*		nodeManager.acquireLock( this, LockType.READ );
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
		}*/
	}
	
	public Iterable<Relationship> getRelationships( Direction dir )
	{
		if ( dir == Direction.BOTH )
		{
			return getRelationships();
		}
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        Iterator<ArrayIntSet> values = mapToCheck.values().iterator();
        int size = 0;
        while ( values.hasNext() )
        {
            ArrayIntSet relTypeSet = values.next();
            size += relTypeSet.size();
        }
        values = mapToCheck.values().iterator();
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
		/*nodeManager.acquireLock( this, LockType.READ );
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
		}*/
	}
	
	public Iterable<Relationship> getRelationships( RelationshipType type )
	{
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        ArrayIntSet relationshipSet = mapToCheck.get( type.name() );
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
/*		nodeManager.acquireLock( this, LockType.READ );
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
		}*/
	}

	public Iterable<Relationship> getRelationships( RelationshipType... types )
	{
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        int size = 0;
        for ( RelationshipType type : types )
        {
            ArrayIntSet relTypeSet = mapToCheck.get( type.name() );
            if ( relTypeSet != null )
            {
                size += relTypeSet.size();
            }
        }
        int[] ids = new int[size];
        int position = 0;
        for ( RelationshipType type : types )
        {
            ArrayIntSet relTypeSet = mapToCheck.get( type.name() );
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
/*		nodeManager.acquireLock( this, LockType.READ );
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
		}*/
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
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        ArrayIntSet relationshipSet = mapToCheck.get( type.name() );
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
        
//        nodeManager.acquireLock( this, LockType.READ );
//      try
//      {
/*            ensureFullRelationships();
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
			return new RelationshipIterator( ids, this, dir, nodeManager );*/
//		}
//		finally
//		{
//			nodeManager.releaseLock( this, LockType.READ );
//		}
	}
	

	public void delete() // throws DeleteException
	{
		nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
		try
		{
            nodeManager.deleteNode( this );
            success = true;
		}
		finally
		{
			nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
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
	 // this method is only called when a relationship is created or 
	 // a relationship delete is undone or when the full node is loaded
	void addRelationship( RelationshipType type, int relId ) 
	{
        if ( cowRelationshipMap != null )
        {
            // write operation, this must be true;
            assert cowTxId == nodeManager.getTransactionId();
        }
        else
        {
            ensureFullRelationships();
            createRelationshipCowMap();
        }
        ArrayIntSet relationshipSet = cowRelationshipMap.get( type.name() );
        if ( relationshipSet == null )
        {
            relationshipSet = new ArrayIntSet();
            cowRelationshipMap.put( type.name(), relationshipSet );
        }
        relationshipSet.add( relId );
        /*        if ( relationshipMap == null )
        {
            relationshipMap = new ArrayMap<String,ArrayIntSet>(); 
        }
		ArrayIntSet relationshipSet = relationshipMap.get( type.name() );
		if ( relationshipSet == null )
		{
			relationshipSet = new ArrayIntSet();
			relationshipMap.put( type.name(), relationshipSet );
		}
		relationshipSet.add( relId );
        */
	}
	
	 // caller is responsible for acquiring lock
	 // this method is only called when a undo create relationship or
	 // a relationship delete is invoked.
	void removeRelationship( RelationshipType type, int relId )
	{
        if ( cowRelationshipMap != null )
        {
            // write operation, this must be true;
            assert cowTxId == nodeManager.getTransactionId();
        }
        else
        {
            ensureFullRelationships();
            createRelationshipCowMap();
        }
        ArrayIntSet relationshipSet = cowRelationshipMap.get( type.name() );
        if ( relationshipSet != null )
        {
            relationshipSet.remove( relId );
            if ( relationshipSet.size() == 0 )
            {
                cowRelationshipMap.remove( type.name() );
            }
        }
/*		ArrayIntSet relationshipSet = null; 
        if ( relationshipMap != null )
        {
           relationshipSet = relationshipMap.get( type.name() );
        }
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
		}*/
	}
	
	boolean internalHasRelationships()
	{
        ArrayMap<String,ArrayIntSet> mapToCheck = null;
        if ( cowRelationshipMap != null && 
            cowTxId == nodeManager.getTransactionId() )
        {
            mapToCheck = cowRelationshipMap;
        }
        else
        {
            ensureFullRelationships();
            mapToCheck = relationshipMap;
        }
        if ( mapToCheck != null )
        {
            return ( mapToCheck.size() > 0 );
        }
        return false;
	}
	
	
	private boolean ensureFullRelationships()
	{
		if ( relPhase != RelPhase.FULL_REL )
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
            if ( relationshipMap != null )
            {
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
            }
			this.relationshipMap = newRelationshipMap;
			relPhase = RelPhase.FULL_REL;
			return true;
		}
        if ( relationshipMap == null )
        {
            relationshipMap = new ArrayMap<String,ArrayIntSet>(); 
        }
		return false;
	}
	
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
    
    @Override
    protected void commitCowMaps()
    {
        super.commitCowMaps();
        if ( cowRelationshipMap != null )
        {
            relationshipMap = cowRelationshipMap;
            cowRelationshipMap = null;
        }
    }

    @Override
    protected void rollbackCowMaps()
    {
        super.rollbackCowMaps();
        cowRelationshipMap = null;
    }
    
    private void createRelationshipCowMap()
    {
        assert cowRelationshipMap == null;
        int currentTxId = nodeManager.getTransactionId();
        if ( cowTxId == -1 )
        {
            cowTxId = currentTxId;
            nodeManager.addCowToTxHook( this );
        }
        else
        {
            assert cowTxId == currentTxId;
        }
        cowRelationshipMap = new ArrayMap<String,ArrayIntSet>();
        if ( relationshipMap == null )
        {
            return; 
        }
        for ( String type : this.relationshipMap.keySet() )
        {
            ArrayIntSet rels = relationshipMap.get( type );
            ArrayIntSet copyRels = new ArrayIntSet();
            for( int rel : rels.values() )
            {
                copyRels.add( rel );
            }
            cowRelationshipMap.put( type, copyRels );
        }
    }
}