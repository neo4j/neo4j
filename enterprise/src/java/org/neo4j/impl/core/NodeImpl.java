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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayMap;
import org.neo4j.impl.util.IntArray;

class NodeImpl extends NeoPrimitive implements Node, Comparable<Node>
{
    private ArrayMap<String,IntArray> relationshipMap = null;

    NodeImpl( int id, NodeManager nodeManager )
    {
        super( id, nodeManager );
    }

    // newNode will only be true for NodeManager.createNode
    NodeImpl( int id, boolean newNode, NodeManager nodeManager )
    {
        super( id, newNode, nodeManager );
        if ( newNode )
        {
            relationshipMap = new ArrayMap<String,IntArray>();
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

    protected ArrayMap<Integer,PropertyData> loadProperties()
    {
        return nodeManager.loadProperties( this );
    }
    
    private List<RelTypeElementIterator> getAllRelationships()
    {
        ensureFullRelationships();
        List<RelTypeElementIterator> relTypeList = 
            new LinkedList<RelTypeElementIterator>();
        ArrayMap<String,IntArray> addMap = 
            nodeManager.getCowRelationshipAddMap( this );
        for ( String type : relationshipMap.keySet() )
        {
            IntArray src = relationshipMap.get( type );
            IntArray remove = nodeManager.getCowRelationshipRemoveMap(
                this, type );
            IntArray add = null;
            if ( addMap != null )
            {
                add = addMap.get( type );
            }
            relTypeList.add( RelTypeElement.create( src, add, remove ) );
        }
        if ( addMap != null )
        {
            for ( String type : addMap.keySet() )
            {
                if ( relationshipMap.get( type ) == null )
                {
                    IntArray remove = nodeManager.getCowRelationshipRemoveMap(
                        this, type );
                    IntArray add = addMap.get( type );
                    relTypeList.add( RelTypeElement.create( null, add, 
                        remove ) );
                }
            }
        }
        return relTypeList;
    }
    
    private List<RelTypeElementIterator> getAllRelationshipsOfType( 
        RelationshipType... types)
    {
        ensureFullRelationships();
        List<RelTypeElementIterator> relTypeList = 
            new LinkedList<RelTypeElementIterator>();
        for ( RelationshipType type : types )
        {
            IntArray src = relationshipMap.get( type.name() );
            IntArray remove = nodeManager.getCowRelationshipRemoveMap(
                this, type.name() );
            IntArray add = nodeManager.getCowRelationshipAddMap( this, 
                type.name() );
            relTypeList.add( RelTypeElement.create( src, add, remove ) );
        }
        return relTypeList;
    }
    
    public Iterable<Relationship> getRelationships()
    {
        return new IntArrayIterator( getAllRelationships(), this,
            Direction.BOTH, nodeManager );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return new IntArrayIterator( getAllRelationships(), this, dir,
            nodeManager );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type )
    {
        return new IntArrayIterator( 
            getAllRelationshipsOfType( new RelationshipType[] { type } ), 
            this, Direction.BOTH, nodeManager );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return new IntArrayIterator( 
            getAllRelationshipsOfType(types ), 
            this, Direction.BOTH, nodeManager );
    }

    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        Iterator<Relationship> rels = new IntArrayIterator( 
            getAllRelationshipsOfType( new RelationshipType[] { type } ), 
            this, dir, nodeManager );
        if ( !rels.hasNext() )
        {
            return null;
        }
        Relationship rel = rels.next();
        if ( rels.hasNext() )
        {
            throw new NotFoundException( "More than one relationship[" +
                type + ", " + dir + "] found for " + this );
        }
        return rel;
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        return new IntArrayIterator( 
            getAllRelationshipsOfType( new RelationshipType[] { type } ), 
            this, dir, nodeManager );
    }
    
    public void delete()
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
     * @param node
     *            the node to compare this node with
     * @return 0 if equal id, 1 if this id is greater else -1
     */
    public int compareTo( Node n )
    {
        long ourId = this.getId(), theirId = n.getId();

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
     * Returns true if object <CODE>o</CODE> is a node with the same id as
     * <CODE>this</CODE>.
     * 
     * @param o
     *            the object to compare
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
        // then x.equals(z) == true
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
        return "NodeImpl#" + this.getId();
    }

    // caller is responsible for acquiring lock
    // this method is only called when a relationship is created or
    // a relationship delete is undone or when the full node is loaded
    void addRelationship( RelationshipType type, int relId )
    {
        IntArray relationshipSet = nodeManager.getCowRelationshipAddMap(
            this, type.name(), true );
        relationshipSet.add( relId );
    }

    // caller is responsible for acquiring lock
    // this method is only called when a undo create relationship or
    // a relationship delete is invoked.
    void removeRelationship( RelationshipType type, int relId )
    {
        IntArray relationshipSet = nodeManager.getCowRelationshipRemoveMap(
            this, type.name(), true );
        relationshipSet.add( relId );
    }

    private boolean ensureFullRelationships()
    {
        if ( relationshipMap == null )
        {
            this.relationshipMap = nodeManager.loadRelationships( this );
            return true;
        }
        return false;
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
            throw new IllegalArgumentException( "Null direction, "
                + "firstDirection=" + firstDirection + "secondDirection="
                + secondDirection );
        }
        if ( firstRelationshipType == null || secondRelationshipType == null )
        {
            throw new IllegalArgumentException( "Null rel type, " + "first="
                + firstRelationshipType + "second=" + secondRelationshipType );
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
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        Object... relationshipTypesAndDirections )
    {
        int length = relationshipTypesAndDirections.length;
        if ( (length % 2) != 0 || length == 0 )
        {
            throw new IllegalArgumentException( "Variable argument should "
                + " consist of [RelationshipType,Direction] pairs" );
        }
        int elements = relationshipTypesAndDirections.length / 2;
        RelationshipType[] types = new RelationshipType[elements];
        Direction[] dirs = new Direction[elements];
        int j = 0;
        for ( int i = 0; i < elements; i++ )
        {
            Object relType = relationshipTypesAndDirections[j++];
            if ( !(relType instanceof RelationshipType) )
            {
                throw new IllegalArgumentException(
                    "Expected RelationshipType at var args pos " + (j - 1)
                        + ", found " + relType );
            }
            types[i] = (RelationshipType) relType;
            Object direction = relationshipTypesAndDirections[j++];
            if ( !(direction instanceof Direction) )
            {
                throw new IllegalArgumentException(
                    "Expected Direction at var args pos " + (j - 1)
                        + ", found " + direction );
            }
            dirs[i] = (Direction) direction;
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

    protected void commitRelationshipMaps( 
        ArrayMap<String,IntArray> cowRelationshipAddMap, 
        ArrayMap<String,IntArray> cowRelationshipRemoveMap )
    {
        if ( relationshipMap == null )
        {
            // we will load full in some other tx
            return;
        }
        if ( cowRelationshipAddMap != null )
        {
            for ( String type : cowRelationshipAddMap.keySet() )
            {
                IntArray add = cowRelationshipAddMap.get( type );
                IntArray remove = null; 
                if ( cowRelationshipRemoveMap != null ) 
                {
                    remove = cowRelationshipRemoveMap.get( type );
                }
                IntArray src = relationshipMap.get( type );
                relationshipMap.put( type, IntArray.composeNew( 
                    src, add, remove ) );
            }
        }
        if ( cowRelationshipRemoveMap != null )
        {
            for ( String type : cowRelationshipRemoveMap.keySet() )
            {
                if ( cowRelationshipAddMap != null && 
                    cowRelationshipAddMap.get( type ) != null ) 
                {
                    continue;
                }
                IntArray src = relationshipMap.get( type );
                IntArray remove = cowRelationshipRemoveMap.get( type ); 
                relationshipMap.put( type, IntArray.composeNew( src, null, 
                     remove ) );
            }
        }
    }
}