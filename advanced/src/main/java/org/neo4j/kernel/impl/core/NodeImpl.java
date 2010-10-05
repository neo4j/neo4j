/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.kernel.impl.core;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipChainPosition;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.IntArray;

class NodeImpl extends Primitive
{
    private volatile ArrayMap<String,IntArray> relationshipMap = null;
    // private RelationshipGrabber relationshipGrabber = null;
    private RelationshipChainPosition relChainPosition = null;

    NodeImpl( int id )
    {
        super( id );
    }

    // newNode will only be true for NodeManager.createNode
    NodeImpl( int id, boolean newNode )
    {
        super( id, newNode );
        if ( newNode )
        {
            relationshipMap = new ArrayMap<String,IntArray>();
            relChainPosition = new RelationshipChainPosition(
                Record.NO_NEXT_RELATIONSHIP.intValue() );
        }
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj || ( obj instanceof NodeImpl && ( (NodeImpl) obj ).id == id );
    }

    @Override
    protected void changeProperty( NodeManager nodeManager, int propertyId, Object value )
    {
        nodeManager.nodeChangeProperty( this, propertyId, value );
    }

    @Override
    protected int addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
    {
        return nodeManager.nodeAddProperty( this, index, value );
    }

    @Override
    protected void removeProperty( NodeManager nodeManager, int propertyId )
    {
        nodeManager.nodeRemoveProperty( this, propertyId );
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager, boolean light )
    {
        return nodeManager.loadProperties( this, light );
    }

    List<RelTypeElementIterator> getAllRelationships( NodeManager nodeManager )
    {
        ensureRelationshipMapNotNull( nodeManager );
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
            if ( src != null || add != null )
            {
                relTypeList.add( RelTypeElement.create( type, this, src, add, remove ) );
            }
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
                    relTypeList.add( RelTypeElement.create( type, this, null, add, remove ) );
                }
            }
        }
        return relTypeList;
    }

    List<RelTypeElementIterator> getAllRelationshipsOfType( NodeManager nodeManager,
        RelationshipType... types)
    {
        ensureRelationshipMapNotNull( nodeManager );
        List<RelTypeElementIterator> relTypeList =
            new LinkedList<RelTypeElementIterator>();
        for ( RelationshipType type : types )
        {
            IntArray src = relationshipMap.get( type.name() );
            IntArray remove = nodeManager.getCowRelationshipRemoveMap(
                this, type.name() );
            IntArray add = nodeManager.getCowRelationshipAddMap( this,
                type.name() );
            if ( src != null || add != null )
            {
                relTypeList.add( RelTypeElement.create( type.name(), this, src, add, remove ) );
            }
        }
        return relTypeList;
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager )
    {
        return new IntArrayIterator( getAllRelationships( nodeManager ), this,
            Direction.BOTH, nodeManager, new RelationshipType[0] );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, Direction dir )
    {
        return new IntArrayIterator( getAllRelationships( nodeManager ), this, dir,
            nodeManager, new RelationshipType[0] );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, RelationshipType type )
    {
        RelationshipType types[] = new RelationshipType[] { type };
        return new IntArrayIterator( getAllRelationshipsOfType( nodeManager, types ),
            this, Direction.BOTH, nodeManager, types );
    }

    public Iterable<Relationship> getRelationships( NodeManager nodeManager,
            RelationshipType... types )
    {
        return new IntArrayIterator( getAllRelationshipsOfType( nodeManager, types ),
            this, Direction.BOTH, nodeManager, types );
    }

    public Relationship getSingleRelationship( NodeManager nodeManager, RelationshipType type,
        Direction dir )
    {
        RelationshipType types[] = new RelationshipType[] { type };
        Iterator<Relationship> rels = new IntArrayIterator( getAllRelationshipsOfType( nodeManager,
                types ),
            this, dir, nodeManager, types );
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

    public Iterable<Relationship> getRelationships( NodeManager nodeManager, RelationshipType type,
        Direction dir )
    {
        RelationshipType types[] = new RelationshipType[] { type };
        return new IntArrayIterator( getAllRelationshipsOfType( nodeManager, types ),
            this, dir, nodeManager, types );
    }

    public void delete( NodeManager nodeManager )
    {
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            ArrayMap<Integer,PropertyData> skipMap =
                nodeManager.getCowPropertyRemoveMap( this, true );
            ArrayMap<Integer,PropertyData> removedProps =
                nodeManager.deleteNode( this );
            if ( removedProps.size() > 0 )
            {
                for ( int index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            success = true;
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    /**
     * Returns this node's string representation.
     *
     * @return the string representation of this node
     */
    @Override
    public String toString()
    {
        return "NodeImpl#" + this.getId();
    }

    // caller is responsible for acquiring lock
    // this method is only called when a relationship is created or
    // a relationship delete is undone or when the full node is loaded
    void addRelationship( NodeManager nodeManager, RelationshipType type, int relId )
    {
        IntArray relationshipSet = nodeManager.getCowRelationshipAddMap(
            this, type.name(), true );
        relationshipSet.add( relId );
    }

    // caller is responsible for acquiring lock
    // this method is only called when a undo create relationship or
    // a relationship delete is invoked.
    void removeRelationship( NodeManager nodeManager, RelationshipType type, int relId )
    {
        IntArray relationshipSet = nodeManager.getCowRelationshipRemoveMap(
            this, type.name(), true );
        relationshipSet.add( relId );
    }

    private void ensureRelationshipMapNotNull( NodeManager nodeManager )
    {
        if ( relationshipMap == null )
        {
            loadInitialRelationships( nodeManager );
        }
    }

    private void loadInitialRelationships( NodeManager nodeManager )
    {
        Map<Integer,RelationshipImpl> map = null;
        synchronized ( this )
        {
            if ( relationshipMap == null )
            {
                this.relChainPosition =
                    nodeManager.getRelationshipChainPosition( this );
                ArrayMap<String,IntArray> tmpRelMap = new ArrayMap<String,IntArray>();
                map = getMoreRelationships( nodeManager, tmpRelMap );
                this.relationshipMap = tmpRelMap;
            }
        }
        if ( map != null )
        {
            nodeManager.putAllInRelCache( map );
        }
    }

    private Map<Integer,RelationshipImpl> getMoreRelationships( NodeManager nodeManager, 
            ArrayMap<String,IntArray> tmpRelMap )
    {
        if ( !relChainPosition.hasMore() )
        {
            return null;
        }
        Pair<ArrayMap<String,IntArray>,Map<Integer,RelationshipImpl>> pair = 
            nodeManager.getMoreRelationships( this );
        ArrayMap<String,IntArray> addMap = pair.first();
        if ( addMap.size() == 0 )
        {
            return null;
        }
        for ( String type : addMap.keySet() )
        {
            IntArray addRels = addMap.get( type );
            IntArray srcRels = tmpRelMap.get( type );
            if ( srcRels == null )
            {
                tmpRelMap.put( type, addRels );
            }
            else
            {
                srcRels.addAll( addRels );
            }
        }
        return pair.other();
        // nodeManager.putAllInRelCache( pair.other() );
    }
    
    boolean getMoreRelationships( NodeManager nodeManager )
    {
        // ArrayMap<String, IntArray> tmpRelMap = relationshipMap;
        Pair<ArrayMap<String,IntArray>,Map<Integer,RelationshipImpl>> pair;
        synchronized ( this )
        {
            if ( !relChainPosition.hasMore() )
            {
                return false;
            }
            
            pair = nodeManager.getMoreRelationships( this );
            ArrayMap<String,IntArray> addMap = pair.first();
            if ( addMap.size() == 0 )
            {
                return false;
            }
            for ( String type : addMap.keySet() )
            {
                IntArray addRels = addMap.get( type );
                // IntArray srcRels = tmpRelMap.get( type );
                IntArray srcRels = relationshipMap.get( type );
                if ( srcRels == null )
                {
                    relationshipMap.put( type, addRels );
                }
                else
                {
                    srcRels.addAll( addRels );
                }
            }
        }
        nodeManager.putAllInRelCache( pair.other() );
        return true;
    }
        

    public Relationship createRelationshipTo( NodeManager nodeManager, Node otherNode,
        RelationshipType type )
    {
        return nodeManager.createRelationship( this, otherNode, type );
    }

    /* Tentative expansion API
    public Expansion<Relationship> expandAll()
    {
        return Traversal.expanderForAllTypes().expand( this );
    }

    public Expansion<Relationship> expand( RelationshipType type )
    {
        return expand( type, Direction.BOTH );
    }

    public Expansion<Relationship> expand( RelationshipType type,
            Direction direction )
    {
        return Traversal.expanderForTypes( type, direction ).expand(
                this );
    }

    public Expansion<Relationship> expand( Direction direction )
    {
        return Traversal.expanderForAllTypes( direction ).expand( this );
    }

    public Expansion<Relationship> expand( RelationshipExpander expander )
    {
        return Traversal.expander( expander ).expand( this );
    }
    */

    public Traverser traverse( NodeManager nodeManager, Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType relationshipType, Direction direction )
    {
        return OldTraverserWrapper.traverse( new NodeProxy( id, nodeManager ),
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipType, direction );
    }

    public Traverser traverse( NodeManager nodeManager, Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType firstRelationshipType, Direction firstDirection,
        RelationshipType secondRelationshipType, Direction secondDirection )
    {
        return OldTraverserWrapper.traverse( new NodeProxy( id, nodeManager ),
                traversalOrder, stopEvaluator,
                returnableEvaluator, firstRelationshipType, firstDirection,
                secondRelationshipType, secondDirection );
    }

    public Traverser traverse( NodeManager nodeManager, Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        Object... relationshipTypesAndDirections )
    {
        return OldTraverserWrapper.traverse( new NodeProxy( id, nodeManager ),
                traversalOrder, stopEvaluator,
                returnableEvaluator, relationshipTypesAndDirections );
    }

    public boolean hasRelationship( NodeManager nodeManager )
    {
        return getRelationships( nodeManager ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, RelationshipType... types )
    {
        return getRelationships( nodeManager, types ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, Direction dir )
    {
        return getRelationships( nodeManager, dir ).iterator().hasNext();
    }

    public boolean hasRelationship( NodeManager nodeManager, RelationshipType type, Direction dir )
    {
        return getRelationships( nodeManager, type, dir ).iterator().hasNext();
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

    RelationshipChainPosition getRelChainPosition()
    {
        return relChainPosition;
    }
}
