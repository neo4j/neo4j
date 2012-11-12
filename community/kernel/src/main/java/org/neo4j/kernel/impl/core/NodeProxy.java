/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.kernel.impl.transaction.LockType;

class NodeProxy implements Node
{
    private final NodeManager nm;

    private final long nodeId;

    NodeProxy( long nodeId, NodeManager nodeManager )
    {
        this.nodeId = nodeId;
        this.nm = nodeManager;
    }

    public long getId()
    {
        return nodeId;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return nm.getGraphDbService();
    }

    public void delete()
    {
        nm.getNodeForProxy( this, LockType.WRITE ).delete( nm, this );
    }

    public Iterable<Relationship> getRelationships()
    {
        return nm.getNodeForProxy( this, null ).getRelationships( nm );
    }

    public boolean hasRelationship()
    {
        return nm.getNodeForProxy( this, null ).hasRelationship( nm );
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return nm.getNodeForProxy( this, null ).getRelationships( nm, dir );
    }

    public boolean hasRelationship( Direction dir )
    {
        return nm.getNodeForProxy( this, null ).hasRelationship( nm, dir );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return nm.getNodeForProxy( this, null ).getRelationships( nm, types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        return nm.getNodeForProxy( this, null ).getRelationships( nm, direction, types );
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        return nm.getNodeForProxy( this, null ).hasRelationship( nm, types );
    }

    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        return nm.getNodeForProxy( this, null ).hasRelationship( nm, direction, types );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        return nm.getNodeForProxy( this, null ).getRelationships( nm, type, dir );
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return nm.getNodeForProxy( this, null ).hasRelationship( nm, type, dir );
    }

    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        return nm.getNodeForProxy( this, null ).getSingleRelationship( nm, type, dir );
    }

    public void setProperty( String key, Object value )
    {
        nm.getNodeForProxy( this, LockType.WRITE ).setProperty( nm, this, key, value );
    }

    public Object removeProperty( String key ) throws NotFoundException
    {
        return nm.getNodeForProxy( this, LockType.WRITE ).removeProperty( nm, this, key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return nm.getNodeForProxy( this, null ).getProperty( nm, key, defaultValue );
    }

    public Iterable<Object> getPropertyValues()
    {
        return nm.getNodeForProxy( this, null ).getPropertyValues( nm );
    }

    public Iterable<String> getPropertyKeys()
    {
        return nm.getNodeForProxy( this, null ).getPropertyKeys( nm );
    }

    public Object getProperty( String key ) throws NotFoundException
    {
        return nm.getNodeForProxy( this, null ).getProperty( nm, key );
    }

    public boolean hasProperty( String key )
    {
        return nm.getNodeForProxy( this, null ).hasProperty( nm, key );
    }

    public int compareTo( Object node )
    {
        Node n = (Node) node;
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

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof Node) )
        {
            return false;
        }
        return this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) (( nodeId >>> 32 ) ^ nodeId );
    }

    @Override
    public String toString()
    {
        return "Node[" + this.getId() + "]";
    }

    public Relationship createRelationshipTo( Node otherNode,
        RelationshipType type )
    {
        return nm.getNodeForProxy( this, LockType.WRITE ).createRelationshipTo( nm, this, otherNode, type );
    }

    /* Tentative expansion API
    public Expansion<Relationship> expandAll()
    {
        return nm.getNodeForProxy( nodeId ).expandAll();
    }

    public Expansion<Relationship> expand( RelationshipType type )
    {
        return nm.getNodeForProxy( nodeId ).expand( type );
    }

    public Expansion<Relationship> expand( RelationshipType type,
            Direction direction )
    {
        return nm.getNodeForProxy( nodeId ).expand( type, direction );
    }

    public Expansion<Relationship> expand( Direction direction )
    {
        return nm.getNodeForProxy( nodeId ).expand( direction );
    }

    public Expansion<Relationship> expand( RelationshipExpander expander )
    {
        return nm.getNodeForProxy( nodeId ).expand( expander );
    }
    */

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType relationshipType, Direction direction )
    {
        return nm.getNodeForProxy( this, null ).traverse( nm, traversalOrder,
            stopEvaluator, returnableEvaluator, relationshipType, direction );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType firstRelationshipType, Direction firstDirection,
        RelationshipType secondRelationshipType, Direction secondDirection )
    {
        return nm.getNodeForProxy( this, null ).traverse( nm, traversalOrder,
            stopEvaluator, returnableEvaluator, firstRelationshipType,
            firstDirection, secondRelationshipType, secondDirection );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        Object... relationshipTypesAndDirections )
    {
        return nm.getNodeForProxy( this, null ).traverse( nm, traversalOrder,
            stopEvaluator, returnableEvaluator, relationshipTypesAndDirections );
    }
}
