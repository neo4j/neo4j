/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;

class NodeProxy implements Node
{
    private final NodeManager nm;

    private final int nodeId;

    NodeProxy( int nodeId, NodeManager nodeManager )
    {
        this.nodeId = nodeId;
        this.nm = nodeManager;
    }

    public long getId()
    {
        return nodeId;
    }

    public void delete()
    {
        nm.getNodeForProxy( nodeId ).delete();
    }

    public Iterable<Relationship> getRelationships()
    {
        return nm.getNodeForProxy( nodeId ).getRelationships();
    }

    public boolean hasRelationship()
    {
        return nm.getNodeForProxy( nodeId ).hasRelationship();
    }

    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return nm.getNodeForProxy( nodeId ).getRelationships( dir );
    }

    public boolean hasRelationship( Direction dir )
    {
        return nm.getNodeForProxy( nodeId ).hasRelationship( dir );
    }

    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return nm.getNodeForProxy( nodeId ).getRelationships( types );
    }

    public boolean hasRelationship( RelationshipType... types )
    {
        return nm.getNodeForProxy( nodeId ).hasRelationship( types );
    }

    public Iterable<Relationship> getRelationships( RelationshipType type,
        Direction dir )
    {
        return nm.getNodeForProxy( nodeId ).getRelationships( type, dir );
    }

    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return nm.getNodeForProxy( nodeId ).hasRelationship( type, dir );
    }

    public Relationship getSingleRelationship( RelationshipType type,
        Direction dir )
    {
        return nm.getNodeForProxy( nodeId ).getSingleRelationship( type, dir );
    }

    public void setProperty( String key, Object value )
    {
        nm.getNodeForProxy( nodeId ).setProperty( key, value );
    }

    public Object removeProperty( String key ) throws NotFoundException
    {
        return nm.getNodeForProxy( nodeId ).removeProperty( key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return nm.getNodeForProxy( nodeId ).getProperty( key, defaultValue );
    }

    public Iterable<Object> getPropertyValues()
    {
        return nm.getNodeForProxy( nodeId ).getPropertyValues();
    }

    public Iterable<String> getPropertyKeys()
    {
        return nm.getNodeForProxy( nodeId ).getPropertyKeys();
    }

    public Object getProperty( String key ) throws NotFoundException
    {
        return nm.getNodeForProxy( nodeId ).getProperty( key );
    }

    public boolean hasProperty( String key )
    {
        return nm.getNodeForProxy( nodeId ).hasProperty( key );
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

    public boolean equals( Object o )
    {
        if ( !(o instanceof Node) )
        {
            return false;
        }
        return this.getId() == ((Node) o).getId();
    }

    public int hashCode()
    {
        return nodeId;
    }

    public String toString()
    {
        return "Node[" + this.getId() + "]";
    }

    public Relationship createRelationshipTo( Node otherNode,
        RelationshipType type )
    {
        return nm.getNodeForProxy( nodeId ).createRelationshipTo( otherNode,
            type );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType relationshipType, Direction direction )
    {
        return nm.getNodeForProxy( nodeId ).traverse( traversalOrder,
            stopEvaluator, returnableEvaluator, relationshipType, direction );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        RelationshipType firstRelationshipType, Direction firstDirection,
        RelationshipType secondRelationshipType, Direction secondDirection )
    {
        return nm.getNodeForProxy( nodeId ).traverse( traversalOrder,
            stopEvaluator, returnableEvaluator, firstRelationshipType,
            firstDirection, secondRelationshipType, secondDirection );
    }

    public Traverser traverse( Order traversalOrder,
        StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator,
        Object... relationshipTypesAndDirections )
    {
        return nm.getNodeForProxy( nodeId ).traverse( traversalOrder,
            stopEvaluator, returnableEvaluator, relationshipTypesAndDirections );
    }
}
