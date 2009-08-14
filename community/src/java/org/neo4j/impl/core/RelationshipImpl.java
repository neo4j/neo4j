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

import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayMap;

class RelationshipImpl extends NeoPrimitive implements Relationship,
    Comparable<Relationship>
{
    private final int startNodeId;
    private final int endNodeId;
    private final RelationshipType type;

    // Dummy constructor for NodeManager to acquire read lock on relationship
    // when loading from PL.
    RelationshipImpl( int id, NodeManager nodeManager )
    {
        super( id, nodeManager );
        this.startNodeId = -1;
        this.endNodeId = -1;
        this.type = null;
    }

    RelationshipImpl( int id, int startNodeId, int endNodeId,
        RelationshipType type, boolean newRel, NodeManager nodeManager )
    {
        super( id, newRel, nodeManager );
        if ( type == null )
        {
            throw new IllegalArgumentException( "Null type" );
        }
        if ( startNodeId == endNodeId )
        {
            throw new IllegalArgumentException( "Start node equals end node" );
        }

        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.type = type;
    }

    protected void changeProperty( int propertyId, Object value )
    {
        nodeManager.relChangeProperty( this, propertyId, value );
    }

    protected int addProperty( PropertyIndex index, Object value )
    {
        return nodeManager.relAddProperty( this, index, value );
    }

    protected void removeProperty( int propertyId )
    {
        nodeManager.relRemoveProperty( this, propertyId );
    }

    protected ArrayMap<Integer,PropertyData> loadProperties()
    {
        return nodeManager.loadProperties( this );
    }

    public Node[] getNodes()
    {
        try
        {
            return new Node[] { new NodeProxy( startNodeId, nodeManager ),
                new NodeProxy( endNodeId, nodeManager ) };
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
            return new NodeProxy( endNodeId, nodeManager );
        }
        if ( endNodeId == (int) node.getId() )
        {
            return new NodeProxy( startNodeId, nodeManager );
        }
        throw new RuntimeException( "Node[" + node.getId()
            + "] not connected to this relationship[" + getId() + "]" );
    }

    public Node getStartNode()
    {
        return new NodeProxy( startNodeId, nodeManager );
    }

    int getStartNodeId()
    {
        return startNodeId;
    }

    public Node getEndNode()
    {
        return new NodeProxy( endNodeId, nodeManager );
    }

    int getEndNodeId()
    {
        return endNodeId;
    }

    public RelationshipType getType()
    {
        return type;
    }

    public boolean isType( RelationshipType otherType )
    {
        return otherType != null
            && otherType.name().equals( this.getType().name() );
    }

    public void delete()
    {
        NodeImpl startNode = null;
        NodeImpl endNode = null;
        boolean startNodeLocked = false;
        boolean endNodeLocked = false;
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            startNode = nodeManager.getLightNode( startNodeId );
            if ( startNode != null )
            {
                nodeManager.acquireLock( startNode, LockType.WRITE );
                startNodeLocked = true;
            }
            endNode = nodeManager.getLightNode( endNodeId );
            if ( endNode != null )
            {
                nodeManager.acquireLock( endNode, LockType.WRITE );
                endNodeLocked = true;
            }
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            nodeManager.deleteRelationship( this );
            if ( startNode != null )
            {
                startNode.removeRelationship( type, id );
            }
            if ( endNode != null )
            {
                endNode.removeRelationship( type, id );
            }
            success = true;
        }
        finally
        {
            boolean releaseFailed = false;
            try
            {
                if ( startNodeLocked )
                {
                    nodeManager.releaseLock( startNode, LockType.WRITE );
                }
            }
            catch ( Exception e )
            {
                releaseFailed = true;
                e.printStackTrace();
            }
            try
            {
                if ( endNodeLocked )
                {
                    nodeManager.releaseLock( endNode, LockType.WRITE );
                }
            }
            catch ( Exception e )
            {
                releaseFailed = true;
                e.printStackTrace();
            }
            nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
            if ( releaseFailed )
            {
                throw new RuntimeException( "Unable to release locks ["
                    + startNode + "," + endNode + "] in relationship delete->"
                    + this );
            }
        }
    }

    public String toString()
    {
        return "RelationshipImpl #" + this.getId() + " of type " + type
            + " between Node[" + startNodeId + "] and Node[" + endNodeId + "]";
    }

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
     * Returns true if object <CODE>o</CODE> is a relationship with the same
     * id as <CODE>this</CODE>.
     * 
     * @param o
     *            the object to compare
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
        // then x.equals(z) == true
        // o consistent: the nodeId never changes
        return this.getId() == ((Relationship) o).getId();

    }

    public int hashCode()
    {
        return id;
    }
}