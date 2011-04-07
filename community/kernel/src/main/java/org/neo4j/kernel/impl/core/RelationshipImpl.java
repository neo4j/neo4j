/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.LockException;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;

class RelationshipImpl extends Primitive
{
    private final long startNodeId;
    private final long endNodeId;
    private final RelationshipType type;

    RelationshipImpl( long id, long startNodeId, long endNodeId, RelationshipType type, boolean newRel )
    {
        super( id, newRel );
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

    @Override
    public int hashCode()
    {
        return (int) (( id >>> 32 ) ^ id );
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj
               || ( obj instanceof RelationshipImpl && ( (RelationshipImpl) obj ).id == id );
    }

    @Override
    protected void changeProperty( NodeManager nodeManager, long propertyId, Object value )
    {
        nodeManager.relChangeProperty( this, propertyId, value );
    }

    @Override
    protected long addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
    {
        return nodeManager.relAddProperty( this, index, value );
    }

    @Override
    protected void removeProperty( NodeManager nodeManager, long propertyId )
    {
        nodeManager.relRemoveProperty( this, propertyId );
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager, boolean light )
    {
        return nodeManager.loadProperties( this, light );
    }

    public Node[] getNodes( NodeManager nodeManager )
    {
        return new Node[] { new NodeProxy( startNodeId, nodeManager ),
            new NodeProxy( endNodeId, nodeManager ) };
    }

    public Node getOtherNode( NodeManager nodeManager, Node node )
    {
        if ( startNodeId == node.getId() )
        {
            return new NodeProxy( endNodeId, nodeManager );
        }
        if ( endNodeId == node.getId() )
        {
            return new NodeProxy( startNodeId, nodeManager );
        }
        throw new NotFoundException( "Node[" + node.getId()
            + "] not connected to this relationship[" + getId() + "]" );
    }

    public Node getStartNode( NodeManager nodeManager )
    {
        return new NodeProxy( startNodeId, nodeManager );
    }

    long getStartNodeId()
    {
        return startNodeId;
    }

    public Node getEndNode( NodeManager nodeManager )
    {
        return new NodeProxy( endNodeId, nodeManager );
    }

    long getEndNodeId()
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

    public void delete( NodeManager nodeManager )
    {
        NodeImpl startNode = null;
        NodeImpl endNode = null;
        boolean startNodeLocked = false;
        boolean endNodeLocked = false;
        boolean thisLocked = false;
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
            nodeManager.acquireLock( this, LockType.WRITE );
            thisLocked = true;
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            ArrayMap<Integer,PropertyData> skipMap = 
                nodeManager.getCowPropertyRemoveMap( this, true );
            ArrayMap<Integer,PropertyData> removedProps = 
                nodeManager.deleteRelationship( this );
            if ( removedProps.size() > 0 )
            {
                for ( int index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            success = true;
            if ( startNode != null )
            {
                startNode.removeRelationship( nodeManager, type, id );
            }
            if ( endNode != null )
            {
                endNode.removeRelationship( nodeManager, type, id );
            }
            success = true;
        }
        finally
        {
            boolean releaseFailed = false;
            try
            {
                if ( thisLocked )
                {
                    nodeManager.releaseLock( this, LockType.WRITE );
                }
            }
            catch ( Exception e )
            {
                releaseFailed = true;
                e.printStackTrace();
            }
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
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
            if ( releaseFailed )
            {
                throw new LockException( "Unable to release locks ["
                    + startNode + "," + endNode + "] in relationship delete->"
                    + this );
            }
        }
    }

    @Override
    public String toString()
    {
        return "RelationshipImpl #" + this.getId() + " of type " + type
            + " between Node[" + startNodeId + "] and Node[" + endNodeId + "]";
    }
}