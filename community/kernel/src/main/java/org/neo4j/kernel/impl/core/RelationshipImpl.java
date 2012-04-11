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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.LockReleaser.CowEntityElement;
import org.neo4j.kernel.impl.core.LockReleaser.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.LockException;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;

public abstract class RelationshipImpl extends ArrayBasedPrimitive
{
    RelationshipImpl( long startNodeId, long endNodeId, boolean newRel )
    {
        super( newRel );
    }

    protected RelationshipType assertTypeNotNull( RelationshipType type )
    {
        if ( type == null )
        {
            throw new IllegalArgumentException( "Null type" );
        }
        return type;
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj
               || ( obj instanceof RelationshipImpl && ( (RelationshipImpl) obj ).getId() == getId() );
    }

    @Override
    protected PropertyData changeProperty( NodeManager nodeManager,
            PropertyData property, Object value )
    {
        return nodeManager.relChangeProperty( this, property, value );
    }

    @Override
    protected PropertyData addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
    {
        return nodeManager.relAddProperty( this, index, value );
    }

    @Override
    public int size()
    {
        return super.size() + 8 + 8;
    }

    @Override
    protected void removeProperty( NodeManager nodeManager,
            PropertyData property )
    {
        nodeManager.relRemoveProperty( this, property );
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties(
            NodeManager nodeManager, boolean light )
    {
        return nodeManager.loadProperties( this, light );
    }

    public Node[] getNodes( NodeManager nodeManager )
    {
        return new Node[] { new NodeProxy( getStartNodeId(), nodeManager ),
            new NodeProxy( getEndNodeId(), nodeManager ) };
    }

    public Node getOtherNode( NodeManager nodeManager, Node node )
    {
        if ( getStartNodeId() == node.getId() )
        {
            return new NodeProxy( getEndNodeId(), nodeManager );
        }
        if ( getEndNodeId() == node.getId() )
        {
            return new NodeProxy( getStartNodeId(), nodeManager );
        }
        throw new NotFoundException( "Node[" + node.getId()
            + "] not connected to this relationship[" + getId() + "]" );
    }

    public Node getStartNode( NodeManager nodeManager )
    {
        return new NodeProxy( getStartNodeId(), nodeManager );
    }

    abstract long getStartNodeId();

    public Node getEndNode( NodeManager nodeManager )
    {
        return new NodeProxy( getEndNodeId(), nodeManager );
    }

    abstract long getEndNodeId();

    public abstract RelationshipType getType( NodeManager nodeManager );

    public boolean isType( NodeManager nodeManager, RelationshipType otherType )
    {
        return otherType != null
            && otherType.name().equals( this.getType( nodeManager ).name() );
    }

    public void delete( NodeManager nodeManager, Relationship proxy )
    {
        NodeImpl startNode = null;
        NodeImpl endNode = null;
        boolean startNodeLocked = false;
        boolean endNodeLocked = false;
        boolean thisLocked = false;
        boolean success = false;
        try
        {
            startNode = nodeManager.getLightNode( getStartNodeId() );
            if ( startNode != null )
            {
                nodeManager.acquireLock( startNode, LockType.WRITE );
                startNodeLocked = true;
            }
            endNode = nodeManager.getLightNode( getEndNodeId() );
            if ( endNode != null )
            {
                nodeManager.acquireLock( endNode, LockType.WRITE );
                endNodeLocked = true;
            }
            nodeManager.acquireLock( proxy, LockType.WRITE );
            thisLocked = true;
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            ArrayMap<Integer,PropertyData> skipMap =
                nodeManager.getOrCreateCowPropertyRemoveMap( this );
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
            RelationshipType type = getType( nodeManager );
            long id = getId();
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
                    nodeManager.releaseLock( proxy, LockType.WRITE );
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
        return "RelationshipImpl #" + this.getId() + " of type " + getType( null )
            + " between Node[" + getStartNodeId() + "] and Node[" + getEndNodeId() + "]";
    }

    @Override
    public CowEntityElement getEntityElement( PrimitiveElement element, boolean create )
    {
        return element.relationshipElement( getId(), create );
    }

    @Override
    PropertyContainer asProxy( NodeManager nm )
    {
        return new RelationshipProxy( getId(), nm );
    }

    @Override
    protected void updateSize( NodeManager nodeManager )
    {
        nodeManager.updateCacheSize( this, size() );
    }
}