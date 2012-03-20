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

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.LockReleaser.CowEntityElement;
import org.neo4j.kernel.impl.core.LockReleaser.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.LockException;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;

public class RelationshipImpl extends ArrayBasedPrimitive
{
    /*
     * The id long is used to store the relationship id (which is at most 35 bits).
     * But also the high order bits for the start node (s) and end node (e) as well
     * as the relationship type. This allows for a more compressed memory
     * representation.
     * 
     *    2 bytes type      start/end high                   5 bytes of id
     * [tttt,tttt][tttt,tttt][ssss,eeee][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
     */
    private final long idAndMore;
    private final int startNodeId;
    private final int endNodeId;

    RelationshipImpl( long id, long startNodeId, long endNodeId, int typeId, boolean newRel )
    {
        super(  newRel );
        this.startNodeId = (int) startNodeId;
        this.endNodeId = (int) endNodeId;
        this.idAndMore = (((long)typeId) << 48) | ((startNodeId&0xF00000000L)<<12) | ((endNodeId&0xF00000000L)<<8) | id;
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

    @Override
    public long getId()
    {
        return idAndMore&0xFFFFFFFFFFL;
    }
    
    long getStartNodeId()
    {
        return (long)(((long)startNodeId&0xFFFFFFFFL) | ((idAndMore&0xF00000000000L)>>12));
    }

    long getEndNodeId()
    {
        return (long)(((long)endNodeId&0xFFFFFFFFL) | ((idAndMore&0xF0000000000L)>>8));
    }
    
    int getTypeId()
    {
        return (int)((idAndMore&0xFFFF000000000000L)>>48);
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
            RelationshipType type = nodeManager.getRelationshipTypeById( getTypeId() );
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
        return "RelationshipImpl #" + this.getId() + " of type " + getTypeId()
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
        return nm.newRelationshipProxyById( getId() );
    }

    @Override
    protected void updateSize( int sizeBefore, int sizeAfter, NodeManager nodeManager )
    {
        nodeManager.updateCacheSize( this, sizeBefore, sizeAfter );
    }
}