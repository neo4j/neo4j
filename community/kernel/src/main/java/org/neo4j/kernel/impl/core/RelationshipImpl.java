/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
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
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int) (idAndMore ^ (idAndMore >>> 32));
        return result;
    }

    @Override
    protected PropertyData changeProperty( NodeManager nodeManager,
            PropertyData property, Object value, TransactionState tx )
    {
        return nodeManager.relChangeProperty( this, property, value, tx );
    }

    @Override
    protected PropertyData addProperty( NodeManager nodeManager, Token index, Object value )
    {
        return nodeManager.relAddProperty( this, index, value );
    }
    
    @Override
    public int size()
    {
        return super.size() + 8/*idAndMore*/ + 8/*startNodeId and endNodeId*/;
    }

    @Override
    protected void removeProperty( NodeManager nodeManager,
            PropertyData property, TransactionState tx )
    {
        nodeManager.relRemoveProperty( this, property, tx );
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
        return (startNodeId&0xFFFFFFFFL) | ((idAndMore&0xF00000000000L)>>12);
    }

    long getEndNodeId()
    {
        return (endNodeId&0xFFFFFFFFL) | ((idAndMore&0xF0000000000L)>>8);
    }
    
    int getTypeId()
    {
        return (int)((idAndMore&0xFFFF000000000000L)>>>48);
    }
    
    public void delete( NodeManager nodeManager, Relationship proxy )
    {
        NodeImpl startNode;
        NodeImpl endNode;
        boolean success = false;
        TransactionState tx;
        try
        {
            tx = nodeManager.getTransactionState();
            startNode = nodeManager.getLightNode( getStartNodeId() );
            if ( startNode != null )
            {
                tx.acquireWriteLock( nodeManager.newNodeProxyById( getStartNodeId() ) );
            }
            endNode = nodeManager.getLightNode( getEndNodeId() );
            if ( endNode != null )
            {
                tx.acquireWriteLock( nodeManager.newNodeProxyById( getEndNodeId() ) );
            }
            tx.acquireWriteLock( proxy );
            // no need to load full relationship, all properties will be
            // deleted when relationship is deleted

            ArrayMap<Integer,PropertyData> skipMap =
                tx.getOrCreateCowPropertyRemoveMap( this );
            ArrayMap<Integer,PropertyData> removedProps =
                nodeManager.deleteRelationship( this, tx );
            if ( removedProps.size() > 0 )
            {
                for ( int index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
            }
            success = true;
            int typeId = getTypeId();
            long id = getId();
            if ( startNode != null )
            {
                tx.getOrCreateCowRelationshipRemoveMap( startNode, typeId ).add( id );
            }
            if ( endNode != null )
            {
                tx.getOrCreateCowRelationshipRemoveMap( endNode, typeId ).add( id );
            }
            success = true;
        }
        finally
        {
            if ( !success )
            {
                nodeManager.setRollbackOnly();
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
    protected void updateSize( NodeManager nodeManager )
    {
        nodeManager.updateCacheSize( this, size() );
    }
}