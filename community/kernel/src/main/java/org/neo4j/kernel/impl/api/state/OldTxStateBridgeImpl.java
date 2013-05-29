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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

public class OldTxStateBridgeImpl implements OldTxStateBridge
{
    private final NodeManager nodeManager;
    private final TransactionState state;

    public OldTxStateBridgeImpl( NodeManager nodeManager, TransactionState transactionState )
    {
        this.nodeManager = nodeManager;
        this.state = transactionState;
    }

    @Override
    public DiffSets<Long> getNodesWithChangedProperty( long propertyKey, Object value )
    {
        DiffSets<Long> diff = new DiffSets<Long>();
        Iterable<WritableTransactionState.CowNodeElement> changedNodes = state.getChangedNodes();

        for ( WritableTransactionState.CowNodeElement changedNode : changedNodes )
        {

            // All nodes where the property has been removed altogether
            ArrayMap<Integer, PropertyData> propRmMap = changedNode.getPropertyRemoveMap( false );
            if ( propRmMap != null )
            {
                for ( PropertyData propertyData : propRmMap.values() )
                {
                    if ( propertyData.getIndex() == propertyKey && propertyData.getValue().equals( value ) )
                    {
                        diff.remove( changedNode.getId() );
                    }
                }
            }

            // All nodes where property has been added or changed
            if ( !changedNode.isDeleted() )
            {
                ArrayMap<Integer, PropertyData> propAddMap = changedNode.getPropertyAddMap( false );
                if ( propAddMap != null )
                {
                    for ( PropertyData propertyData : propAddMap.values() )
                    {
                        if ( propertyData.getIndex() == propertyKey )
                        {
                            // Added if value is the same, removed if value is different.
                            if ( propertyData.getValue().equals( value ) )
                            {
                                diff.add( changedNode.getId() );
                            }
                            else
                            {
                                diff.remove( changedNode.getId() );
                            }
                        }
                    }
                }
            }
        }

        return diff;
    }

    @Override
    public void deleteNode( long nodeId )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        boolean success = false;
        try
        {
            ArrayMap<Integer, PropertyData> skipMap = state.getOrCreateCowPropertyRemoveMap( node );
            ArrayMap<Integer, PropertyData> removedProps = nodeManager.deleteNode( node, state );
            if ( removedProps.size() > 0 )
            {
                for ( Integer index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
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
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return state.getCreatedNodes().contains( nodeId );
    }

    @Override
    public void deleteRelationship( long relationshipId )
    {
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId, null );
        boolean success = false;
        try
        {
            ArrayMap<Integer, PropertyData> skipMap = state.getOrCreateCowPropertyRemoveMap( relationship );
            ArrayMap<Integer, PropertyData> removedProps = nodeManager.deleteRelationship( relationship, state );
            if ( removedProps.size() > 0 )
            {
                for ( Integer index : removedProps.keySet() )
                {
                    skipMap.put( index, removedProps.get( index ) );
                }
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
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return state.getCreatedRelationships().contains( relationshipId );
    }

    @Override
    public boolean hasChanges()
    {
        return state.hasChanges();
    }

    @Override
    public void nodeSetProperty( long nodeId, PropertyData property )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        state.getOrCreateCowPropertyAddMap( node ).put( property.getIndex(), property );
        ArrayMap<Integer, PropertyData> removed = state.getCowPropertyRemoveMap( node );
        if ( removed != null )
        {
            removed.remove( property.getIndex() );
        }
    }

    @Override
    public void relationshipSetProperty( long relationshipId, PropertyData property )
    {
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId, null );
        state.getOrCreateCowPropertyAddMap( relationship ).put( property.getIndex(), property );
        ArrayMap<Integer, PropertyData> removed = state.getCowPropertyRemoveMap( relationship );
        if ( removed != null )
        {
            removed.remove( property.getIndex() );
        }
    }

    @Override
    public void graphSetProperty( PropertyData property )
    {
        GraphPropertiesImpl properties = nodeManager.getGraphProperties();
        state.getOrCreateCowPropertyAddMap( properties ).put( property.getIndex(), property );
        ArrayMap<Integer, PropertyData> removed = state.getCowPropertyRemoveMap( properties );
        if ( removed != null )
        {
            removed.remove( property.getIndex() );
        }
    }

    @Override
    public void nodeRemoveProperty( long nodeId, Property property )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        state.getOrCreateCowPropertyRemoveMap( node ).put( (int) property.propertyKeyId(),
                property.asPropertyDataJustForIntegration() );
        ArrayMap<Integer, PropertyData> added = state.getCowPropertyAddMap( node );
        if ( added != null )
        {
            added.remove( (int) property.propertyKeyId() );
        }
    }

    @Override
    public void relationshipRemoveProperty( long relationshipId, Property property )
    {
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId, null );
        state.getOrCreateCowPropertyRemoveMap( relationship ).put( (int) property.propertyKeyId(),
                property.asPropertyDataJustForIntegration() );
        ArrayMap<Integer, PropertyData> added = state.getCowPropertyAddMap( relationship );
        if ( added != null )
        {
            added.remove( (int) property.propertyKeyId() );
        }
    }

    @Override
    public void graphRemoveProperty( Property property )
    {
        GraphPropertiesImpl properties = nodeManager.getGraphProperties();
        state.getOrCreateCowPropertyRemoveMap( properties ).put( (int) property.propertyKeyId(),
                property.asPropertyDataJustForIntegration() );
        ArrayMap<Integer, PropertyData> added = state.getCowPropertyAddMap( properties );
        if ( added != null )
        {
            added.remove( (int) property.propertyKeyId() );
        }

    }
}
