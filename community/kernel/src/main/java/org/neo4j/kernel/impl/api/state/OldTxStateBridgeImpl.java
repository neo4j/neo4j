/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.DiffSets;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.core.WritableTransactionState;
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
    public DiffSets<Long> getNodesWithChangedProperty( int propertyKey, Object value )
    {
        DiffSets<Long> diff = new DiffSets<>();
        for ( WritableTransactionState.CowNodeElement changedNode : state.getChangedNodes() )
        {
            // All nodes where the property has been removed altogether
            DefinedProperty removed = propertyChange( changedNode.getPropertyRemoveMap( false ), propertyKey );
            if ( removed != null && removed.value().equals( value ) )
            {
                diff.remove( changedNode.getId() );
            }

            // All nodes where property has been added or changed
            if ( !changedNode.isDeleted() )
            {
                DefinedProperty added = propertyChange( changedNode.getPropertyAddMap( false ), propertyKey );
                if ( added != null )
                {
                    if ( added.valueEquals( value ) )
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
        return diff;
    }

    @Override
    public Map<Long, Object> getNodesWithChangedProperty( int propertyKeyId )
    {
        HashMap<Long, Object> result = new HashMap<>();
        for ( WritableTransactionState.CowNodeElement changedNode : state.getChangedNodes() )
        {
            if ( changedNode.isDeleted() )
            {
                result.put( changedNode.getId(), new Object() );
                continue;
            }
            DefinedProperty added = propertyChange( changedNode.getPropertyAddMap( false ), propertyKeyId );
            if ( added != null )
            {
                result.put( changedNode.getId(), added.value() );
            }
            else if ( null != propertyChange( changedNode.getPropertyRemoveMap( false ), propertyKeyId ) )
            {
                result.put( changedNode.getId(), new Object() );
            }
        }
        return result;
    }

    private static DefinedProperty propertyChange( ArrayMap<Integer, DefinedProperty> propertyDataMap, long propertyKeyId )
    {
        return propertyDataMap == null ? null : propertyDataMap.get( (int) propertyKeyId );
    }

    @Override
    public void deleteNode( long nodeId )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        boolean success = false;
        try
        {
            ArrayMap<Integer, DefinedProperty> skipMap = state.getOrCreateCowPropertyRemoveMap( node );
            ArrayMap<Integer, DefinedProperty> removedProps = nodeManager.deleteNode( node, state );
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
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId );
        boolean success = false;
        try
        {
            ArrayMap<Integer, DefinedProperty> skipMap = state.getOrCreateCowPropertyRemoveMap( relationship );
            ArrayMap<Integer, DefinedProperty> removedProps = nodeManager.deleteRelationship( relationship, state );
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
    public void nodeSetProperty( long nodeId, DefinedProperty property )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        state.getOrCreateCowPropertyAddMap( node ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> removed = state.getCowPropertyRemoveMap( node );
        if ( removed != null )
        {
            removed.remove( property.propertyKeyId() );
        }
    }

    @Override
    public void relationshipSetProperty( long relationshipId, DefinedProperty property )
    {
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId );
        state.getOrCreateCowPropertyAddMap( relationship ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> removed = state.getCowPropertyRemoveMap( relationship );
        if ( removed != null )
        {
            removed.remove( property.propertyKeyId() );
        }
    }

    @Override
    public void graphSetProperty( DefinedProperty property )
    {
        GraphPropertiesImpl properties = nodeManager.getGraphProperties();
        state.getOrCreateCowPropertyAddMap( properties ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> removed = state.getCowPropertyRemoveMap( properties );
        if ( removed != null )
        {
            removed.remove( property.propertyKeyId() );
        }
    }

    @Override
    public void nodeRemoveProperty( long nodeId, DefinedProperty property )
    {
        NodeImpl node = nodeManager.getNodeForProxy( nodeId, null );
        state.getOrCreateCowPropertyRemoveMap( node ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> added = state.getCowPropertyAddMap( node );
        if ( added != null )
        {
            added.remove( property.propertyKeyId() );
        }
    }

    @Override
    public void relationshipRemoveProperty( long relationshipId, DefinedProperty property )
    {
        RelationshipImpl relationship = nodeManager.getRelationshipForProxy( relationshipId );
        state.getOrCreateCowPropertyRemoveMap( relationship ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> added = state.getCowPropertyAddMap( relationship );
        if ( added != null )
        {
            added.remove( property.propertyKeyId() );
        }
    }

    @Override
    public void graphRemoveProperty( DefinedProperty property )
    {
        GraphPropertiesImpl properties = nodeManager.getGraphProperties();
        state.getOrCreateCowPropertyRemoveMap( properties ).put( property.propertyKeyId(), property );
        ArrayMap<Integer, DefinedProperty> added = state.getCowPropertyAddMap( properties );
        if ( added != null )
        {
            added.remove( property.propertyKeyId() );
        }
    }
}
