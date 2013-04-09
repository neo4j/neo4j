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

import org.neo4j.kernel.impl.api.DiffSets;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
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
}
