/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.schema.NodeSchemaMatcher;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public class IndexTxStateUpdater
{
    private final StoreReadLayer storeReadLayer;
    private final EntityReadOperations readOps;
    private final NodeSchemaMatcher nodeIndexMatcher;
    private final IndexingService indexingService;

    // We can use the StoreReadLayer directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater( StoreReadLayer storeReadLayer, EntityReadOperations readOps, IndexingService indexingService )
    {
        this.storeReadLayer = storeReadLayer;
        this.readOps = readOps;
        this.nodeIndexMatcher = new NodeSchemaMatcher( readOps );
        this.indexingService = indexingService;
    }

    // LABEL CHANGES

    public enum LabelChangeType
    {
        ADDED_LABEL,
        REMOVED_LABEL
    }

    public void onLabelChange( KernelStatement state, int labelId, NodeItem node, LabelChangeType changeType )
    {
        assert noSchemaChangedInTx( state );
        PrimitiveIntSet nodePropertyIds = Primitive.intSet();
        nodePropertyIds.addAll( readOps.nodeGetPropertyKeys( state, node ).iterator() );

        Iterator<SchemaIndexDescriptor> indexes = storeReadLayer.indexesGetForLabel( labelId );

        while ( indexes.hasNext() )
        {
            SchemaIndexDescriptor index = indexes.next();
            int[] indexPropertyIds = index.schema().getPropertyIds();
            if ( nodeHasIndexProperties( nodePropertyIds, indexPropertyIds ) )
            {
                Value[] values = getValueTuple( state, node, indexPropertyIds );
                if ( changeType == LabelChangeType.ADDED_LABEL )
                {
                    indexingService.validateBeforeCommit( index.schema(), values );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, ValueTuple.of( values ) );
                }
                else
                {
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), ValueTuple.of( values ), null );
                }
            }
        }
    }

    private boolean noSchemaChangedInTx( KernelStatement state )
    {
        return !(state.txState().hasChanges() && !state.txState().hasDataChanges());
    }

    // PROPERTY CHANGES

    public void onPropertyAdd( KernelStatement state, NodeItem node, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx( state );
        Iterator<SchemaIndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Value[] values = getValueTuple( state, node, propertyKeyId, value, index.schema().getPropertyIds() );
                    indexingService.validateBeforeCommit( index.schema(), values );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, ValueTuple.of( values ) );
                } );
    }

    public void onPropertyRemove( KernelStatement state, NodeItem node, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx( state );
        Iterator<SchemaIndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Value[] values = getValueTuple( state, node, propertyKeyId, value, index.schema().getPropertyIds() );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), ValueTuple.of( values ), null );
                });
    }

    public void onPropertyChange( KernelStatement state, NodeItem node, int propertyKeyId, Value beforeValue, Value afterValue )
    {
        assert noSchemaChangedInTx( state );
        Iterator<SchemaIndexDescriptor> indexes = storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    int[] indexPropertyIds = index.schema().getPropertyIds();

                    Value[] valuesBefore = new Value[indexPropertyIds.length];
                    Value[] valuesAfter = new Value[indexPropertyIds.length];
                    for ( int i = 0; i < indexPropertyIds.length; i++ )
                    {
                        int indexPropertyId = indexPropertyIds[i];
                        if ( indexPropertyId == propertyKeyId )
                        {
                            valuesBefore[i] = beforeValue;
                            valuesAfter[i] = afterValue;
                        }
                        else
                        {
                            Value value = readOps.nodeGetProperty( state, node, indexPropertyId );
                            valuesBefore[i] = value;
                            valuesAfter[i] = value;
                        }
                    }
                    indexingService.validateBeforeCommit( index.schema(), valuesAfter );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(),
                            ValueTuple.of( valuesBefore ), ValueTuple.of( valuesAfter ) );
                });
    }

    // HELPERS

    private Value[] getValueTuple( KernelStatement state, NodeItem node,
            int[] indexPropertyIds )
    {
        return getValueTuple( state, node, NO_SUCH_PROPERTY_KEY, Values.NO_VALUE, indexPropertyIds );
    }

    private Value[] getValueTuple( KernelStatement state, NodeItem node,
            int changedPropertyKeyId, Value changedValue, int[] indexPropertyIds )
    {
        Value[] values = new Value[indexPropertyIds.length];
        Cursor<PropertyItem> propertyCursor = readOps.nodeGetProperties( state, node );
        while ( propertyCursor.next() )
        {
            PropertyItem property = propertyCursor.get();
            int k = ArrayUtils.indexOf( indexPropertyIds, property.propertyKeyId() );
            if ( k >= 0 )
            {
                values[k] = indexPropertyIds[k] == changedPropertyKeyId
                            ? changedValue : property.value();
            }
        }

        if ( changedPropertyKeyId != NO_SUCH_PROPERTY_KEY )
        {
            int k = ArrayUtils.indexOf( indexPropertyIds, changedPropertyKeyId );
            if ( k >= 0 )
            {
                values[k] = changedValue;
            }
        }

        return values;
    }

    private static boolean nodeHasIndexProperties( PrimitiveIntSet nodeProperties, int[] indexPropertyIds )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( !nodeProperties.contains( indexPropertyId ) )
            {
                return false;
            }
        }
        return true;
    }
}
