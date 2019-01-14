/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.newapi;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Utility class that performs necessary updates for the transaction state.
 */
public class IndexTxStateUpdater
{
    private final StoreReadLayer storeReadLayer;
    private final Read read;
    private final IndexingService indexingService;

    // We can use the StoreReadLayer directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater( StoreReadLayer storeReadLayer, Read read, IndexingService indexingService )
    {
        this.storeReadLayer = storeReadLayer;
        this.read = read;
        this.indexingService = indexingService;
    }

    // LABEL CHANGES

    public enum LabelChangeType
    {
        ADDED_LABEL,
        REMOVED_LABEL
    }

    /**
     * A label has been changed, figure out what updates are needed to tx state.
     *
     * @param labelId The id of the changed label
     * @param node cursor to the node where the change was applied
     * @param propertyCursor cursor to the properties of node
     * @param changeType The type of change event
     */
    void onLabelChange( int labelId, NodeCursor node, PropertyCursor propertyCursor, LabelChangeType changeType )
    {
        assert noSchemaChangedInTx();

        // Find properties of the changed node
        PrimitiveIntSet nodePropertyIds = Primitive.intSet();
        node.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            nodePropertyIds.add( propertyCursor.propertyKey() );
        }

        // Check all indexes of the changed label
        Iterator<SchemaIndexDescriptor> indexes = storeReadLayer.indexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            SchemaIndexDescriptor index = indexes.next();
            int[] indexPropertyIds = index.schema().getPropertyIds();
            if ( nodeHasIndexProperties( nodePropertyIds, indexPropertyIds ) )
            {
                Value[] values = getValueTuple( node, propertyCursor, indexPropertyIds );
                switch ( changeType )
                {
                case ADDED_LABEL:
                    indexingService.validateBeforeCommit( index.schema(), values );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, ValueTuple.of( values ) );
                    break;
                case REMOVED_LABEL:
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), ValueTuple.of( values ), null );
                    break;
                default:
                    throw new IllegalStateException( changeType + " is not a supported event" );
                }
            }
        }
    }

    private boolean noSchemaChangedInTx()
    {
        return !(read.txState().hasChanges() && !read.txState().hasDataChanges());
    }

    //PROPERTY CHANGES

    void onPropertyAdd( NodeCursor node, PropertyCursor propertyCursor, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx();
        Iterator<SchemaIndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        NodeSchemaMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Value[] values = getValueTuple( node, propertyCursor, propertyKeyId, value, index.schema().getPropertyIds() );
                    indexingService.validateBeforeCommit( index.schema(), values );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, ValueTuple.of( values ) );
                } );
    }

    void onPropertyRemove( NodeCursor node, PropertyCursor propertyCursor, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx();
        Iterator<SchemaIndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        NodeSchemaMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Value[] values = getValueTuple( node, propertyCursor, propertyKeyId, value, index.schema().getPropertyIds() );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), ValueTuple.of( values ), null );
                } );
    }

    void onPropertyChange( NodeCursor node, PropertyCursor propertyCursor, int propertyKeyId,
            Value beforeValue, Value afterValue )
    {
        assert noSchemaChangedInTx();
        Iterator<SchemaIndexDescriptor> indexes = storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        NodeSchemaMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
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
                            node.properties( propertyCursor );
                            Value value = NO_VALUE;
                            while ( propertyCursor.next() )
                            {
                                if ( propertyCursor.propertyKey() == indexPropertyId )
                                {
                                    value = propertyCursor.propertyValue();
                                }
                            }
                            valuesBefore[i] = value;
                            valuesAfter[i] = value;
                        }
                    }
                    indexingService.validateBeforeCommit( index.schema(), valuesAfter );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(),
                            ValueTuple.of( valuesBefore ), ValueTuple.of( valuesAfter ) );
                } );
    }

    private Value[] getValueTuple( NodeCursor node, PropertyCursor propertyCursor, int[] indexPropertyIds )
    {
        return getValueTuple( node, propertyCursor, NO_SUCH_PROPERTY_KEY, NO_VALUE, indexPropertyIds );
    }

    private Value[] getValueTuple( NodeCursor node, PropertyCursor propertyCursor,
            int changedPropertyKeyId, Value changedValue, int[] indexPropertyIds )
    {
        Value[] values = new Value[indexPropertyIds.length];
        node.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            int k = ArrayUtils.indexOf( indexPropertyIds, propertyCursor.propertyKey() );
            if ( k >= 0 )
            {
                values[k] = indexPropertyIds[k] == changedPropertyKeyId
                            ? changedValue : propertyCursor.propertyValue();
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
