/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.Validators;
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
    private final NodeSchemaMatcher nodeIndexMatcher;

    // We can use the StoreReadLayer directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater( StoreReadLayer storeReadLayer, Read read )
    {
        this.storeReadLayer = storeReadLayer;
        this.read = read;
        this.nodeIndexMatcher = new NodeSchemaMatcher( read );
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
    void onLabelChange( int labelId, org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor, LabelChangeType changeType )
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
        Iterator<IndexDescriptor> indexes = storeReadLayer.indexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            int[] indexPropertyIds = index.schema().getPropertyIds();
            if ( nodeHasIndexProperties( nodePropertyIds, indexPropertyIds ) )
            {
                ValueTuple values = getValueTuple( node, propertyCursor, indexPropertyIds );
                switch ( changeType )
                {
                case ADDED_LABEL:
                    for ( int i = 0; i < values.size(); i++ )
                    {
                        Validators.INDEX_VALUE_VALIDATOR.validate( values.valueAt( i ) );
                    }
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, values );
                    break;
                case REMOVED_LABEL:
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), values, null );
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

    void onPropertyAdd( org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx();
        Iterator<IndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Validators.INDEX_VALUE_VALIDATOR.validate( value );
                    ValueTuple values =
                            getValueTuple( node, propertyCursor, propertyKeyId, value,
                                    index.schema().getPropertyIds() );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, values );
                } );
    }

    void onPropertyRemove( org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor, int propertyKeyId, Value value )
    {
        assert noSchemaChangedInTx();
        Iterator<IndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    ValueTuple values =
                            getValueTuple( node, propertyCursor, propertyKeyId, value,
                                    index.schema().getPropertyIds() );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), values, null );
                } );
    }

    void onPropertyChange( org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor, int propertyKeyId, Value beforeValue,
            Value afterValue )
    {
        assert noSchemaChangedInTx();
        Iterator<IndexDescriptor> indexes = storeReadLayer.indexesGetRelatedToProperty( propertyKeyId );
        nodeIndexMatcher.onMatchingSchema( indexes, node, propertyCursor, propertyKeyId,
                ( index, propertyKeyIds ) ->
                {
                    Validators.INDEX_VALUE_VALIDATOR.validate( afterValue );
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
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(),
                            ValueTuple.of( valuesBefore ), ValueTuple.of( valuesAfter ) );
                } );
    }

    private ValueTuple getValueTuple( org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor, int[] indexPropertyIds )
    {
        return getValueTuple( node, propertyCursor, NO_SUCH_PROPERTY_KEY, NO_VALUE, indexPropertyIds );
    }

    private ValueTuple getValueTuple( org.neo4j.internal.kernel.api.NodeCursor node,
            org.neo4j.internal.kernel.api.PropertyCursor propertyCursor,
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

        return ValueTuple.of( values );
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
