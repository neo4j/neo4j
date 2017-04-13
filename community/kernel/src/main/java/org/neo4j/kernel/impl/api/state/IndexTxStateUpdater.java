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
package org.neo4j.kernel.impl.api.state;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema.OrderedPropertyValues;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.schema.NodeSchemaMatcher;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.StoreReadLayer;

import static org.neo4j.kernel.api.properties.DefinedProperty.NO_SUCH_PROPERTY;

public class IndexTxStateUpdater
{
    private final StoreReadLayer storeReadLayer;
    private final EntityReadOperations readOps;
    private final NodeSchemaMatcher nodeIndexMatcher;

    // We can use the StoreReadLayer directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater( StoreReadLayer storeReadLayer, EntityReadOperations readOps )
    {
        this.storeReadLayer = storeReadLayer;
        this.readOps = readOps;
        this.nodeIndexMatcher = new NodeSchemaMatcher( readOps );
    }

    // LABEL CHANGES

    public enum LabelChangeType
    {
        ADDED_LABEL,
        REMOVED_LABEL
    };

    public void onLabelChange( KernelStatement state, int labelId, NodeItem node, LabelChangeType changeType )
            throws EntityNotFoundException
    {
        assert noSchemaChangedInTx( state );
        PrimitiveIntSet nodePropertyIds = Primitive.intSet();
        nodePropertyIds.addAll( readOps.nodeGetPropertyKeys( state, node ).iterator() );

        Iterator<IndexDescriptor> indexes = storeReadLayer.indexesGetForLabel( labelId );

        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            int[] indexPropertyIds = index.schema().getPropertyIds();
            if ( nodeHasIndexProperties( nodePropertyIds, indexPropertyIds ) )
            {
                OrderedPropertyValues values = getOrderedPropertyValues( state, node, indexPropertyIds );
                if ( changeType == LabelChangeType.ADDED_LABEL )
                {
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, values );
                }
                else
                {
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), values, null );
                }
            }
        }
    }

    private boolean noSchemaChangedInTx( KernelStatement state )
    {
        return !(state.txState().hasChanges() && !state.txState().hasDataChanges());
    }

    // PROPERTY CHANGES

    public void onPropertyAdd( KernelStatement state, NodeItem node, DefinedProperty after )
            throws EntityNotFoundException
    {
        assert noSchemaChangedInTx( state );
        Iterator<IndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( after.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, after.propertyKeyId(),
                ( index, propertyKeyIds ) ->
                {
                    Validators.INDEX_VALUE_VALIDATOR.validate( after.value() );
                    OrderedPropertyValues values =
                            getOrderedPropertyValues( state, node, after, index.schema().getPropertyIds() );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, values );
                } );
    }

    public void onPropertyRemove( KernelStatement state, NodeItem node, DefinedProperty before )
            throws EntityNotFoundException
    {
        assert noSchemaChangedInTx( state );
        Iterator<IndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( before.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, before.propertyKeyId(),
                ( index, propertyKeyIds ) ->
                {
                    OrderedPropertyValues values =
                            getOrderedPropertyValues( state, node, before, index.schema().getPropertyIds() );
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), values, null );
                });
    }

    public void onPropertyChange( KernelStatement state, NodeItem node, DefinedProperty before, DefinedProperty after )
            throws EntityNotFoundException
    {
        assert noSchemaChangedInTx( state );
        assert before.propertyKeyId() == after.propertyKeyId();
        Iterator<IndexDescriptor> indexes =
                storeReadLayer.indexesGetRelatedToProperty( before.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, before.propertyKeyId(),
                ( index, propertyKeyIds ) ->
                {
                    Validators.INDEX_VALUE_VALIDATOR.validate( after.value() );
                    int[] indexPropertyIds = index.schema().getPropertyIds();

                    Object[] valuesBefore = new Object[indexPropertyIds.length];
                    Object[] valuesAfter = new Object[indexPropertyIds.length];
                    for ( int i = 0; i < indexPropertyIds.length; i++ )
                    {
                        int indexPropertyId = indexPropertyIds[i];
                        if ( indexPropertyId == before.propertyKeyId() )
                        {
                            valuesBefore[i] = before.value();
                            valuesAfter[i] = after.value();
                        }
                        else
                        {
                            Object value = readOps.nodeGetProperty( state, node, indexPropertyId );
                            valuesBefore[i] = value;
                            valuesAfter[i] = value;
                        }
                    }
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(),
                            OrderedPropertyValues.ofUndefined( valuesBefore ), OrderedPropertyValues.ofUndefined( valuesAfter ) );
                });
    }

    // HELPERS

    private OrderedPropertyValues getOrderedPropertyValues( KernelStatement state, NodeItem node,
            int[] indexPropertyIds )
    {
        return getOrderedPropertyValues( state, node, NO_SUCH_PROPERTY, indexPropertyIds );
    }

    private OrderedPropertyValues getOrderedPropertyValues( KernelStatement state, NodeItem node,
            DefinedProperty changedProperty, int[] indexPropertyIds )
    {
        DefinedProperty[] values = new DefinedProperty[indexPropertyIds.length];
        Cursor<PropertyItem> propertyCursor = readOps.nodeGetProperties( state, node );
        while ( propertyCursor.next() )
        {
            PropertyItem property = propertyCursor.get();
            int k = ArrayUtils.indexOf( indexPropertyIds, property.propertyKeyId() );
            if ( k >= 0 )
            {
                values[k] = indexPropertyIds[k] == changedProperty.propertyKeyId()
                            ? changedProperty
                            : Property.property( indexPropertyIds[k], property.value() );
            }
        }

        if ( changedProperty != NO_SUCH_PROPERTY )
        {
            int k = ArrayUtils.indexOf( indexPropertyIds, changedProperty.propertyKeyId() );
            if ( k >= 0 )
            {
                values[k] = changedProperty;
            }
        }

        return OrderedPropertyValues.of( values );
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
