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

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.OrderedPropertyValues;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.schema.NodeSchemaMatcher;
import org.neo4j.storageengine.api.NodeItem;

import static org.neo4j.kernel.api.properties.DefinedProperty.NO_SUCH_PROPERTY;
import static org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates.hasProperty;

public class IndexTxStateUpdater
{
    private final SchemaReadOperations schemaReadOps;
    private final EntityReadOperations readOps;
    private final NodeSchemaMatcher<NewIndexDescriptor> nodeIndexMatcher;

    public IndexTxStateUpdater( SchemaReadOperations schemaReadOps, EntityReadOperations readOps )
    {
        this.schemaReadOps = schemaReadOps;
        this.readOps = readOps;
        this.nodeIndexMatcher = new NodeSchemaMatcher<>( readOps );
    }

    // LABEL CHANGES

    public enum LabelChangeType { ADDED_LABEL, REMOVED_LABEL };

    public void onLabelChange( KernelStatement state, int labelId, NodeItem node, LabelChangeType changeType )
            throws EntityNotFoundException
    {
        PrimitiveIntSet nodePropertyIds = Primitive.intSet();
        nodePropertyIds.addAll( readOps.nodeGetPropertyKeys( state, node ).iterator() );

        Iterator<NewIndexDescriptor> indexes =
                Iterators.concat(
                        schemaReadOps.indexesGetForLabel( state, labelId ),
                        schemaReadOps.uniqueIndexesGetForLabel( state, labelId ) );

        while ( indexes.hasNext() )
        {
            NewIndexDescriptor index = indexes.next();
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

    // PROPERTY CHANGES

    public void onPropertyAdd( KernelStatement state, NodeItem node, DefinedProperty after )
            throws EntityNotFoundException
    {
        Iterator<NewIndexDescriptor> indexes = getIndexesForProperty( state, after.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, after.propertyKeyId(),
                index -> {
                    OrderedPropertyValues values = getOrderedPropertyValues( state, node, after, index.schema().getPropertyIds() );
                    if ( values != null )
                    {
                        values.validate();
                        state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, values );
                    }
                });
    }

    public void onPropertyRemove( KernelStatement state, NodeItem node, DefinedProperty before )
            throws EntityNotFoundException
    {
        Iterator<NewIndexDescriptor> indexes = getIndexesForProperty( state, before.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, before.propertyKeyId(),
                index -> {
                    OrderedPropertyValues values = getOrderedPropertyValues( state, node, before, index.schema().getPropertyIds() );
                    if ( values != null )
                    {
                        state.txState().indexDoUpdateEntry( index.schema(), node.id(), values, null );
                    }
                });
    }

    public void onPropertyChange( KernelStatement state, NodeItem node, DefinedProperty before, DefinedProperty after )
            throws EntityNotFoundException
    {
        assert before.propertyKeyId() == after.propertyKeyId();
        Iterator<NewIndexDescriptor> indexes = getIndexesForProperty( state, before.propertyKeyId() );
        nodeIndexMatcher.onMatchingSchema( state, indexes, node, before.propertyKeyId(),
                index -> {
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
        for ( int i = 0; i < values.length; i++ )
        {
            int indexPropertyId = indexPropertyIds[i];
            values[i] = indexPropertyId == changedProperty.propertyKeyId()
                        ? changedProperty
                        : Property.property( indexPropertyId, readOps.nodeGetProperty( state, node, indexPropertyId ) );
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

    // Lifting this method to the schemaReadOps layer could allow more efficient finding of indexes, by introducing
    // suitable maps in the SchemaCache. This can be done when we have a benchmarking reason.
    private Iterator<NewIndexDescriptor> getIndexesForProperty( KernelStatement state, int propertyId )
    {
        Iterator<NewIndexDescriptor> allIndexes =
                Iterators.concat(
                        schemaReadOps.indexesGetAll( state ),
                        schemaReadOps.uniqueIndexesGetAll( state ) );

        return Iterators.filter( hasProperty( propertyId ), allIndexes );
    }

}
