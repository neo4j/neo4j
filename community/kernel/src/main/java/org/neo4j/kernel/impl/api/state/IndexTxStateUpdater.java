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
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.OrderedPropertyValues;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.storageengine.api.NodeItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.api.properties.DefinedProperty.NO_SUCH_PROPERTY;
import static org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates.hasProperty;

public class IndexTxStateUpdater
{
    private final SchemaReadOperations schemaReadOps;
    private final EntityReadOperations readOps;

    public IndexTxStateUpdater( SchemaReadOperations schemaReadOps, EntityReadOperations readOps )
    {
        this.schemaReadOps = schemaReadOps;
        this.readOps = readOps;
    }

    // LABEL CHANGES

    public enum LabelChangeType { ADDED_LABEL, REMOVED_LABEL };

    public void onLabelChange( KernelStatement state, int labelId, NodeItem node, LabelChangeType changeType )
            throws EntityNotFoundException
    {
        PrimitiveIntSet propertyIds = Primitive.intSet();
        propertyIds.addAll( readOps.nodeGetPropertyKeys( state, node ).iterator() );

        Iterator<NewIndexDescriptor> indexes =
                Iterators.concat(
                        schemaReadOps.indexesGetForLabel( state, labelId ),
                        schemaReadOps.uniqueIndexesGetForLabel( state, labelId ) );

        while ( indexes.hasNext() )
        {
            NewIndexDescriptor index = indexes.next();
            OrderedPropertyValues values = valuesIfPropertiesMatch( state, propertyIds, index, node );
            if ( values != null )
            {
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

    interface PropertyUpdate
    {
        void updateIndexIfApplicable( KernelStatement state, NodeItem node, PrimitiveIntSet nodePropertyIds,
                NewIndexDescriptor index ) throws EntityNotFoundException;

        int propertyId();
    }

    public PropertyUpdate add( DefinedProperty after )
    {
        return new PropertyUpdate()
        {
            @Override
            public void updateIndexIfApplicable( KernelStatement state, NodeItem node,
                    PrimitiveIntSet nodePropertyIds, NewIndexDescriptor index ) throws EntityNotFoundException
            {
                OrderedPropertyValues values = valuesIfPropertiesMatch(
                        state, nodePropertyIds, index, node, after );
                if ( values != null )
                {
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), null, values );
                }
            }

            @Override
            public int propertyId()
            {
                return after.propertyKeyId();
            }
        };
    }

    public PropertyUpdate remove( DefinedProperty before )
    {
        return new PropertyUpdate()
        {
            @Override
            public void updateIndexIfApplicable( KernelStatement state, NodeItem node,
                    PrimitiveIntSet nodePropertyIds, NewIndexDescriptor index ) throws EntityNotFoundException
            {
                OrderedPropertyValues values = valuesIfPropertiesMatch( state, nodePropertyIds, index, node,
                        before );
                if ( values != null )
                {
                    state.txState().indexDoUpdateEntry( index.schema(), node.id(), values, null );
                }
            }

            @Override
            public int propertyId()
            {
                return before.propertyKeyId();
            }
        };
    }

    public PropertyUpdate change( DefinedProperty before, DefinedProperty after )
    {
        assert before.propertyKeyId() == after.propertyKeyId();
        return new PropertyUpdate()
        {
            @Override
            public void updateIndexIfApplicable( KernelStatement state, NodeItem node,
                    PrimitiveIntSet nodePropertyIds, NewIndexDescriptor index ) throws EntityNotFoundException
            {
                int[] indexPropertyIds = index.schema().getPropertyIds();
                if ( nodeHasIndexProperties( nodePropertyIds, indexPropertyIds ) )
                {
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
                            OrderedPropertyValues.of( valuesBefore ), OrderedPropertyValues.of( valuesAfter ) );
                }
            }

            @Override
            public int propertyId()
            {
                return before.propertyKeyId();
            }
        };
    }

    public void onPropertyChange( KernelStatement state, NodeItem node, PropertyUpdate update )
            throws EntityNotFoundException
    {
        PrimitiveIntSet nodePropertyIds = null;
        Iterator<NewIndexDescriptor> indexes = getIndexesForProperty( state, update.propertyId() );
        while ( indexes.hasNext() )
        {
            NewIndexDescriptor index = indexes.next();
            LabelSchemaDescriptor schema = index.schema();
            if ( node.labels().contains( schema.getLabelId() ) )
            {
                if ( nodePropertyIds == null )
                {
                    nodePropertyIds = Primitive.intSet();
                    nodePropertyIds.addAll( readOps.nodeGetPropertyKeys( state, node ).iterator() );
                }

                update.updateIndexIfApplicable( state, node, nodePropertyIds, index );
            }
        }
    }

    private OrderedPropertyValues valuesIfPropertiesMatch( KernelStatement state, PrimitiveIntSet nodeProperties,
            NewIndexDescriptor index, NodeItem node ) throws EntityNotFoundException
    {
        return valuesIfPropertiesMatch( state, nodeProperties, index, node, NO_SUCH_PROPERTY );
    }

    private OrderedPropertyValues valuesIfPropertiesMatch( KernelStatement state, PrimitiveIntSet
            nodeProperties,
            NewIndexDescriptor index, NodeItem node, DefinedProperty changedProperty ) throws EntityNotFoundException
    {
        int[] indexPropertyIds = index.schema().getPropertyIds();
        if ( !nodeHasIndexProperties( nodeProperties, indexPropertyIds, changedProperty.propertyKeyId() ) )
        {
            return null;
        }

        Object[] values = new Object[indexPropertyIds.length];
        for ( int i = 0; i < values.length; i++ )
        {
            int indexPropertyId = indexPropertyIds[i];
            values[i] = indexPropertyId == changedProperty.propertyKeyId() ?
                        changedProperty.value() : readOps.nodeGetProperty( state, node, indexPropertyId );
        }

        return OrderedPropertyValues.of( values );
    }

    private static boolean nodeHasIndexProperties( PrimitiveIntSet nodeProperties, int[] indexPropertyIds )
    {
        return nodeHasIndexProperties( nodeProperties, indexPropertyIds, NO_SUCH_PROPERTY_KEY );
    }

    private static boolean nodeHasIndexProperties(
            PrimitiveIntSet nodeProperties, int[] indexPropertyIds, int changedPropertyId )
    {
        for ( int indexPropertyId : indexPropertyIds )
        {
            if ( indexPropertyId != changedPropertyId && !nodeProperties.contains( indexPropertyId ) )
            {
                return false;
            }
        }
        return true;
    }

    private Iterator<NewIndexDescriptor> getIndexesForProperty( KernelStatement state, int propertyId )
    {
        Iterator<NewIndexDescriptor> allIndexes =
                Iterators.concat(
                        schemaReadOps.indexesGetAll( state ),
                        schemaReadOps.uniqueIndexesGetAll( state ) );

        return Iterators.filter( hasProperty( propertyId ), allIndexes );
    }

}
