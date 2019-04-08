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
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.util.Collection;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Utility class that performs necessary updates for the transaction state.
 */
public class IndexTxStateUpdater
{
    private final Read read;
    private final IndexingService indexingService;

    public IndexTxStateUpdater( Read read, IndexingService indexingService )
    {
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
     * @param existingPropertyKeyIds all property key ids the node has, sorted by id
     * @param node cursor to the node where the change was applied
     * @param propertyCursor cursor to the properties of node
     * @param changeType The type of change event
     */
    void onLabelChange( int labelId, int[] existingPropertyKeyIds, NodeCursor node, PropertyCursor propertyCursor, LabelChangeType changeType )
    {
        assert noSchemaChangedInTx();

        // Check all indexes of the changed label
        Collection<SchemaDescriptor> indexes = indexingService.getRelatedIndexes( new long[]{labelId}, existingPropertyKeyIds, NODE );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            for ( SchemaDescriptor index : indexes )
            {
                int[] indexPropertyIds = index.schema().getPropertyIds();
                Value[] values = getValueTuple( node, propertyCursor, NO_SUCH_PROPERTY_KEY, NO_VALUE, indexPropertyIds, materializedProperties );
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

    void onPropertyAdd( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds, Value value )
    {
        assert noSchemaChangedInTx();
        Collection<SchemaDescriptor> indexes = indexingService.getRelatedIndexes( labels, propertyKeyId, NODE );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            NodeSchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        Value[] values = getValueTuple( node, propertyCursor, propertyKeyId, value, index.schema().getPropertyIds(), materializedProperties );
                        indexingService.validateBeforeCommit( index.schema(), values );
                        read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, ValueTuple.of( values ) );
                    } );
        }
    }

    void onPropertyRemove( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds, Value value )
    {
        assert noSchemaChangedInTx();
        Collection<SchemaDescriptor> indexes = indexingService.getRelatedIndexes( labels, propertyKeyId, NODE );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            NodeSchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        Value[] values = getValueTuple( node, propertyCursor, propertyKeyId, value, index.schema().getPropertyIds(), materializedProperties );
                        read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), ValueTuple.of( values ), null );
                    } );
        }
    }

    void onPropertyChange( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds,
            Value beforeValue, Value afterValue )
    {
        assert noSchemaChangedInTx();
        Collection<SchemaDescriptor> indexes = indexingService.getRelatedIndexes( labels, propertyKeyId, NODE );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            NodeSchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        int[] propertyIds = index.getPropertyIds();
                        Value[] valuesAfter = getValueTuple( node, propertyCursor, propertyKeyId, afterValue, propertyIds, materializedProperties );

                        // The valuesBefore tuple is just like valuesAfter, except is has the afterValue instead of the beforeValue
                        Value[] valuesBefore = valuesAfter.clone();
                        int k = ArrayUtils.indexOf( propertyIds, propertyKeyId );
                        valuesBefore[k] = beforeValue;

                        indexingService.validateBeforeCommit( index, valuesAfter );
                        read.txState().indexDoUpdateEntry( index, node.nodeReference(),
                                ValueTuple.of( valuesBefore ), ValueTuple.of( valuesAfter ) );
                    } );
        }
    }

    private Value[] getValueTuple( NodeCursor node, PropertyCursor propertyCursor,
            int changedPropertyKeyId, Value changedValue, int[] indexPropertyIds,
            MutableIntObjectMap<Value> materializedValues )
    {
        Value[] values = new Value[indexPropertyIds.length];
        int missing = 0;

        // First get whatever values we already have on the stack, like the value change that provoked this update in the first place
        // and already loaded values that we can get from the map of materialized values.
        for ( int k = 0; k < indexPropertyIds.length; k++ )
        {
            values[k] = indexPropertyIds[k] == changedPropertyKeyId ? changedValue : materializedValues.get( indexPropertyIds[k] );
            if ( values[k] == null )
            {
                missing++;
            }
        }

        // If we couldn't get all values that we wanted we need to load from the node. While we're loading values
        // we'll place those values in the map so that other index updates from this change can just used them.
        if ( missing > 0 )
        {
            node.properties( propertyCursor );
            while ( missing > 0 && propertyCursor.next() )
            {
                int k = ArrayUtils.indexOf( indexPropertyIds, propertyCursor.propertyKey() );
                if ( k >= 0 && values[k] == null )
                {
                    int propertyKeyId = indexPropertyIds[k];
                    boolean thisIsTheChangedProperty = propertyKeyId == changedPropertyKeyId;
                    values[k] = thisIsTheChangedProperty ? changedValue : propertyCursor.propertyValue();
                    if ( !thisIsTheChangedProperty )
                    {
                        materializedValues.put( propertyKeyId, values[k] );
                    }
                    missing--;
                }
            }
        }

        return values;
    }
}
