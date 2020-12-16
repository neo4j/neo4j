/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Utility class that performs necessary updates for the transaction state.
 */
public class IndexTxStateUpdater
{
    private final StorageReader storageReader;
    private final Read read;
    private final IndexingService indexingService;

    // We can use the StorageReader directly instead of the SchemaReadOps, because we know that in transactions
    // where this class is needed we will never have index changes.
    public IndexTxStateUpdater( StorageReader storageReader, Read read, IndexingService indexingService )
    {
        this.storageReader = storageReader;
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
        Collection<IndexDescriptor> indexes = storageReader.indexesGetRelated( new long[]{labelId}, existingPropertyKeyIds, NODE );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            for ( IndexDescriptor index : indexes )
            {
                MemoryTracker memoryTracker = read.txState().memoryTracker();
                int[] indexPropertyIds = index.schema().getPropertyIds();
                Value[] values = getValueTuple( new NodeCursorWrapper( node ), propertyCursor, NO_SUCH_PROPERTY_KEY, NO_VALUE, indexPropertyIds,
                        materializedProperties, memoryTracker );
                ValueTuple valueTuple = ValueTuple.of( values );
                memoryTracker.allocateHeap( valueTuple.getShallowSize() );
                switch ( changeType )
                {
                case ADDED_LABEL:
                    indexingService.validateBeforeCommit( index, values, node.nodeReference() );
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), null, valueTuple );
                    break;
                case REMOVED_LABEL:
                    read.txState().indexDoUpdateEntry( index.schema(), node.nodeReference(), valueTuple, null );
                    break;
                default:
                    throw new IllegalStateException( changeType + " is not a supported event" );
                }
            }
        }
    }

    void onPropertyAdd( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds, Value value )
    {
        onPropertyAdd( new NodeCursorWrapper( node ), propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, value );
    }

    void onPropertyRemove( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds, Value value )
    {
        onPropertyRemove( new NodeCursorWrapper( node ), propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, value );
    }

    void onPropertyChange( NodeCursor node, PropertyCursor propertyCursor, long[] labels, int propertyKeyId, int[] existingPropertyKeyIds, Value beforeValue,
            Value afterValue )
    {
        onPropertyChange( new NodeCursorWrapper( node ), propertyCursor, labels, propertyKeyId, existingPropertyKeyIds, beforeValue, afterValue );
    }

    void onPropertyAdd( RelationshipScanCursor relationship, PropertyCursor propertyCursor, int type, int propertyKeyId, int[] existingPropertyKeyIds,
            Value value )
    {
        onPropertyAdd( new RelationshipCursorWrapper( relationship ), propertyCursor, new long[]{type}, propertyKeyId, existingPropertyKeyIds, value );
    }

    void onPropertyRemove( RelationshipScanCursor relationship, PropertyCursor propertyCursor, int type, int propertyKeyId, int[] existingPropertyKeyIds,
            Value value )
    {
        onPropertyRemove( new RelationshipCursorWrapper( relationship ), propertyCursor, new long[]{type}, propertyKeyId, existingPropertyKeyIds, value );
    }

    void onPropertyChange( RelationshipScanCursor relationship, PropertyCursor propertyCursor, int type, int propertyKeyId, int[] existingPropertyKeyIds,
            Value beforeValue, Value afterValue )
    {
        onPropertyChange( new RelationshipCursorWrapper( relationship ), propertyCursor, new long[]{type}, propertyKeyId, existingPropertyKeyIds, beforeValue,
                afterValue );
    }

    private boolean noSchemaChangedInTx()
    {
        return !(read.txState().hasChanges() && !read.txState().hasDataChanges());
    }

    //PROPERTY CHANGES

    private void onPropertyAdd( EntityCursor entity, PropertyCursor propertyCursor, long[] tokens, int propertyKeyId, int[] existingPropertyKeyIds,
            Value value )
    {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.indexesGetRelated( tokens, propertyKeyId, entity.entityType() );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        MemoryTracker memoryTracker = read.txState().memoryTracker();
                        SchemaDescriptor schema = index.schema();
                        Value[] values = getValueTuple( entity, propertyCursor, propertyKeyId, value, schema.getPropertyIds(), materializedProperties,
                                                        memoryTracker );
                        indexingService.validateBeforeCommit( index, values, entity.reference() );
                        ValueTuple valueTuple = ValueTuple.of( values );
                        memoryTracker.allocateHeap( valueTuple.getShallowSize() );
                        read.txState().indexDoUpdateEntry( schema, entity.reference(), null, valueTuple );
                    } );
        }
    }

    private void onPropertyRemove( EntityCursor entity, PropertyCursor propertyCursor, long[] tokens, int propertyKeyId, int[] existingPropertyKeyIds,
            Value value )
    {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.indexesGetRelated( tokens, propertyKeyId, entity.entityType() );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        MemoryTracker memoryTracker = read.txState().memoryTracker();
                        SchemaDescriptor schema = index.schema();
                        Value[] values = getValueTuple( entity, propertyCursor, propertyKeyId, value, schema.getPropertyIds(), materializedProperties,
                                                        memoryTracker );
                        ValueTuple valueTuple = ValueTuple.of( values );
                        memoryTracker.allocateHeap( valueTuple.getShallowSize() );
                        read.txState().indexDoUpdateEntry( schema, entity.reference(), valueTuple, null );
                    } );
        }
    }

    private void onPropertyChange( EntityCursor entity, PropertyCursor propertyCursor, long[] tokens, int propertyKeyId, int[] existingPropertyKeyIds,
            Value beforeValue, Value afterValue )
    {
        assert noSchemaChangedInTx();
        Collection<IndexDescriptor> indexes = storageReader.indexesGetRelated( tokens, propertyKeyId, entity.entityType() );
        if ( !indexes.isEmpty() )
        {
            MutableIntObjectMap<Value> materializedProperties = IntObjectMaps.mutable.empty();
            SchemaMatcher.onMatchingSchema( indexes.iterator(), propertyKeyId, existingPropertyKeyIds,
                    index ->
                    {
                        MemoryTracker memoryTracker = read.txState().memoryTracker();
                        SchemaDescriptor schema = index.schema();
                        int[] propertyIds = schema.getPropertyIds();
                        Value[] valuesAfter =
                                getValueTuple( entity, propertyCursor, propertyKeyId, afterValue, propertyIds, materializedProperties, memoryTracker );

                        // The valuesBefore tuple is just like valuesAfter, except is has the afterValue instead of the beforeValue
                        Value[] valuesBefore = Arrays.copyOf( valuesAfter, valuesAfter.length );
                        int k = ArrayUtils.indexOf( propertyIds, propertyKeyId );
                        valuesBefore[k] = beforeValue;

                        indexingService.validateBeforeCommit( index, valuesAfter, entity.reference() );
                        ValueTuple valuesTupleBefore = ValueTuple.of( valuesBefore );
                        ValueTuple valuesTupleAfter = ValueTuple.of( valuesAfter );
                        memoryTracker.allocateHeap( valuesTupleBefore.getShallowSize() * 2 ); // They are copies and same shallow size
                        read.txState().indexDoUpdateEntry( schema, entity.reference(), valuesTupleBefore, valuesTupleAfter );
                    } );
        }
    }

    private Value[] getValueTuple( EntityCursor entity, PropertyCursor propertyCursor, int changedPropertyKeyId, Value changedValue, int[] indexPropertyIds,
            MutableIntObjectMap<Value> materializedValues, MemoryTracker memoryTracker )
    {
        Value[] values = new Value[indexPropertyIds.length];
        int missing = 0;

        // First get whatever values we already have on the stack, like the value change that provoked this update in the first place
        // and already loaded values that we can get from the map of materialized values.
        for ( int k = 0; k < indexPropertyIds.length; k++ )
        {
            values[k] = indexPropertyIds[k] == changedPropertyKeyId ? changedValue : materializedValues.getIfAbsent( indexPropertyIds[k], () -> NO_VALUE );
            if ( values[k] == NO_VALUE )
            {
                missing++;
            }
        }

        // If we couldn't get all values that we wanted we need to load from the entity. While we're loading values
        // we'll place those values in the map so that other index updates from this change can just used them.
        if ( missing > 0 )
        {
            entity.properties( propertyCursor );
            while ( missing > 0 && propertyCursor.next() )
            {
                int k = ArrayUtils.indexOf( indexPropertyIds, propertyCursor.propertyKey() );
                if ( k >= 0 && values[k] == NO_VALUE )
                {
                    int propertyKeyId = indexPropertyIds[k];
                    boolean thisIsTheChangedProperty = propertyKeyId == changedPropertyKeyId;
                    values[k] = thisIsTheChangedProperty ? changedValue : propertyCursor.propertyValue();
                    if ( !thisIsTheChangedProperty )
                    {
                        materializedValues.put( propertyKeyId, values[k] );
                        memoryTracker.allocateHeap( values[k].estimatedHeapUsage() );
                    }
                    missing--;
                }
            }
        }

        return values;
    }

    /**
     * A common interface for operations needed from both node and relationship cursors.
     */
    private interface EntityCursor
    {
        long reference();

        void properties( PropertyCursor cursor );

        EntityType entityType();
    }

    private static class NodeCursorWrapper implements EntityCursor
    {

        private final NodeCursor node;

        private NodeCursorWrapper( NodeCursor node )
        {
            this.node = node;
        }

        @Override
        public long reference()
        {
            return node.nodeReference();
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
            node.properties( cursor );
        }

        @Override
        public EntityType entityType()
        {
            return NODE;
        }
    }

    private static class RelationshipCursorWrapper implements EntityCursor
    {

        private final RelationshipScanCursor relationship;

        private RelationshipCursorWrapper( RelationshipScanCursor relationship )
        {
            this.relationship = relationship;
        }

        @Override
        public long reference()
        {
            return relationship.relationshipReference();
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
            relationship.properties( cursor );
        }

        @Override
        public EntityType entityType()
        {
            return RELATIONSHIP;
        }
    }
}
