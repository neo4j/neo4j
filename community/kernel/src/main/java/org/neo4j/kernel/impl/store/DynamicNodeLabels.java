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
package org.neo4j.kernel.impl.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;
import static org.neo4j.kernel.impl.store.LabelIdArray.filter;
import static org.neo4j.kernel.impl.store.LabelIdArray.stripNodeId;
import static org.neo4j.kernel.impl.store.NodeLabelsField.fieldPointsToDynamicRecordOfLabels;
import static org.neo4j.kernel.impl.store.NodeLabelsField.firstDynamicLabelRecordId;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsBody;
import static org.neo4j.kernel.impl.store.NodeStore.getDynamicLabelsArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;

public class DynamicNodeLabels implements NodeLabels
{
    private final long labelField;
    private final NodeRecord node;

    public DynamicNodeLabels( long labelField, NodeRecord node )
    {
        this.labelField = labelField;
        this.node = node;
    }

    @Override
    public long[] get( NodeStore nodeStore )
    {
        return get( node, nodeStore );
    }

    public static long[] get( NodeRecord node, NodeStore nodeStore )
    {
        if ( node.isLight() )
        {
            nodeStore.ensureHeavy( node, firstDynamicLabelRecordId( node.getLabelField() ) );
        }
        return nodeStore.getDynamicLabelsArray( node.getUsedDynamicLabelRecords() );
    }

    @Override
    public long[] getIfLoaded()
    {
        if ( node.isLight() )
        {
            return null;
        }
        for ( DynamicRecord dynamic : node.getUsedDynamicLabelRecords() )
        {
            if ( dynamic.isLight() )
            {
                return null;
            }
        }
        return stripNodeId( (long[]) getRightArray( readFullByteArrayFromHeavyRecords(
                node.getUsedDynamicLabelRecords(), ARRAY ) ) );
    }

    @Override
    public Collection<DynamicRecord> put( long[] labelIds, NodeStore nodeStore, DynamicRecordAllocator allocator )
    {
        Arrays.sort( labelIds );
        return putSorted( node, labelIds, nodeStore, allocator );
    }

    public static Collection<DynamicRecord> putSorted( NodeRecord node, long[] labelIds, NodeStore nodeStore,
            DynamicRecordAllocator allocator )
    {
        long existingLabelsField = node.getLabelField();
        long existingLabelsBits = parseLabelsBody( existingLabelsField );

        Collection<DynamicRecord> changedDynamicRecords = node.getDynamicLabelRecords();

        long labelField = node.getLabelField();
        if ( fieldPointsToDynamicRecordOfLabels( labelField ) )
        {
            // There are existing dynamic label records, get them
            nodeStore.ensureHeavy( node, existingLabelsBits );
            changedDynamicRecords = node.getDynamicLabelRecords();
            setNotInUse( changedDynamicRecords );
        }

        if ( !InlineNodeLabels.tryInlineInNodeRecord( node, labelIds, changedDynamicRecords ) )
        {
            Iterator<DynamicRecord> recycledRecords = changedDynamicRecords.iterator();
            Collection<DynamicRecord> allocatedRecords =
                    NodeStore.allocateRecordsForDynamicLabels( node.getId(), labelIds,
                            recycledRecords, allocator );
            // Set the rest of the previously set dynamic records as !inUse
            while ( recycledRecords.hasNext() )
            {
                DynamicRecord removedRecord = recycledRecords.next();
                removedRecord.setInUse( false );
                allocatedRecords.add( removedRecord );
            }
            node.setLabelField( dynamicPointer( allocatedRecords ), allocatedRecords );
            changedDynamicRecords = allocatedRecords;
        }

        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> add( long labelId, NodeStore nodeStore, DynamicRecordAllocator allocator )
    {
        nodeStore.ensureHeavy( node, firstDynamicLabelRecordId( labelField ) );
        Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        long[] existingLabelIds = nodeStore.getDynamicLabelsArray( existingRecords );
        long[] newLabelIds = LabelIdArray.concatAndSort( existingLabelIds, labelId );
        Collection<DynamicRecord> changedDynamicRecords =
                NodeStore.allocateRecordsForDynamicLabels( node.getId(), newLabelIds, existingRecords.iterator(), allocator );
        node.setLabelField( dynamicPointer( changedDynamicRecords ), changedDynamicRecords );
        return changedDynamicRecords;
    }

    @Override
    public Collection<DynamicRecord> remove( long labelId, NodeStore nodeStore )
    {
        nodeStore.ensureHeavy( node, firstDynamicLabelRecordId( labelField ) );
        Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
        long[] existingLabelIds = nodeStore.getDynamicLabelsArray( existingRecords );
        long[] newLabelIds = filter( existingLabelIds, labelId );
        if ( InlineNodeLabels.tryInlineInNodeRecord( node, newLabelIds, existingRecords ) )
        {
            setNotInUse( existingRecords );
        }
        else
        {
            Collection<DynamicRecord> newRecords =
                    nodeStore.allocateRecordsForDynamicLabels( node.getId(), newLabelIds, existingRecords.iterator() );
            node.setLabelField( dynamicPointer( newRecords ), existingRecords );
            if ( !newRecords.equals( existingRecords ) )
            {   // One less dynamic record, mark that one as not in use
                for ( DynamicRecord record : existingRecords )
                {
                    if ( !newRecords.contains( record ) )
                    {
                        record.setInUse( false );
                        record.setLength( 0 ); // so that it will not be made heavy again...
                    }
                }
            }
        }
        return existingRecords;
    }

    public long getFirstDynamicRecordId()
    {
        return firstDynamicLabelRecordId( labelField );
    }

    public static long dynamicPointer( Collection<DynamicRecord> newRecords )
    {
        return 0x8000000000L | first( newRecords ).getId();
    }

    private static void setNotInUse( Collection<DynamicRecord> changedDynamicRecords )
    {
        for ( DynamicRecord record : changedDynamicRecords )
        {
            record.setInUse( false );
        }
    }

    @Override
    public boolean isInlined()
    {
        return false;
    }

    @Override
    public String toString()
    {
        if ( node.isLight() )
        {
            return format( "Dynamic(id:%d)", firstDynamicLabelRecordId( node.getLabelField() ) );
        }
        return format( "Dynamic(id:%d,[%s])", firstDynamicLabelRecordId( node.getLabelField() ),
                Arrays.toString( getDynamicLabelsArrayFromHeavyRecords( node.getUsedDynamicLabelRecords() ) ) );
    }
}
