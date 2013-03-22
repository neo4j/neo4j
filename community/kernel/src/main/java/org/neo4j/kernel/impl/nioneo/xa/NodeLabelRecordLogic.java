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
package org.neo4j.kernel.impl.nioneo.xa;

import static java.lang.Long.highestOneBit;
import static java.lang.System.arraycopy;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.Bits.bits;
import static org.neo4j.kernel.impl.util.Bits.bitsFromLongs;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Logic for parsing and constructing {@link NodeRecord#getLabelField()} and dynamic label
 * records in {@link NodeRecord#getDynamicLabelRecords()} from label ids.
 * 
 * Each node has a label field of 5 bytes, where labels will be stored, if sufficient space
 * (max bits required for storing each label id is considered). If not then the field will
 * point to a dynamic record where the labels will be stored in the format of an array property.
 * 
 * [hhhh,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb]
 * h: header
 *    - 0x0<=h<=0x7 (leaving high bit reserved): number of in-lined labels in the body
 *    - 0x8: body will be a pointer to first dynamic record in node-labels dynamic store
 * b: body
 *    - 0x0<=h<=0x7 (leaving high bit reserved): bits of this many in-lined label ids
 *    - 0x8: pointer to node-labels store
 */
public class NodeLabelRecordLogic
{
    private static final int LABEL_BITS = 36;
    private final NodeRecord node;
    private final NodeStore nodeStore;

    public NodeLabelRecordLogic( NodeRecord node, NodeStore nodeStore )
    {
        this.node = node;
        this.nodeStore = nodeStore;
    }

    public Iterable<DynamicRecord> add( long labelId )
    {
        long existingLabelsField = node.getLabelField();
        byte header = getHeader( existingLabelsField );
        long existingLabelsBits = parseLabelsBody( existingLabelsField );
        Collection<DynamicRecord> changedDynamicRecords = Collections.emptyList();
        if ( !highHeaderBitSet( header ) )
        {   // There's in-lined or no labels
            long[] newLabelIds = header == 0 ?
                    new long[] {labelId} : concatAndSort( parseInlined( existingLabelsBits, header ), labelId );
            if ( !tryInline( newLabelIds, changedDynamicRecords ) )
            {
                changedDynamicRecords = nodeStore.allocateRecordsForDynamicLabels( newLabelIds );
                node.setLabelField( dynamicPointer( changedDynamicRecords ), changedDynamicRecords );
            }
        }
        else
        {   // The labels are in dynamic records
            nodeStore.ensureHeavy( node, existingLabelsBits );
            Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
            long[] existingLabelIds = nodeStore.getDynamicLabelsArray( existingRecords );
            long[] newLabelIds = concatAndSort( existingLabelIds, labelId );
            changedDynamicRecords = nodeStore.allocateRecordsForDynamicLabels( newLabelIds, existingRecords.iterator() );
            node.setLabelField( dynamicPointer( changedDynamicRecords ), changedDynamicRecords );
        }
        return changedDynamicRecords;
    }

    private void assertNotContains( long[] existingLabels, long labelId )
    {
        if ( Arrays.binarySearch( existingLabels, labelId ) >= 0 )
            throw new IllegalStateException( "Label " + labelId + " already exists on " + node );
    }

    public static boolean highHeaderBitSet( byte header )
    {
        return (header & 0x8) != 0;
    }

    public Iterable<DynamicRecord> remove( long labelId )
    {
        long existingLabelsField = node.getLabelField();
        byte header = getHeader( existingLabelsField );
        long existingLabelsBits = parseLabelsBody( existingLabelsField );
        Collection<DynamicRecord> changedDynamicRecords = Collections.emptyList();
        if ( header > 0 && !highHeaderBitSet( header ) )
        {   // There's in-lined labels
            long[] newLabelIds = filter( parseInlined( existingLabelsBits, header ), labelId );
            boolean inlined = tryInline( newLabelIds, changedDynamicRecords );
            assert inlined;
        }
        else
        {   // The labels are in dynamic records
            nodeStore.ensureHeavy( node, existingLabelsBits );
            Collection<DynamicRecord> existingRecords = node.getDynamicLabelRecords();
            long[] existingLabelIds = nodeStore.getDynamicLabelsArray( existingRecords );
            long[] newLabelIds = filter( existingLabelIds, labelId );
            if ( tryInline( newLabelIds, existingRecords ) )
            {
                for ( DynamicRecord record : existingRecords )
                    record.setInUse( false );
            }
            else
            {
                Collection<DynamicRecord> newRecords = nodeStore.allocateRecordsForDynamicLabels( newLabelIds,
                        existingRecords.iterator() );
                node.setLabelField( dynamicPointer( newRecords ), existingRecords );
                if ( !newRecords.equals( existingRecords ) )
                {   // One less dynamic record, mark that one as not in use
                    for ( DynamicRecord record : existingRecords )
                        if ( !newRecords.contains( record ) )
                            record.setInUse( false );
                }
            }
            changedDynamicRecords = existingRecords;
        }
        return changedDynamicRecords;
    }

    public static long parseLabelsBody( long labelsField )
    {
        return labelsField & 0xFFFFFFFFFL;
    }

    public static long dynamicPointer( Collection<DynamicRecord> newRecords )
    {
        return 0x8000000000L | first( newRecords ).getId();
    }

    private long[] filter( long[] ids, long excludeId )
    {
        boolean found = false;
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( ids[i] == excludeId )
            {
                found = true;
                break;
            }
        }
        if ( !found )
            throw new IllegalStateException( "Label " + excludeId + " not found on " + node );
        
        long[] result = new long[ids.length-1];
        int writerIndex = 0;
        for ( int i = 0; i < ids.length; i++ )
            if ( ids[i] != excludeId )
                result[writerIndex++] = ids[i];
        return result;
    }

    private boolean tryInline( long[] ids, Collection<DynamicRecord> changedDynamicRecords )
    {
        // We reserve the high header bit for future extensions of the format of the in-lined label bits
        // i.e. the 0-valued high header bit can allow for 0-7 in-lined labels in the bit-packed format.
        if ( ids.length > 7 )
            return false;
        
        byte bitsPerLabel = (byte) (ids.length > 0 ? (LABEL_BITS/ids.length) : LABEL_BITS);
        long limit = 1 << bitsPerLabel;
        Bits bits = bits( 5 );
        for ( long id : ids )
        {
            if ( highestOneBit( id ) < limit )
                bits.put( id, bitsPerLabel );
            else
                return false;
        }
        node.setLabelField( putHeader( bits.getLongs()[0], (byte) ids.length ), changedDynamicRecords );
        return true;
    }

    private long[] concatAndSort( long[] existing, long additional )
    {
        assertNotContains( existing, additional );
        
        long[] result = new long[existing.length+1];
        arraycopy( existing, 0, result, 0, existing.length );
        result[existing.length] = additional;
        Arrays.sort( result );
        return result;
    }

    public static long[] parseInlined( long existingLabelsField, byte numberOfLabels )
    {
        if ( numberOfLabels == 0 )
            return new long[0];
        
        byte bitsPerLabel = (byte) (LABEL_BITS/numberOfLabels);
        Bits bits = bitsFromLongs( new long[] { existingLabelsField } );
        long[] result = new long[numberOfLabels];
        for ( int i = 0; i < result.length; i++ )
            result[i] = bits.getLong( bitsPerLabel );
        return result;
    }

    public static byte getHeader( long labelField )
    {
        return (byte) ((labelField & 0xF000000000L) >>> 36);
    }
    
    public static long putHeader( long labelBits, byte numberOfLabels )
    {
        return (((long)numberOfLabels << 36) | labelBits);
    }
}
