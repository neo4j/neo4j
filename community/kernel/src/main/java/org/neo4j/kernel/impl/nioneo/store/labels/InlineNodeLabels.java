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
package org.neo4j.kernel.impl.nioneo.store.labels;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.util.Bits;

import static java.lang.Long.highestOneBit;

import static org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray.concatAndSort;
import static org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray.filter;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsBody;
import static org.neo4j.kernel.impl.util.Bits.bits;
import static org.neo4j.kernel.impl.util.Bits.bitsFromLongs;

public class InlineNodeLabels implements NodeLabels
{
    private static final int LABEL_BITS = 36;
    private final long labelField;
    private final NodeRecord node;

    public InlineNodeLabels( long labelField, NodeRecord node )
    {
        this.labelField = labelField;
        this.node = node;
    }

    @Override
    public long[] get( NodeStore nodeStore )
    {
        return parseInlined( labelField );
    }

    @Override
    public Collection<DynamicRecord> put( long[] labelIds, NodeStore nodeStore )
    {
        if ( tryInlineInNodeRecord( labelIds, Collections.<DynamicRecord>emptyList() ) )
        {
            return Collections.emptyList();
        }
        else
        {
            return new DynamicNodeLabels( 0, node ).put( labelIds, nodeStore );
        }
    }

    @Override
    public Collection<DynamicRecord> add( long labelId, NodeStore nodeStore )
    {
        long[] augmentedLabelIds = labelCount( labelField ) == 0 ? new long[]{labelId} :
                concatAndSort( parseInlined( labelField ), labelId );

        return put( augmentedLabelIds, nodeStore );
    }

    @Override
    public Collection<DynamicRecord> remove( long labelId, NodeStore nodeStore )
    {
        long[] newLabelIds = filter( parseInlined( labelField ), labelId );
        boolean inlined = tryInlineInNodeRecord( newLabelIds, Collections.<DynamicRecord>emptyList() );
        assert inlined;
        return Collections.emptyList();
    }

    @Override
    public void ensureHeavy( NodeStore nodeStore )
    {
        // no dynamic records
    }

    boolean tryInlineInNodeRecord( long[] ids, Collection<DynamicRecord> changedDynamicRecords )
    {
        // We reserve the high header bit for future extensions of the format of the in-lined label bits
        // i.e. the 0-valued high header bit can allow for 0-7 in-lined labels in the bit-packed format.
        if ( ids.length > 7 )
        {
            return false;
        }

        byte bitsPerLabel = (byte) (ids.length > 0 ? (LABEL_BITS / ids.length) : LABEL_BITS);
        long limit = 1 << bitsPerLabel;
        Bits bits = bits( 5 );
        for ( long id : ids )
        {
            if ( highestOneBit( id ) < limit )
            {
                bits.put( id, bitsPerLabel );
            }
            else
            {
                return false;
            }
        }
        node.setLabelField( combineLabelCountAndLabelStorage( (byte) ids.length, bits.getLongs()[0] ), changedDynamicRecords );
        return true;
    }

    private static long[] parseInlined( long labelField )
    {
        byte numberOfLabels = labelCount( labelField );
        if ( numberOfLabels == 0 )
        {
            return new long[0];
        }

        long existingLabelsField = parseLabelsBody( labelField );
        byte bitsPerLabel = (byte) (LABEL_BITS / numberOfLabels);
        Bits bits = bitsFromLongs( new long[]{existingLabelsField} );
        long[] result = new long[numberOfLabels];
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = bits.getLong( bitsPerLabel );
        }
        return result;
    }

    private static long combineLabelCountAndLabelStorage( byte labelCount, long labelBits )
    {
        return (((long) labelCount << 36) | labelBits);
    }

    private static byte labelCount( long labelField )
    {
        return (byte) ((labelField & 0xF000000000L) >>> 36);
    }
}
