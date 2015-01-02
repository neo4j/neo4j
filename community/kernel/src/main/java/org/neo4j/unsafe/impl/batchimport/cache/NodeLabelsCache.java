/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.kernel.impl.util.Bits;

import static java.lang.Integer.numberOfLeadingZeros;

import static org.neo4j.kernel.impl.util.Bits.bitsFromLongs;

/**
 * Caches labels for each node. Tries to keep memory as 8b (a long) per node. If a particular node has many labels
 * it will spill over into two or more longs in a separate array.
 */
public class NodeLabelsCache
{
    private final LongArray cache;
    private final LongArray spillOver;
    private long spillOverIndex;
    private final int bitsPerLabel;

    private final long[] labelScratch;
    private final Bits labelBits;
    private final long[] fieldScratch = new long[1];
    private final Bits fieldBits = bitsFromLongs( fieldScratch );

    public NodeLabelsCache( NumberArrayFactory cacheFactory, int highLabelId )
    {
        this( cacheFactory, highLabelId, 10_000_000 );
    }

    public NodeLabelsCache( NumberArrayFactory cacheFactory, int highLabelId, int chunkSize )
    {
        this.cache = cacheFactory.newDynamicLongArray( chunkSize, 0 );
        this.spillOver = cacheFactory.newDynamicLongArray( chunkSize / 5, 0 ); // expect way less of these
        this.bitsPerLabel = Integer.SIZE-numberOfLeadingZeros( highLabelId );

        int worstCaseLongsNeeded = ((bitsPerLabel * (highLabelId+1 /*length slot*/)) - 1) / Long.SIZE + 1;
        this.labelScratch = new long[worstCaseLongsNeeded];
        this.labelBits = bitsFromLongs( labelScratch );
    }

    /**
     * Keeps label ids for the given node id. Labels ids are int[] really, but by accident they arrive
     * from the store disguised as long[]. When looping over them there can be assumed that they are ints.
     *
     * The format is that the longs in this cache are divided up into bit slots of size whatever bitsPerLabel is.
     * The first slot will contain number of labels for this node. If those labels fit in the long, after the
     * length slot, they will be stored there. Otherwise the rest of the bits will point to the index into
     * the spillOver array.
     */
    public void put( long nodeId, long[] labelIds )
    {
        labelBits.clear( true );
        labelBits.put( labelIds.length, bitsPerLabel );
        for ( long labelId : labelIds )
        {
            labelBits.put( (int) labelId, bitsPerLabel );
        }

        int longsInUse = labelBits.longsInUse();
        assert longsInUse > 0 : "Uhm";
        if ( longsInUse == 1 )
        {   // We only require one long, so put it right in there
            cache.set( nodeId, labelScratch[0] );
        }
        else
        {   // Now it gets tricky, we have to spill over into another array
            // So create the reference
            fieldBits.clear( true );
            fieldBits.put( labelIds.length, bitsPerLabel );
            fieldBits.put( spillOverIndex, Long.SIZE - bitsPerLabel );
            cache.set( nodeId, fieldBits.getLongs()[0] );

            // And set the longs in the spill over array. For simplicity we put the encoded bits as they
            // are right into the spill over array, where the first slot will have the length "again".
            for ( int i = 0; i < longsInUse; i++ )
            {
                spillOver.set( spillOverIndex++, labelScratch[i] );
            }
        }
    }

    /**
     * Write labels for a node into {@code target}. If target isn't big enough it will grow.
     * The target, intact or grown, will be returned.
     */
    public int[] get( long nodeId, int[] target )
    {
        // make this field available to our Bits instance, hackish? meh
        fieldBits.clear( false );
        fieldScratch[0] = cache.get( nodeId );
        if ( fieldScratch[0] == 0 )
        {   // Nothing here
            target[0] = -1; // mark the end
            return target;
        }

        int length = fieldBits.getInt( bitsPerLabel );
        int longsInUse = ((bitsPerLabel * (length+1))-1) / Long.SIZE + 1;
        target = ensureCapacity( target, length );
        if ( longsInUse == 1 )
        {
            decode( fieldBits, length, target );
        }
        else
        {
            // Read data from spill over cache into the label bits array for decoding
            long spillOverIndex = fieldBits.getLong( Long.SIZE - bitsPerLabel );
            labelBits.clear( false );
            for ( int i = 0; i < longsInUse; i++ )
            {
                labelScratch[i] = spillOver.get( spillOverIndex + i );
            }
            labelBits.getInt( bitsPerLabel ); // first one ignored, since it's just the length
            decode( labelBits, length, target );
        }

        return target;
    }

    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        cache.visitMemoryStats( visitor );
        spillOver.visitMemoryStats( visitor );
    }

    private void decode( Bits bits, int length, int[] target )
    {
        for ( int i = 0; i < length; i++ )
        {
            target[i] = bits.getInt( bitsPerLabel );
        }

        if ( target.length > length )
        {   // we have to mark the end here, since the target array is larger
            target[length] = -1;
        }
    }

    private static int[] ensureCapacity( int[] target, int capacity )
    {
        return capacity > target.length
                ? new int[capacity]
                : target;
    }

    public void close()
    {
        cache.close();
        spillOver.close();
    }
}
