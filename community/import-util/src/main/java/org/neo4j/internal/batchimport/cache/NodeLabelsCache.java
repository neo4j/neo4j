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
package org.neo4j.internal.batchimport.cache;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Bits;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;

/**
 * Caches labels for each node. Tries to keep memory as 8b (a long) per node. If a particular node has many labels
 * it will spill over into two or more longs in a separate array.
 */
public class NodeLabelsCache implements MemoryStatsVisitor.Visitable, AutoCloseable
{
    public static class Client
    {
        private final long[] labelScratch;
        private final Bits labelBits;
        private final long[] fieldScratch = new long[1];
        private final Bits fieldBits = Bits.bitsFromLongs( fieldScratch );
        private long[] target = new long[20];

        public Client( int worstCaseLongsNeeded )
        {
            this.labelScratch = new long[worstCaseLongsNeeded];
            this.labelBits = Bits.bitsFromLongs( labelScratch );
        }
    }

    private final LongArray cache;
    private final LongArray spillOver;
    private final AtomicLong spillOverIndex = new AtomicLong();
    private final int bitsPerLabel;
    private final int worstCaseLongsNeeded;
    private final Client putClient;

    public NodeLabelsCache( NumberArrayFactory cacheFactory, long highNodeId, int highLabelId, MemoryTracker memoryTracker )
    {
        this( cacheFactory, highNodeId, highLabelId, 2_000_000, memoryTracker );
    }

    public NodeLabelsCache( NumberArrayFactory cacheFactory, long highNodeId, int highLabelId, int spillOverChunkSize, MemoryTracker memoryTracker )
    {
        this.cache = cacheFactory.newLongArray( highNodeId, 0, memoryTracker );
        this.spillOver = cacheFactory.newDynamicLongArray( spillOverChunkSize, 0, memoryTracker ); // expect way less of these
        this.bitsPerLabel = max( Integer.SIZE - numberOfLeadingZeros( highLabelId ), 1 );
        this.worstCaseLongsNeeded = ((bitsPerLabel * (highLabelId + 1 /*length slot*/)) - 1) / Long.SIZE + 1;
        this.putClient = new Client( worstCaseLongsNeeded );
    }

    /**
     * @return a new {@link Client} used in {@link #get(Client, long)}. {@link Client} contains
     * mutable state and so each thread calling {@link #get(Client, long)} must create their own
     * client instance once and (re)use it for every get-call they do.
     */
    public Client newClient()
    {
        return new Client( worstCaseLongsNeeded );
    }

    public void put( long nodeId, long[] labelIds )
    {
        put( putClient, nodeId, labelIds );
    }

    /**
     * Keeps label ids for the given node id. Labels ids are int[] really, but by accident they arrive
     * from the store disguised as long[]. When looping over them there can be assumed that they are ints.
     *
     * The format is that the longs in this cache are divided up into bit slots of size whatever bitsPerLabel is.
     * The first slot will contain number of labels for this node. If those labels fit in the long, after the
     * length slot, they will be stored there. Otherwise the rest of the bits will point to the index into
     * the spillOver array.
     *
     * This method may only be called by a single thread, putting from multiple threads may cause undeterministic
     * behaviour.
     */
    public void put( Client putClient, long nodeId, long[] labelIds )
    {
        putClient.labelBits.clear( true );
        putClient.labelBits.put( labelIds.length, bitsPerLabel );
        for ( long labelId : labelIds )
        {
            putClient.labelBits.put( (int) labelId, bitsPerLabel );
        }

        int longsInUse = putClient.labelBits.longsInUse();
        assert longsInUse > 0 : "Uhm";
        if ( longsInUse == 1 )
        {   // We only require one long, so put it right in there
            cache.set( nodeId, putClient.labelScratch[0] );
        }
        else
        {   // Now it gets tricky, we have to spill over into another array
            // So create the reference
            putClient.fieldBits.clear( true );
            putClient.fieldBits.put( labelIds.length, bitsPerLabel );
            long spillOverIndex = this.spillOverIndex.getAndAdd( longsInUse );
            putClient.fieldBits.put( spillOverIndex, Long.SIZE - bitsPerLabel );
            cache.set( nodeId, putClient.fieldBits.getLongs()[0] );

            // And set the longs in the spill over array. For simplicity we put the encoded bits as they
            // are right into the spill over array, where the first slot will have the length "again".
            for ( int i = 0; i < longsInUse; i++ )
            {
                spillOver.set( spillOverIndex++, putClient.labelScratch[i] );
            }
        }
    }

    /**
     * Write labels for a node into {@code target}. If target isn't big enough it will grow.
     * The target, intact or grown, will be returned.
     *
     * Multiple threads may call this method simultaneously, given that they do so with each their own {@link Client}
     * instance.
     */
    public long[] get( Client client, long nodeId )
    {
        // make this field available to our Bits instance, hackish? meh
        client.fieldBits.clear( false );
        client.fieldScratch[0] = cache.get( nodeId );
        if ( client.fieldScratch[0] == 0 )
        {   // Nothing here
            client.target[0] = -1; // mark the end
            return client.target;
        }

        int length = client.fieldBits.getInt( bitsPerLabel );
        int longsInUse = ((bitsPerLabel * (length + 1)) - 1) / Long.SIZE + 1;
        client.target = ensureCapacity( client.target, length );
        if ( longsInUse == 1 )
        {
            decode( client.fieldBits, length, client.target );
        }
        else
        {
            // Read data from spill over cache into the label bits array for decoding
            long spillOverIndex = client.fieldBits.getLong( Long.SIZE - bitsPerLabel );
            client.labelBits.clear( false );
            for ( int i = 0; i < longsInUse; i++ )
            {
                client.labelScratch[i] = spillOver.get( spillOverIndex + i );
            }
            client.labelBits.getInt( bitsPerLabel ); // first one ignored, since it's just the length
            decode( client.labelBits, length, client.target );
        }

        return client.target;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        cache.acceptMemoryStatsVisitor( visitor );
        spillOver.acceptMemoryStatsVisitor( visitor );
    }

    private void decode( Bits bits, int length, long[] target )
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

    private static long[] ensureCapacity( long[] target, int capacity )
    {
        return capacity > target.length
                ? new long[capacity]
                : target;
    }

    @Override
    public void close()
    {
        cache.close();
        spillOver.close();
    }
}
