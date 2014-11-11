/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.Arrays;

/**
 * A {@code long[]} on heap, abstracted into a {@link LongArray}.
 */
public class HeapLongArray implements LongArray
{
    private final long[][] shards;
    private final long length;
    private final long defaultValue;
    private long highestSetIndex = -1;
    private long size;

    public HeapLongArray( long length, long defaultValue )
    {
        this.length = length;
        this.defaultValue = defaultValue;
        int numShards = (int) ((length-1) / Integer.MAX_VALUE) + 1;
        this.shards = new long[numShards][];
        for ( int i = 0; i < numShards-1; i++ )
        {
            this.shards[i] = newShard( Integer.MAX_VALUE );
        }
        this.shards[numShards-1] = newShard( (int) (length % Integer.MAX_VALUE) );
    }

    private long[] newShard( int length )
    {
        long[] shard = new long[length];
        if ( defaultValue != 0 )
        {
            Arrays.fill( shard, defaultValue );
        }
        return shard;
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public long get( long index )
    {
        return shard( index )[arrayIndex( index )];
    }

    @Override
    public void set( long index, long value )
    {
        long[] shard = shard( index );
        int arrayIndex = arrayIndex( index );
        if ( shard[arrayIndex] == defaultValue )
        {
            size++;
        }
        shard[arrayIndex] = value;
        if ( index > highestSetIndex )
        {
            highestSetIndex = index;
        }
    }

    @Override
    public long highestSetIndex()
    {
        return highestSetIndex;
    }

    @Override
    public long size()
    {
        return size;
    }

    private long[] shard( long index )
    {
        return shards[shardIndex( index )];
    }

    @Override
    public void clear()
    {
        for ( long[] shard : shards )
        {
            Arrays.fill( shard, defaultValue );
        }
        highestSetIndex = -1;
        size = 0;
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            long fromValue = get( fromIndex+i );
            long toValue = get( toIndex+i );
            set( fromIndex+i, toValue );
            set( toIndex+i, fromValue );
        }
    }

    private int arrayIndex( long index )
    {
        return index < Integer.MAX_VALUE ? (int) index : (int) (index % Integer.MAX_VALUE);
    }

    private int shardIndex( long index )
    {
        return index < Integer.MAX_VALUE ? 0 : (int) (index / Integer.MAX_VALUE);
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        visitor.heapUsage( length*8 ); // roughly
    }
}
