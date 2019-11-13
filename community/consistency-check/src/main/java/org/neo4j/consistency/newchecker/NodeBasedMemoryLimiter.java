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
package org.neo4j.consistency.newchecker;

import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.io.os.OsBeanUtil;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;

public class NodeBasedMemoryLimiter extends PrefetchingIterator<LongRange>
{
    public interface Factory
    {
        NodeBasedMemoryLimiter create( long pageCacheMemory, long highNodeId );
    }

    public static Factory DEFAULT = ( pageCacheMemory, highNodeId ) ->
    {
        long jvmMemory = Runtime.getRuntime().maxMemory();
        long machineMemory = OsBeanUtil.getTotalPhysicalMemory();
        long perNodeMemory = CacheSlots.CACHE_LINE_SIZE_BYTES;
        return new NodeBasedMemoryLimiter( pageCacheMemory, jvmMemory, machineMemory, perNodeMemory, highNodeId );
    };

    // Original parameters
    private final long pageCacheMemory;
    private final long jvmMemory;
    private final long machineMemory;
    private final long requiredMemoryPerNode;

    // Calculated values
    private final long effectiveJvmMemory;
    private final long occupiedMemory;
    private long effectiveMachineMemory;

    private final long highNodeId;
    private final long nodesPerRange;
    private long currentRangeStart;
    private long currentRangeEnd;

    public NodeBasedMemoryLimiter( long pageCacheMemory, long jvmMemory, long machineMemory, long requiredMemoryPerNode, long highNodeId )
    {
        // Store the original parameters so that they can be printed for reference later
        this.pageCacheMemory = pageCacheMemory;
        this.jvmMemory = jvmMemory;
        this.machineMemory = machineMemory;
        this.requiredMemoryPerNode = requiredMemoryPerNode;

        // Store calculated values
        this.effectiveJvmMemory = jvmMemory == Long.MAX_VALUE ? Runtime.getRuntime().totalMemory() : jvmMemory;
        this.occupiedMemory = pageCacheMemory + effectiveJvmMemory;
        this.effectiveMachineMemory = machineMemory == VALUE_UNAVAILABLE
                                      // When the OS can't provide a number, we assume at least twice page-cache size, and at least 2GiB
                                      ? max( pageCacheMemory * 2, gibiBytes( 2 ) )
                                      : machineMemory;
        this.effectiveMachineMemory = max( effectiveMachineMemory, occupiedMemory );
        long availableMemory = effectiveMachineMemory - occupiedMemory;

        assert availableMemory > 0;
        assert requiredMemoryPerNode > 0;

        this.highNodeId = highNodeId;
        this.nodesPerRange = max( 1, min( highNodeId, availableMemory / requiredMemoryPerNode ) );
        this.currentRangeStart = 0;
        this.currentRangeEnd = min( this.highNodeId, nodesPerRange );
    }

    int numberOfRanges()
    {
        return toIntExact( (long) ((((double) highNodeId - 1) / nodesPerRange) + 1) );
    }

    long rangeSize()
    {
        return nodesPerRange;
    }

    @Override
    protected LongRange fetchNextOrNull()
    {
        if ( currentRangeStart >= highNodeId )
        {
            return null;
        }

        LongRange range = LongRange.range( currentRangeStart, currentRangeEnd );
        currentRangeStart = currentRangeEnd;
        currentRangeEnd = min( highNodeId, currentRangeEnd + nodesPerRange );
        return range;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( getClass().getSimpleName() + ":" );
        builder.append( format( "%n  pageCacheMemory:%s", bytesToString( pageCacheMemory ) ) );
        builder.append( format( "%n  jvmMemory:%s", bytesToString( jvmMemory ) ) );
        builder.append( format( "%n  machineMemory:%s", bytesToString( machineMemory ) ) );
        builder.append( format( "%n  perNodeMemory:%s", bytesToString( requiredMemoryPerNode ) ) );
        builder.append( format( "%n  nodeHighId:%s", highNodeId ) );
        if ( effectiveJvmMemory != jvmMemory )
        {
            builder.append( format( "%n  effective jvmMemory:%s", bytesToString( effectiveJvmMemory ) ) );
        }
        if ( effectiveMachineMemory != machineMemory )
        {
            builder.append( format( "%n  effective machineMemory:%s", bytesToString( effectiveMachineMemory ) ) );
        }
        builder.append( format( "%n  occupiedMemory:%s", bytesToString( occupiedMemory ) ) );
        builder.append( format( "%n  ==> numberOfRanges:%d", numberOfRanges() ) );
        builder.append( format( "%n  ==> numberOfNodesPerRange:%d", nodesPerRange ) );
        return builder.toString();
    }

    boolean isFirst( LongRange range )
    {
        return range.from() == 0;
    }

    boolean isLast( LongRange range )
    {
        return range.to() == highNodeId;
    }
}
