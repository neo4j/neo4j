/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checker;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.bytesToString;

import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

/**
 * Even though this memory limiter handles ranges for both nodes and relationships, it bases the range size on the number of nodes
 * to not make relationship heavy stores allocate a lot more memory.
 */
public class EntityBasedMemoryLimiter extends PrefetchingIterator<EntityBasedMemoryLimiter.CheckRange> {
    public static Factory defaultMemoryLimiter(long maxOffHeapCachingMemory) {
        return new DefaultFactory(maxOffHeapCachingMemory);
    }

    // Original parameters
    private final long maxOffHeapCachingMemory;
    private final long requiredMemoryPerEntity;

    // Calculated values
    private final long highNodeId;
    private final long highRelationshipId;
    private final long highEntityId;
    private final long entitiesPerRange;
    private long currentRangeStart;
    private long currentRangeEnd;

    public EntityBasedMemoryLimiter(
            long maxOffHeapCachingMemory, long requiredMemoryPerEntity, long highNodeId, long highRelationshipId) {
        assert maxOffHeapCachingMemory > 0 : "Max off-heap caching memory is " + maxOffHeapCachingMemory;
        assert requiredMemoryPerEntity > 0 : "Required memory per entity is " + requiredMemoryPerEntity;
        this.maxOffHeapCachingMemory = maxOffHeapCachingMemory;
        this.requiredMemoryPerEntity = requiredMemoryPerEntity;
        this.highNodeId = highNodeId;
        this.highRelationshipId = highRelationshipId;
        this.highEntityId = max(highNodeId, highRelationshipId);
        this.entitiesPerRange = max(1, min(highNodeId, maxOffHeapCachingMemory / requiredMemoryPerEntity));
        this.currentRangeStart = 0;
        this.currentRangeEnd = min(this.highEntityId, entitiesPerRange);
    }

    int numberOfRanges() {
        return toIntExact((long) ((((double) highEntityId - 1) / entitiesPerRange) + 1));
    }

    int numberOfNodeRanges() {
        return toIntExact((long) ((((double) highNodeId - 1) / entitiesPerRange) + 1));
    }

    int numberOfRelationshipRanges() {
        return toIntExact((long) ((((double) highRelationshipId - 1) / entitiesPerRange) + 1));
    }

    long rangeSize() {
        return entitiesPerRange;
    }

    @Override
    protected CheckRange fetchNextOrNull() {
        if (currentRangeStart >= highEntityId) {
            return null;
        }

        CheckRange range = new CheckRange(currentRangeStart, currentRangeEnd, highNodeId, highRelationshipId);

        currentRangeStart = currentRangeEnd;
        currentRangeEnd = min(highEntityId, currentRangeEnd + entitiesPerRange);
        return range;
    }

    @Override
    public String toString() {
        StringBuilder builder =
                new StringBuilder().append(getClass().getSimpleName()).append(':');
        builder.append(format("%n  maxOffHeapCachingMemory:%s", bytesToString(maxOffHeapCachingMemory)));
        builder.append(format("%n  perEntityMemory:%s", bytesToString(requiredMemoryPerEntity)));
        builder.append(format("%n  nodeHighId:%s", highNodeId));
        builder.append(format("%n  relationshipHighId:%s", highRelationshipId));
        builder.append(format("%n  ==> numberOfRanges:%d", numberOfRanges()));
        builder.append(format("%n  ==> numberOfEntitiesPerRange:%d", entitiesPerRange));
        return builder.toString();
    }

    static boolean isFirst(LongRange range) {
        return range.from() == 0;
    }

    boolean isLast(LongRange range, boolean isNodeRange) {
        if (isNodeRange) {
            return range.to() == highNodeId;
        }
        return range.to() == highRelationshipId;
    }

    public interface Factory {
        EntityBasedMemoryLimiter create(long highNodeId, long highRelationshipId);
    }

    private static class DefaultFactory implements Factory {
        private final long maxOffHeapCachingMemory;

        DefaultFactory(long maxOffHeapCachingMemory) {
            this.maxOffHeapCachingMemory = maxOffHeapCachingMemory;
        }

        @Override
        public EntityBasedMemoryLimiter create(long highNodeId, long highRelationshipId) {
            long perEntityMemory = CacheSlots.CACHE_LINE_SIZE_BYTES;
            return new EntityBasedMemoryLimiter(
                    maxOffHeapCachingMemory, perEntityMemory, highNodeId, highRelationshipId);
        }
    }

    static class CheckRange {
        private final LongRange nodeRange;
        private final LongRange relationshipRange;
        private final long rangeStart;
        private final long rangeEnd;

        CheckRange(long rangeStart, long rangeEnd, long highNodeId, long highRelationshipId) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            relationshipRange = rangeStart < highRelationshipId
                    ? LongRange.range(rangeStart, min(rangeEnd, highRelationshipId))
                    : null;
            nodeRange = rangeStart < highNodeId ? LongRange.range(rangeStart, min(rangeEnd, highNodeId)) : null;
        }

        boolean applicableForNodeBasedChecks() {
            return nodeRange != null;
        }

        boolean applicableForRelationshipBasedChecks() {
            return relationshipRange != null;
        }

        public LongRange getNodeRange() {
            return nodeRange;
        }

        public LongRange getRelationshipRange() {
            return relationshipRange;
        }

        public long from() {
            return rangeStart;
        }

        @Override
        public String toString() {
            return LongRange.range(rangeStart, rangeEnd).toString();
        }
    }
}
