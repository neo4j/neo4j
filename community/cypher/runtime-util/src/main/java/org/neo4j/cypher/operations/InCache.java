/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.operations;

import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class InCache implements AutoCloseable {
    private final LinkedHashMap<CacheKey, InChecker> seen;

    public InCache() {
        this(16);
    }

    public InCache(int maxSize) {
        seen = new LinkedHashMap<>(maxSize >> 2, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<CacheKey, InChecker> eldest) {
                return super.size() > maxSize;
            }
        };
    }

    public Value check(AnyValue value, ListValue list, MemoryTracker memoryTracker) {
        if (list.size() < 128 || value == NO_VALUE) {
            return CypherFunctions.in(value, list);
        } else {
            final var key = new CacheKey(list);
            var checker = seen.get(key);

            if (checker == null) {
                seen.put(key, DelayInCacheChecker.INSTANCE);
                return CypherFunctions.in(value, list);
            } else if (checker instanceof InCacheChecker cachedChecker) {
                return cachedChecker.check(value);
            } else {
                final var cachedChecker = new InCacheChecker(list, memoryTracker);
                seen.put(key, cachedChecker);
                return cachedChecker.check(value);
            }
        }
    }

    @Override
    public void close() {
        seen.values().forEach(InChecker::close);
    }

    interface InChecker extends AutoCloseable {
        @Override
        void close();
    }

    private static class DelayInCacheChecker implements InChecker {
        private static final DelayInCacheChecker INSTANCE = new DelayInCacheChecker();

        @Override
        public void close() {}
    }

    private static class InCacheChecker implements InChecker, AutoCloseable {
        private final HeapTrackingUnifiedSet<AnyValue> seen;
        private final Iterator<AnyValue> iterator;
        private boolean seenUndefined; // Not valid for sequence values and maps

        private InCacheChecker(ListValue list, MemoryTracker memoryTracker) {
            this.iterator = list.iterator();
            this.seen = HeapTrackingCollections.newSet(memoryTracker);
        }

        private Value check(AnyValue value) {
            assert value != NO_VALUE;

            if (seen.contains(value)) {
                return TRUE;
            }

            while (iterator.hasNext()) {
                var next = iterator.next();
                if (next == NO_VALUE) {
                    seenUndefined = true;
                } else {
                    seen.add(next);

                    if (next.ternaryEquals(value) == Equality.TRUE) {
                        return TRUE;
                    }
                }
            }

            if (seenUndefined) {
                return NO_VALUE;
            } else if (value instanceof SequenceValue || value instanceof MapValue) {
                var undefinedEquality =
                        seen.stream().anyMatch(seenValue -> value.ternaryEquals(seenValue) == Equality.UNDEFINED);
                return undefinedEquality ? NO_VALUE : FALSE;
            } else {
                return FALSE;
            }
        }

        @Override
        public void close() {
            seen.close();
        }
    }
}

class CacheKey {
    private final ListValue list;

    CacheKey(ListValue list) {
        this.list = list;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CacheKey otherKey) {
            return list == otherKey.list;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(list);
    }
}
