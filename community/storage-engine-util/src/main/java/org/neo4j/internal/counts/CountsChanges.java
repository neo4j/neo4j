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
package org.neo4j.internal.counts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.neo4j.util.Preconditions;

public abstract class CountsChanges {
    static final long ABSENT = -1;

    protected final ConcurrentMap<CountsKey, AtomicLong> changes;
    protected volatile ConcurrentMap<CountsKey, AtomicLong> previousChanges;
    private volatile boolean frozen;

    protected CountsChanges(ConcurrentMap<CountsKey, AtomicLong> changes) {
        this.changes = changes;
    }

    /**
     * Makes this instance immutable and returns a new (mutable) instance with this instance as the instance to first read counts from,
     * since those counts represent persisted counts which may or may not yet have made it to the backing tree.
     * @return a new instance which will replace this instance for making updates to.
     */
    CountsChanges freezeAndFork() {
        frozen = true;
        CountsChanges fork = fork();
        fork.previousChanges = changes;
        return fork;
    }

    protected abstract CountsChanges fork();

    /**
     * Clears the reference to the old instances that now has been written to the backing tree.
     */
    void clearPreviousChanges() {
        this.previousChanges = null;
    }

    /**
     * Make a relative counts change to the given key.
     *
     * @param key {@link CountsKey} the key to make the update for.
     * @param delta the delta for the count, can be positive or negative.
     * @param defaultToStoredCount where to read the absolute count if it isn't already loaded into this instance (or the "old" instance).
     * @return {@code true} if the absolute value either was 0 before this change, or became zero after the change. Otherwise {@code false}.
     */
    boolean add(CountsKey key, long delta, Function<CountsKey, AtomicLong> defaultToStoredCount) {
        Preconditions.checkState(!frozen, "Can't make changes in a frozen state");
        long absoluteValueAfterChange = getCounter(key, defaultToStoredCount).addAndGet(delta);
        return delta > 0 ? absoluteValueAfterChange - delta == 0 : absoluteValueAfterChange == 0;
    }

    private AtomicLong getCounter(CountsKey key, Function<CountsKey, AtomicLong> defaultToStoredCount) {
        ConcurrentMap<CountsKey, AtomicLong> prev = previousChanges;
        Function<CountsKey, AtomicLong> defaultFunction = prev == null
                ? defaultToStoredCount
                : k -> {
                    AtomicLong prevCount = prev.get(k);
                    if (prevCount != null) {
                        return new AtomicLong(prevCount.get());
                    }
                    return defaultToStoredCount.apply(k);
                };
        return changes.computeIfAbsent(key, defaultFunction);
    }

    Iterable<Map.Entry<CountsKey, AtomicLong>> sortedChanges(Comparator<CountsKey> comparator) {
        List<Map.Entry<CountsKey, AtomicLong>> sortedChanges = new ArrayList<>(changes.entrySet());
        sortedChanges.sort((e1, e2) -> comparator.compare(e1.getKey(), e2.getKey()));
        return sortedChanges;
    }

    /**
     * @param key {@link CountsKey} to check.
     * @return {@code true} if there have been an update to the given key in this instance or in the "old" instance.
     */
    boolean containsChange(CountsKey key) {
        if (changes.containsKey(key)) {
            return true;
        }
        ConcurrentMap<CountsKey, AtomicLong> prev = previousChanges;
        return prev != null && prev.containsKey(key);
    }

    /**
     * @param key {@link CountsKey} to get count for.
     * @return the absolute count for the given key, be it from this instance or the "old" instance. If this key doesn't exist here then
     * {@link #ABSENT} is returned, but that can still mean that the count exist, although in the backing tree.
     */
    long get(CountsKey key) {
        AtomicLong count = changes.get(key);
        if (count != null) {
            return count.get();
        }
        ConcurrentMap<CountsKey, AtomicLong> prev = previousChanges;
        if (prev != null) {
            AtomicLong prevCount = prev.get(key);
            if (prevCount != null) {
                return prevCount.get();
            }
        }
        return ABSENT;
    }

    int size() {
        return changes.size();
    }
}
