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
package org.neo4j.io.pagecache.impl.muninn;

import static org.neo4j.util.Preconditions.requirePositive;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.util.VisibleForTesting;

/**
 * The SwapperSet maintains the set of allocated {@link PageSwapper}s, and their mapping to swapper ids.
 * These swapper ids are a limited resource, so they must eventually be reused as files are mapped and unmapped.
 * Before a swapper id can be reused, we have to make sure that there are no pages in the page cache, that
 * are bound to the old swapper id. To ensure this, we release all pages if possible when we close file,
 * or fallback to periodic {@link MuninnPageCache#sweep(SwapperSet)} if for some reason that is not possible (we still have cursors open)
 * The sweeping process will then fully evict all pages that are bound to a page swapper ids that were released previously but failed their
 * quick round of page releases.
 */
public final class SwapperSet {
    public static final int FREE_CANDIDATES_THRESHOLD = 1 << 14;
    // The sentinel is used to reserve swapper id 0 as a special value.
    private static final SwapperMapping SENTINEL = new SwapperMapping(0, null);
    private static final int MAX_SWAPPER_ID = (1 << 21) - 1;
    private volatile SwapperMapping[] swapperMappings = new SwapperMapping[] {SENTINEL};
    private final LinkedList<Integer> free = new LinkedList<>();
    private final MutableIntSet postponedIds = new IntHashSet();
    private final Lock sweepCandidatesLock = new ReentrantLock();

    /**
     * The mapping entry between a {@link PageSwapper} and its swapper id.
     */
    static final class SwapperMapping {
        public final int id;
        public final PageSwapper swapper;

        private SwapperMapping(int id, PageSwapper swapper) {
            this.id = id;
            this.swapper = swapper;
        }
    }

    /**
     * Get the {@link SwapperMapping} for the given swapper id.
     */
    SwapperMapping getAllocation(int id) {
        requirePositive(id);
        return swapperMappings[id];
    }

    /**
     * Allocate a new swapper id for the given {@link PageSwapper}.
     */
    public synchronized int allocate(PageSwapper swapper) {
        SwapperMapping[] swapperMappings = this.swapperMappings;

        // First look for an available freed slot.
        var freeId = free.pollFirst();
        if (freeId != null) {
            int id = freeId;
            swapperMappings[id] = new SwapperMapping(id, swapper);
            this.swapperMappings = swapperMappings;
            return id;
        }

        // No free slot was found above, so we extend the array to make room for a new slot.
        int id = swapperMappings.length;
        if (id + 1 > MAX_SWAPPER_ID) {
            throw new IllegalStateException("All swapper ids are allocated: " + MAX_SWAPPER_ID);
        }
        swapperMappings = Arrays.copyOf(swapperMappings, id + 1);
        swapperMappings[id] = new SwapperMapping(id, swapper);
        this.swapperMappings = swapperMappings;
        return id;
    }

    /**
     * Free the given swapper id.
     */
    synchronized void free(int id) {
        requirePositive(id);
        SwapperMapping[] swapperMappings = this.swapperMappings;
        SwapperMapping current = swapperMappings[id];
        if (current == null) {
            throw new IllegalStateException(
                    "PageSwapper allocation id " + id + " is currently not allocated. Likely a double free bug.");
        }
        swapperMappings[id] = null;
        this.swapperMappings = swapperMappings;
        free.add(id);
    }

    public synchronized void postponedFree(int swapperId) {
        postponedIds.add(swapperId);
    }

    void sweep(Consumer<IntSet> evictAllLoadedPagesCallback) {
        if (skipSweep()) {
            return;
        }
        sweepCandidatesLock.lock();
        try {
            if (skipSweep()) {
                return;
            }
            ImmutableIntSet candidates = sweepCandidates();
            evictAllLoadedPagesCallback.accept(candidates);

            synchronized (this) {
                candidates.forEach(id -> {
                    postponedIds.remove(id);
                    free(id);
                });
            }
        } finally {
            sweepCandidatesLock.unlock();
        }
    }

    @VisibleForTesting
    synchronized boolean skipSweep() {
        return postponedIds.size() < FREE_CANDIDATES_THRESHOLD;
    }

    private synchronized ImmutableIntSet sweepCandidates() {
        return postponedIds.toImmutable();
    }
}
