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
package org.neo4j.memory;

import org.neo4j.util.VisibleForTesting;

/**
 * A {@link DefaultScopedMemoryTracker} with a mutable inner delegate memory tracker.
 * When the inner delegate memory tracker is null (the default), all allocation and release calls go to the
 * outer scoped memory tracker, but when an inner delegate is set with {@link #setInnerDelegate(MemoryTracker)},
 * all allocation and release calls gets routed to the inner delegate memory tracker instead.
 * The inner delegate memory tracker can be reset with {@link #closeInner()}, after which all subsequent allocation and
 * release calls will go back to get routed to the outer scoped memory tracker again.
 * <p>
 * This class is intended to simplify the management of recording allocations over inner transactions.
 */
public class RebindableDualScopedMemoryTracker extends DefaultScopedMemoryTracker {
    // This is always scoped
    private MemoryTracker innerDelegate;

    // This is memory that was allocated inside an inner scope but was not released
    // and is carried over to the next inner scope.
    private long unreleasedInnerScopeNative;
    private long unreleasedInnerScopeHeap;

    public RebindableDualScopedMemoryTracker(MemoryTracker outerDelegate) {
        super(outerDelegate);
    }

    public void setInnerDelegate(MemoryTracker innerDelegate) {
        final var scopedInner = innerDelegate.getScopedMemoryTracker();

        // Inherit memory that was not deallocated in the last inner scope
        // This is not ideal, but happens easily because it's very hard to
        // do perfect cleanup of failing queries in CALL IN TRANSACTIONS
        // with ON ERROR CONTINUE|BREAK.
        scopedInner.allocateNative(unreleasedInnerScopeNative);
        scopedInner.allocateHeap(unreleasedInnerScopeHeap);
        this.innerDelegate = scopedInner;
    }

    @Override
    public long usedNativeMemory() {
        final long outer = super.usedNativeMemory();
        return innerDelegate != null ? innerDelegate.usedNativeMemory() + outer : outer;
    }

    @Override
    public long estimatedHeapMemory() {
        final long outer = super.estimatedHeapMemory();
        return innerDelegate != null ? innerDelegate.estimatedHeapMemory() + outer : outer;
    }

    @Override
    public void allocateNative(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.allocateNative(bytes);
        } else {
            super.allocateNative(bytes);
        }
    }

    @Override
    public void releaseNative(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.releaseNative(bytes);
        } else {
            super.releaseNative(bytes);
        }
    }

    @Override
    public void allocateHeap(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.allocateHeap(bytes);
        } else {
            super.allocateHeap(bytes);
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.releaseHeap(bytes);
        } else {
            super.releaseHeap(bytes);
        }
    }

    @Override
    public long heapHighWaterMark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        innerDelegate.reset();
        unreleasedInnerScopeNative = 0;
        unreleasedInnerScopeHeap = 0;
        super.reset();
    }

    @Override
    public void close() {
        if (innerDelegate != null) {
            closeInner();
        }

        assert unreleasedInnerScopeNative == 0 : "Unreleased inner native memory";
        unreleasedInnerScopeNative = 0;
        assert unreleasedInnerScopeHeap == 0 : "Unreleased inner heap memory";
        unreleasedInnerScopeHeap = 0;

        super.close();
    }

    public void closeInner() {
        if (innerDelegate != null) {
            unreleasedInnerScopeNative = innerDelegate.usedNativeMemory();
            unreleasedInnerScopeHeap = innerDelegate.estimatedHeapMemory();

            // Pretend that everything was released in the inner scope.
            // We carry over unreleased memory to the next inner scope
            // and assert it's zero by the time we close this tracker.
            innerDelegate.close();
            innerDelegate = null;
        }
    }

    @Override
    public MemoryTracker getScopedMemoryTracker() {
        return new DefaultScopedMemoryTracker(this);
    }

    @VisibleForTesting
    protected long unreleasedInnerScopeNative() {
        return unreleasedInnerScopeNative;
    }

    @VisibleForTesting
    protected long unreleasedInnerScopeHeap() {
        return unreleasedInnerScopeHeap;
    }
}
