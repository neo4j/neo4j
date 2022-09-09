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
package org.neo4j.memory;

/**
 * A {@link ScopedMemoryTracker} with a mutable inner delegate memory tracker.
 * When the inner delegate memory tracker is null (the default), all allocation and release calls go to the
 * outer scoped memory tracker, but when an inner delegate is set with {@link #setInnerDelegate(MemoryTracker)},
 * all allocation and release calls gets routed to the inner delegate memory tracker instead.
 * The inner delegate memory tracker can be reset with {@link #closeInner()}, after which all subsequent allocation and
 * release calls will go back to get routed to the outer scoped memory tracker again.
 * <p>
 * There is also support for explicitly forcing recording allocation and release of heap memory on the outer scope with
 * the {@link OuterInnerHeapMemoryTracker} interface.
 * <p>
 * This class is intended to simplify the management of recording allocations over inner transactions.
 */
public class OuterInnerScopedMemoryTracker extends ScopedMemoryTracker implements OuterInnerHeapMemoryTracker {
    private MemoryTracker innerDelegate;
    private long innerTrackedNative;
    private long innerTrackedHeap;

    public OuterInnerScopedMemoryTracker(MemoryTracker outerDelegate) {
        super(outerDelegate);
    }

    public void setInnerDelegate(MemoryTracker innerDelegate) {
        this.innerDelegate = innerDelegate;
    }

    @Override
    public long usedNativeMemory() {
        return super.usedNativeMemory() + innerTrackedNative;
    }

    @Override
    public long estimatedHeapMemory() {
        return super.estimatedHeapMemory() + innerTrackedHeap;
    }

    @Override
    public void allocateNative(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.allocateNative(bytes);
            innerTrackedNative += bytes;
        } else {
            super.allocateNative(bytes);
        }
    }

    @Override
    public void releaseNative(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.releaseNative(bytes);
            innerTrackedNative -= bytes;
        } else {
            super.releaseNative(bytes);
        }
    }

    @Override
    public void allocateHeap(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.allocateHeap(bytes);
            innerTrackedHeap += bytes;
        } else {
            super.allocateHeap(bytes);
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        if (innerDelegate != null) {
            innerDelegate.releaseHeap(bytes);
            innerTrackedHeap -= bytes;
        } else {
            super.releaseHeap(bytes);
        }
    }

    @Override
    public void allocateHeapOuter(long bytes) {
        super.allocateHeap(bytes);
    }

    @Override
    public void releaseHeapOuter(long bytes) {
        super.releaseHeap(bytes);
    }

    @Override
    public long heapHighWaterMark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        innerDelegate.releaseNative(innerTrackedNative);
        innerDelegate.releaseHeap(innerTrackedHeap);
        innerTrackedNative = 0;
        innerTrackedHeap = 0;
        super.reset();
    }

    @Override
    public void close() {
        // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
        if (innerDelegate != null
                && (!(innerDelegate instanceof ScopedMemoryTracker)
                        || !((ScopedMemoryTracker) innerDelegate).isClosed)) {
            innerDelegate.releaseNative(innerTrackedNative);
            innerDelegate.releaseHeap(innerTrackedHeap);
            innerDelegate = null;
        }
        innerTrackedNative = 0;
        innerTrackedHeap = 0;
        super.close();
    }

    public void closeInner() {
        if (innerDelegate != null) {
            // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
            if (!(innerDelegate instanceof ScopedMemoryTracker) || !((ScopedMemoryTracker) innerDelegate).isClosed) {
                innerDelegate.releaseNative(innerTrackedNative);
                innerDelegate.releaseHeap(innerTrackedHeap);
            }
            innerTrackedNative = 0;
            innerTrackedHeap = 0;
            innerDelegate = null;
        }
    }

    @Override
    public MemoryTracker getScopedMemoryTracker() {
        return new ScopedMemoryTracker(this);
    }
}
