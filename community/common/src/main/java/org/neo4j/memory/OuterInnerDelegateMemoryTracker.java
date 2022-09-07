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
 * TODO: Docs
 */
// TODO: Maybe extend ScopedMemoryTracker as the outer tracked values.
//  Only really needed if we need to play in together with the isClosed hack
//  (only release memory if that parent was not already closed)
//  Verify if we still need this check on close
//  TODO: Unit tests
public class OuterInnerDelegateMemoryTracker implements MemoryTracker, OuterInnerHeapMemoryTracker {
    private final MemoryTracker outerDelegate;
    private MemoryTracker innerDelegate;
    private long outerTrackedNative;
    private long innerTrackedNative;
    private long outerTrackedHeap;
    private long innerTrackedHeap;
    private boolean isClosed;

    public OuterInnerDelegateMemoryTracker(MemoryTracker outerDelegate) {
        this.outerDelegate = outerDelegate;
    }

    public void setInnerDelegate(MemoryTracker innerDelegate) {
        this.innerDelegate = innerDelegate;
    }

    @Override
    public long usedNativeMemory() {
        return outerTrackedNative + innerTrackedNative;
    }

    @Override
    public long estimatedHeapMemory() {
        return outerTrackedHeap + innerTrackedHeap;
    }

    @Override
    public void allocateNative(long bytes) {
        throwIfClosed();
        if (innerDelegate != null) {
            innerDelegate.allocateNative(bytes);
            innerTrackedNative += bytes;
        } else {
            outerDelegate.allocateNative(bytes);
            outerTrackedNative += bytes;
        }
    }

    @Override
    public void releaseNative(long bytes) {
        throwIfClosed();
        if (innerDelegate != null) {
            innerDelegate.releaseNative(bytes);
            innerTrackedNative -= bytes;
        } else {
            outerDelegate.releaseNative(bytes);
            outerTrackedNative -= bytes;
        }
    }

    @Override
    public void allocateHeap(long bytes) {
        throwIfClosed();
        if (innerDelegate != null) {
            innerDelegate.allocateHeap(bytes);
            innerTrackedHeap += bytes;
        } else {
            outerDelegate.allocateHeap(bytes);
            outerTrackedHeap += bytes;
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        throwIfClosed();
        if (innerDelegate != null) {
            innerDelegate.releaseHeap(bytes);
            innerTrackedHeap -= bytes;
        } else {
            outerDelegate.releaseHeap(bytes);
            outerTrackedHeap -= bytes;
        }
    }

    @Override
    public void allocateHeapOuter(long bytes) {
        throwIfClosed();
        outerDelegate.allocateHeap(bytes);
        outerTrackedHeap += bytes;
    }

    @Override
    public void releaseHeapOuter(long bytes) {
        throwIfClosed();
        outerDelegate.releaseHeap(bytes);
        outerTrackedHeap -= bytes;
    }

    private void throwIfClosed() {
        if (isClosed) {
            throw new IllegalStateException("Should not use a closed ScopedMemoryTracker");
        }
    }

    @Override
    public long heapHighWaterMark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        innerDelegate.releaseNative(innerTrackedNative);
        innerDelegate.releaseHeap(innerTrackedHeap);
        outerDelegate.releaseNative(outerTrackedNative);
        outerDelegate.releaseHeap(outerTrackedHeap);
        innerTrackedNative = 0;
        innerTrackedHeap = 0;
        outerTrackedNative = 0;
        outerTrackedHeap = 0;
    }

    @Override
    public void close() {
        // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
        // TODO: Check if we still need this?
        if (!(innerDelegate instanceof ScopedMemoryTracker) || !((ScopedMemoryTracker) innerDelegate).isClosed) {
            innerDelegate.releaseNative(innerTrackedNative);
            innerDelegate.releaseHeap(innerTrackedHeap);
        }
        if (!(outerDelegate instanceof ScopedMemoryTracker) || !((ScopedMemoryTracker) outerDelegate).isClosed) {
            outerDelegate.releaseNative(outerTrackedNative);
            outerDelegate.releaseHeap(outerTrackedHeap);
        }
        innerTrackedNative = 0;
        innerTrackedHeap = 0;
        outerTrackedNative = 0;
        outerTrackedHeap = 0;
        isClosed = true;
    }

    public void closeInner() {
        if (innerDelegate != null) {
            // On a parent ScopedMemoryTracker, only release memory if that parent was not already closed.
            // TODO: Check if we still need this?
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
