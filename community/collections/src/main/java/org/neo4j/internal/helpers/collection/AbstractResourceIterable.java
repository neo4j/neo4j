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
package org.neo4j.internal.helpers.collection;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

public abstract class AbstractResourceIterable<T> implements ResourceIterable<T> {
    // start with 2 as most cases will only generate a single iterator but gives a little leeway to save on expansions
    private TrackingResourceIterator<?>[] trackedIterators = new TrackingResourceIterator<?>[2];

    private long trackedIteratorsInUse;

    private boolean closed;

    protected abstract ResourceIterator<T> newIterator();

    @Override
    public final ResourceIterator<T> iterator() {
        if (closed) {
            throw new ResourceIteratorCloseFailedException(
                    ResourceIterable.class.getSimpleName() + " has already been closed");
        }

        return new TrackingResourceIterator<>(Objects.requireNonNull(newIterator()), this::register, this::unregister);
    }

    @Override
    public final void close() {
        if (!closed) {
            try {
                internalClose();
            } finally {
                closed = true;
                onClosed();
            }
        }
    }

    /**
     * Callback method that allows subclasses to perform their own specific closing logic
     */
    protected void onClosed() {}

    private int register(TrackingResourceIterator<?> iterator) {
        assert Long.bitCount(trackedIteratorsInUse) < Long.SIZE;
        if (Long.bitCount(trackedIteratorsInUse) == trackedIterators.length) {
            trackedIterators = Arrays.copyOf(trackedIterators, trackedIterators.length << 1);
        }

        int index = Long.numberOfTrailingZeros(~trackedIteratorsInUse);
        trackedIterators[index] = iterator;
        trackedIteratorsInUse |= trackBit(index);
        return index;
    }

    private void unregister(TrackingResourceIterator<?> iterator) {
        assert (trackedIteratorsInUse & trackBit(iterator.registerIndex)) != 0;
        trackedIterators[iterator.registerIndex] = null;
        untrack(iterator.registerIndex);
    }

    private static long trackBit(int index) {
        return 1L << index;
    }

    private void untrack(int index) {
        trackedIteratorsInUse ^= trackBit(index);
    }

    private void internalClose() {
        ResourceIteratorCloseFailedException closeThrowable = null;

        while (trackedIteratorsInUse != 0) {
            int index = Long.numberOfTrailingZeros(trackedIteratorsInUse);
            try {
                trackedIterators[index].internalClose();
            } catch (Exception e) {
                if (closeThrowable == null) {
                    closeThrowable =
                            new ResourceIteratorCloseFailedException("Exception closing a resource iterator.", e);
                } else {
                    closeThrowable.addSuppressed(e);
                }
            }
            untrack(index);
        }

        trackedIterators = null;

        if (closeThrowable != null) {
            throw closeThrowable;
        }
    }

    private static final class TrackingResourceIterator<T> implements ResourceIterator<T> {
        private final ResourceIterator<T> delegate;
        private final ToIntFunction<TrackingResourceIterator<?>> registerCallback;
        private final Consumer<TrackingResourceIterator<?>> unregisterCallback;
        private final int registerIndex;

        private boolean closed;

        private TrackingResourceIterator(
                ResourceIterator<T> delegate,
                ToIntFunction<TrackingResourceIterator<?>> registerCallback,
                Consumer<TrackingResourceIterator<?>> unregisterCallback) {
            this.delegate = delegate;
            this.registerCallback = registerCallback;
            this.unregisterCallback = unregisterCallback;
            this.registerIndex = registerCallback.applyAsInt(this);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = delegate.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public T next() {
            return delegate.next();
        }

        @Override
        public <R> ResourceIterator<R> map(Function<T, R> map) {
            return new TrackingResourceIterator<>(
                    ResourceIterator.super.map(map), registerCallback, unregisterCallback);
        }

        @Override
        public void close() {
            if (!closed) {
                internalClose();
                unregisterCallback.accept(this);
            }
        }

        private void internalClose() {
            try {
                delegate.close();
            } finally {
                closed = true;
            }
        }
    }
}
