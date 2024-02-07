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
package org.neo4j.collection;

import static java.util.Arrays.copyOf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.helpers.collection.Iterators;

/**
 * Basic and common primitive int collection utils and manipulations.
 */
public final class PrimitiveLongCollections {
    private PrimitiveLongCollections() {
        // nop
    }

    public static LongIterator single(long item) {
        return new SingleLongIterator(item);
    }

    private static final class SingleLongIterator implements LongIterator {
        private final long item;
        private boolean consumed;

        SingleLongIterator(long item) {
            this.item = item;
        }

        @Override
        public long next() {
            if (consumed) {
                throw new NoSuchElementException("No such element");
            }
            consumed = true;
            return item;
        }

        @Override
        public boolean hasNext() {
            return !consumed;
        }
    }

    public static LongIterator iterator(final long... items) {
        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator(Resource.EMPTY) {
            private int index = -1;

            @Override
            protected boolean fetchNext() {
                index++;
                return index < items.length && next(items[index]);
            }
        };
    }

    public static LongIterator reverseIterator(final long... items) {
        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator(Resource.EMPTY) {
            private int index = items.length;

            @Override
            protected boolean fetchNext() {
                index--;
                return index >= 0 && next(items[index]);
            }
        };
    }

    // Concating
    public static LongIterator concat(LongIterator... longIterators) {
        return concat(Iterators.iterator(longIterators));
    }

    public static LongIterator concat(Iterator<LongIterator> primitiveLongIterators) {
        return new PrimitiveLongConcatenatingIterator(primitiveLongIterators);
    }

    public static LongIterator filter(LongIterator source, final LongPredicate filter) {
        return new AbstractPrimitiveLongFilteringIterator(source) {
            @Override
            public boolean test(long item) {
                return filter.test(item);
            }
        };
    }

    // Range
    public static RangedLongIterator range(long start, long end) {
        return new PrimitiveLongRangeIterator(start, end);
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static int indexOf(LongIterator iterator, long item) {
        for (int i = 0; iterator.hasNext(); i++) {
            if (item == iterator.next()) {
                return i;
            }
        }
        return -1;
    }

    public static MutableLongSet asSet(Collection<Long> collection) {
        final MutableLongSet set = new LongHashSet(collection.size());
        for (Long next : collection) {
            set.add(next);
        }
        return set;
    }

    public static MutableLongSet asSet(LongIterator iterator) {
        MutableLongSet set = new LongHashSet();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

    public static int count(LongIterator iterator) {
        int count = 0;
        while (iterator.hasNext()) { // Just loop through this
            iterator.next();
            count++;
        }
        return count;
    }

    public static long[] asArray(LongIterator iterator) {
        long[] array = new long[8];
        int i = 0;
        for (; iterator.hasNext(); i++) {
            if (i >= array.length) {
                array = copyOf(array, i << 1);
            }
            array[i] = iterator.next();
        }

        if (i < array.length) {
            array = copyOf(array, i);
        }
        return array;
    }

    public static long[] asArray(Iterator<Long> iterator) {
        return asArray(toPrimitiveIterator(iterator));
    }

    private static LongIterator toPrimitiveIterator(final Iterator<Long> iterator) {
        return new AbstractPrimitiveLongBaseIterator() {
            @Override
            protected boolean fetchNext() {
                if (iterator.hasNext()) {
                    Long nextValue = iterator.next();
                    if (null == nextValue) {
                        throw new IllegalArgumentException("Cannot convert null Long to primitive long");
                    }
                    return next(nextValue);
                }
                return false;
            }
        };
    }

    /**
     * Wraps a {@link LongIterator} in a {@link PrimitiveLongResourceIterator} which closes
     * the provided {@code resource} in {@link PrimitiveLongResourceIterator#close()}.
     *
     * @param iterator {@link LongIterator} to convert
     * @param resource {@link Resource} to close in {@link PrimitiveLongResourceIterator#close()}
     * @return Wrapped {@link LongIterator}.
     */
    public static PrimitiveLongResourceIterator resourceIterator(final LongIterator iterator, final Resource resource) {
        return new PrimitiveLongResourceIterator() {
            @Override
            public void close() {
                if (resource != null) {
                    resource.close();
                }
            }

            @Override
            public long next() {
                return iterator.next();
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
        };
    }

    /**
     * Convert primitive set into a plain old java {@link Set}, boxing each long.
     *
     * @param set {@link LongSet} set of primitive values.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet(LongSet set) {
        return toSet(set.longIterator());
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link Set}, boxing each long.
     *
     * @param iterator {@link LongIterator} to pull values from.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet(LongIterator iterator) {
        Set<Long> set = new HashSet<>();
        while (iterator.hasNext()) {
            addUnique(set, iterator.next());
        }
        return set;
    }

    private static <T, C extends Collection<T>> void addUnique(C collection, T item) {
        if (!collection.add(item)) {
            throw new IllegalStateException("Encountered an already added item:" + item
                    + " when adding items uniquely to a collection:" + collection);
        }
    }

    /**
     * Base iterator for simpler implementations of {@link LongIterator}s.
     */
    public abstract static class AbstractPrimitiveLongBaseIterator implements LongIterator {
        private boolean hasNextDecided;
        private boolean hasNext;
        protected long next;

        @Override
        public boolean hasNext() {
            if (!hasNextDecided) {
                hasNext = fetchNext();
                hasNextDecided = true;
            }
            return hasNext;
        }

        @Override
        public long next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in " + this);
            }
            hasNextDecided = false;
            return next;
        }

        /**
         * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
         * item was found, that value must have been set inside the implementation of this method
         * using {@link #next(long)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
         * This method returns {@code true} so that it can be used in short-hand conditionals
         * (TODO what are they called?), like:
         * <pre>
         * protected boolean fetchNext()
         * {
         *     return source.hasNext() ? next( source.next() ) : false;
         * }
         * </pre>
         * @param nextItem the next item found.
         */
        protected boolean next(long nextItem) {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static class PrimitiveLongConcatenatingIterator extends AbstractPrimitiveLongBaseIterator {
        private final Iterator<? extends LongIterator> iterators;
        private LongIterator currentIterator;

        PrimitiveLongConcatenatingIterator(Iterator<? extends LongIterator> iterators) {
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext() {
            if (currentIterator == null || !currentIterator.hasNext()) {
                while (iterators.hasNext()) {
                    currentIterator = iterators.next();
                    if (currentIterator.hasNext()) {
                        break;
                    }
                }
            }
            return (currentIterator != null && currentIterator.hasNext()) && next(currentIterator.next());
        }
    }

    public abstract static class AbstractPrimitiveLongFilteringIterator extends AbstractPrimitiveLongBaseIterator
            implements LongPredicate {
        protected final LongIterator source;

        AbstractPrimitiveLongFilteringIterator(LongIterator source) {
            this.source = source;
        }

        @Override
        protected boolean fetchNext() {
            while (source.hasNext()) {
                long testItem = source.next();
                if (test(testItem)) {
                    return next(testItem);
                }
            }
            return false;
        }
    }

    public static class PrimitiveLongRangeIterator extends AbstractPrimitiveLongBaseIterator
            implements RangedLongIterator {
        private final long start;
        private long current;
        private final long end;

        PrimitiveLongRangeIterator(long start, long end) {
            this.start = start;
            this.current = start;
            this.end = end;
        }

        @Override
        protected boolean fetchNext() {
            try {
                return current <= end && next(current);
            } finally {
                current++;
            }
        }

        @Override
        public long startInclusive() {
            return start;
        }

        @Override
        public long endExclusive() {
            return end + 1;
        }
    }

    public static MutableLongSet mergeToSet(LongIterable a, LongIterable b) {
        return LongSets.mutable
                .withInitialCapacity(a.size() + b.size())
                .withAll(a)
                .withAll(b);
    }

    public interface RangedLongIterator extends LongIterator {
        long startInclusive();

        long endExclusive();
    }
}
