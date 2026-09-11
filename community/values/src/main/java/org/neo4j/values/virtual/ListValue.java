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
package org.neo4j.values.virtual;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;
import static org.neo4j.values.SequenceValue.IterationPreference.ITERATION;
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;
import static org.neo4j.values.virtual.ArrayHelpers.assertValueRepresentation;
import static org.neo4j.values.virtual.ArrayHelpers.containsNull;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.github.jamm.Unmetered;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.memory.HeapEstimatorCache;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.TernaryComparator;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

public abstract class ListValue extends VirtualValue implements SequenceValue, Iterable<AnyValue> {
    public static final String CYPHER_TYPE_NAME = "LIST";

    public static final int LIST_DEPTH_COMPACTION_THRESHOLD = 128;

    /**
     * ListValues may be nested structures such as AppendList, PrependList, ConcatList etc. We track the depth of
     * these nested structures in order to compact them for the sake of performance/avoiding stack overflows.
     * It is important that classes extending ListValue override this method if they contain an inner ListValue, and
     * that the overridden method does not call listDepth() on the inner ListValue (this should be done in the
     * constructor and assigned to a field)
     * */
    protected int listDepth() {
        return 1;
    }

    public Value ternaryContains(AnyValue value) {
        Iterator<AnyValue> iterator = iterator();

        boolean undefinedEquality = false;
        while (iterator.hasNext()) {
            AnyValue nextValue = iterator.next();
            Equality equality = nextValue.ternaryEquals(value);

            if (equality == Equality.TRUE) {
                return BooleanValue.TRUE;
            } else if (equality == Equality.UNDEFINED && !undefinedEquality) {
                undefinedEquality = true;
            }
        }

        return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
    }

    public abstract ValueRepresentation itemValueRepresentation();

    @Override
    public String getTypeName() {
        return "List";
    }

    public static final class ArrayValueListValue extends ListValue {
        private static final long ARRAY_VALUE_LIST_VALUE_SHALLOW_SIZE =
                shallowSizeOfInstance(ArrayValueListValue.class);
        private final ArrayValue array;

        ArrayValueListValue(ArrayValue array) {
            this.array = array;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public ArrayValue toStorableArray() {
            return array;
        }

        @Override
        public long actualSize() {
            return array.intSize();
        }

        @Override
        public int intSize() {
            return array.intSize();
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return array.iterator();
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return isEmpty() ? ValueRepresentation.ANYTHING : head().valueRepresentation();
        }

        @Override
        public AnyValue value(long offset) {
            return array.value(offset);
        }

        @Override
        protected int computeHashToMemoize() {
            return array.hashCode();
        }

        @Override
        public long estimatedHeapUsage() {
            return ARRAY_VALUE_LIST_VALUE_SHALLOW_SIZE + array.estimatedHeapUsage();
        }
    }

    public static final class RelationshipListValue extends ListValue {
        private static final long REL_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(RelationshipListValue.class);

        private final List<VirtualRelationshipValue> list;

        RelationshipListValue(List<VirtualRelationshipValue> list) {
            this.list = list;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public ArrayValue toStorableArray() {
            throw CypherTypeException.propertyWithRelCollection(list);
        }

        @Override
        public long actualSize() {
            return intSize();
        }

        @Override
        public int intSize() {
            return list.size();
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return Iterators.map(Function.identity(), list.iterator());
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return ValueRepresentation.ANYTHING;
        }

        @Override
        public AnyValue value(long offset) {
            if (offset < 0L || offset >= list.size()) {
                throw new IndexOutOfBoundsException();
            }
            return list.get((int) offset);
        }

        @Override
        public long estimatedHeapUsage() {
            int length = list.size();
            if (length == 0) {
                return REL_LIST_VALUE_SHALLOW_SIZE;
            } else {
                return REL_LIST_VALUE_SHALLOW_SIZE
                        + sizeOfObjectArray(sizeOf(list.get(0)), length); // Use first element as probe
            }
        }
    }

    public static final class ArrayListValue extends ListValue {
        private static final long ARRAY_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(ArrayListValue.class);
        private final AnyValue[] values;
        private final long payloadSize;

        @Unmetered
        private final ValueRepresentation itemRepresentation;

        ArrayListValue(AnyValue[] values, long payloadSize, ValueRepresentation itemRepresentation) {
            assert values != null
                    && payloadSize >= 0
                    && !containsNull(values)
                    && assertValueRepresentation(values, itemRepresentation);
            this.payloadSize = shallowSizeOfObjectArray(values.length) + payloadSize;

            this.values = values;
            this.itemRepresentation = itemRepresentation;
        }

        @Override
        public IterationPreference iterationPreference() {
            return RANDOM_ACCESS;
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return Iterators.iterator(values);
        }

        @Override
        public long actualSize() {
            return intSize();
        }

        @Override
        public int intSize() {
            return values.length;
        }

        @Override
        public AnyValue value(long offset) {
            if (offset < 0L || offset >= values.length) {
                throw new IndexOutOfBoundsException();
            }
            return values[(int) offset];
        }

        @Override
        public AnyValue[] asArray() {
            return values;
        }

        @Override
        protected int computeHashToMemoize() {
            return Arrays.hashCode(values);
        }

        @Override
        protected long compactInto(AnyValue[] array, int fromInclusive) {
            System.arraycopy(values, 0, array, fromInclusive, values.length);
            return payloadSize;
        }

        @Override
        public long estimatedHeapUsage() {
            return ARRAY_LIST_VALUE_SHALLOW_SIZE + payloadSize;
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return itemRepresentation;
        }

        @Override
        public Value ternaryContains(AnyValue value) {
            if (values.length == 0) {
                return BooleanValue.FALSE;
            }
            boolean undefinedEquality = false;
            for (AnyValue nextValue : values) {
                Equality equality = nextValue.ternaryEquals(value);
                if (equality == Equality.TRUE) {
                    return BooleanValue.TRUE;
                } else if (equality == Equality.UNDEFINED && !undefinedEquality) {
                    undefinedEquality = true;
                }
            }
            return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
        }
    }

    private static final long JAVA_LIST_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(JavaListListValue.class);

    static final class JavaListListValue extends ListValue {
        private final List<AnyValue> values;
        private final long payloadSize;

        @Unmetered
        private final ValueRepresentation itemRepresentation;

        JavaListListValue(List<AnyValue> values, long payloadSize, ValueRepresentation itemRepresentation) {
            assert payloadSize >= 0
                    && values != null
                    && !containsNull(values)
                    && assertValueRepresentation(values.toArray(AnyValue[]::new), itemRepresentation);

            this.payloadSize = payloadSize;
            this.values = values;
            this.itemRepresentation = itemRepresentation;
        }

        @Override
        public IterationPreference iterationPreference() {
            if (values instanceof RandomAccess) {
                return RANDOM_ACCESS;
            } else {
                return ITERATION;
            }
        }

        @Override
        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public long actualSize() {
            return values.size();
        }

        @Override
        public Value ternaryContains(AnyValue value) {
            if (values.isEmpty()) {
                return BooleanValue.FALSE;
            }
            boolean undefinedEquality = false;
            for (AnyValue nextValue : values) {
                Equality equality = nextValue.ternaryEquals(value);
                if (equality == Equality.TRUE) {
                    return BooleanValue.TRUE;
                } else if (equality == Equality.UNDEFINED && !undefinedEquality) {
                    undefinedEquality = true;
                }
            }
            return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
        }

        @Override
        public AnyValue value(long offset) {
            if (offset < 0L || offset >= values.size()) {
                throw new IndexOutOfBoundsException();
            }
            return values.get((int) offset);
        }

        @Override
        public AnyValue[] asArray() {
            return values.toArray(new AnyValue[0]);
        }

        @Override
        protected int computeHashToMemoize() {
            return values.hashCode();
        }

        @Override // override to skip recomputing payloadSize
        protected long compactInto(AnyValue[] array, int fromInclusive) {
            int i = fromInclusive;
            for (var x : this) {
                array[i] = x;
                i++;
            }
            return payloadSize;
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return values.iterator();
        }

        @Override
        public long estimatedHeapUsage() {
            return JAVA_LIST_LIST_VALUE_SHALLOW_SIZE + payloadSize;
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return itemRepresentation;
        }
    }

    private static final long LIST_SLICE_SHALLOW_SIZE = shallowSizeOfInstance(ListSlice.class);

    static final class ListSlice extends ListValue {
        private final ListValue inner;
        private final long fromInclusive;
        private final long toExclusive;

        ListSlice(ListValue inner, long fromInclusive, long toExclusive) {
            assert fromInclusive >= 0;
            assert toExclusive <= inner.actualSize();
            assert fromInclusive <= toExclusive;
            this.listDepth = inner.listDepth() + 1;
            this.inner = inner;
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
        }

        private final int listDepth;

        @Override
        protected int listDepth() {
            return listDepth;
        }

        @Override
        public IterationPreference iterationPreference() {
            return inner.iterationPreference();
        }

        @Override
        public long actualSize() {
            return toExclusive - fromInclusive;
        }

        @Override
        public AnyValue value(long offset) {
            return inner.value(offset + fromInclusive);
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return switch (inner.iterationPreference()) {
                case RANDOM_ACCESS -> randomAccessIterator();
                case ITERATION -> new ListSliceIterator();
            };
        }

        private class ListSliceIterator extends PrefetchingIterator<AnyValue> {
            private long count;
            private final Iterator<AnyValue> innerIterator = inner.iterator();

            @Override
            protected AnyValue fetchNextOrNull() {
                // make sure we are at least at first element
                while (count < fromInclusive && innerIterator.hasNext()) {
                    innerIterator.next();
                    count++;
                }
                // check if we are done
                if (count < fromInclusive || count >= toExclusive || !innerIterator.hasNext()) {
                    return null;
                }
                // take the next step
                count++;
                return innerIterator.next();
            }
        }

        @Override
        public long estimatedHeapUsage() {
            return LIST_SLICE_SHALLOW_SIZE + inner.estimatedHeapUsage();
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return inner.itemValueRepresentation();
        }
    }

    private static final long REVERSED_LIST_SHALLOW_SIZE = shallowSizeOfInstance(ReversedList.class);

    static final class ReversedList extends ListValue {
        private final ListValue inner;
        private final int listDepth;

        ReversedList(ListValue inner) {
            this.listDepth = inner.listDepth() + 1;
            this.inner = inner;
        }

        @Override
        protected int listDepth() {
            return listDepth;
        }

        @Override
        public ListValue reverse() {
            return this.inner;
        }

        @Override
        public IterationPreference iterationPreference() {
            return inner.iterationPreference();
        }

        @Override
        public Iterator<AnyValue> iterator() {
            // NOTE: this is dangerous since the underlying may not prefer random access
            return randomAccessIterator();
        }

        @Override
        public long actualSize() {
            return inner.actualSize();
        }

        @Override
        public boolean isEmpty() {
            return inner.isEmpty();
        }

        @Override
        public Value ternaryContains(AnyValue value) {
            return inner.ternaryContains(value);
        }

        @Override
        public AnyValue value(long offset) {
            return inner.value(actualSize() - 1 - offset);
        }

        @Override
        public long estimatedHeapUsage() {
            return REVERSED_LIST_SHALLOW_SIZE + inner.estimatedHeapUsage();
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return inner.itemValueRepresentation();
        }
    }

    private static final long CONCAT_LIST_SHALLOW_SIZE = shallowSizeOfInstance(ConcatList.class);

    static final class ConcatList extends ListValue {
        private final ListValue[] lists;
        private final ValueRepresentation itemValueRepresentation;
        private volatile long size = -1;
        private final IterationPreference iterationPreference;

        ConcatList(ListValue[] lists) {
            ValueRepresentation representation = ValueRepresentation.ANYTHING;
            var pref = lists.length < 10 ? RANDOM_ACCESS : ITERATION;
            int depth = 0;
            for (ListValue list : lists) {
                representation = representation.coerce(list.itemValueRepresentation());
                if (list.iterationPreference() == ITERATION) {
                    pref = ITERATION;
                }
                if (list.listDepth() > depth) {
                    depth = list.listDepth();
                }
            }
            this.iterationPreference = pref;
            this.itemValueRepresentation = representation;
            this.lists = lists;
            this.listDepth = depth + 1;
        }

        private final int listDepth;

        @Override
        protected int listDepth() {
            return listDepth;
        }

        @Override
        public IterationPreference iterationPreference() {
            return iterationPreference;
        }

        @Override
        protected long compactInto(AnyValue[] array, int fromInclusive) {
            long payloadSize = 0;
            int start = fromInclusive;
            for (var list : lists) {
                payloadSize += list.compactInto(array, start);
                // Proven bounded after appendAll validation (total <= MAX_ARRAY_SIZE), but fail fast
                // rather than wrap if ever misused.
                start = Math.addExact(start, list.intSize());
            }
            return payloadSize;
        }

        @Override
        public Iterator<AnyValue> iterator() {
            if (iterationPreference == RANDOM_ACCESS) {
                return randomAccessIterator();
            } else {
                return new ConcatListIterator();
            }
        }

        @Override
        public long actualSize() {
            if (size < 0) {
                long s = 0;
                for (ListValue list : lists) {
                    try {
                        s = Math.addExact(s, list.actualSize());
                    } catch (java.lang.ArithmeticException e) {
                        throw ArithmeticException.numericValueOutOfRangeWithCause(
                                s + "+" + list.actualSize(), "+", e);
                    }
                }
                size = s;
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            for (ListValue list : lists) {
                if (!list.isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public AnyValue value(long offset) {
            for (ListValue list : lists) {
                long size = list.actualSize();
                if (offset < size) {
                    return list.value(offset);
                }
                offset -= size;
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public long estimatedHeapUsage() {
            long s = 0;
            for (ListValue list : lists) {
                s += list.estimatedHeapUsage();
            }
            return CONCAT_LIST_SHALLOW_SIZE + s;
        }

        @Override
        public long estimatedHeapUsage(HeapEstimatorCache estimatorCache) {
            long s = 0;
            for (ListValue list : lists) {
                s += list.estimatedHeapUsage(estimatorCache);
            }
            return CONCAT_LIST_SHALLOW_SIZE + s;
        }

        @Override
        public ListValue appendAll(ListValue other) {
            var newSize = lists.length + 1;
            var newArray = new ListValue[newSize];
            System.arraycopy(lists, 0, newArray, 0, lists.length);
            newArray[lists.length] = other;
            return new ConcatList(newArray);
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return itemValueRepresentation;
        }

        private final class ConcatListIterator extends PrefetchingIterator<AnyValue> {
            private int index = 1;
            private Iterator<AnyValue> inner = lists[0].iterator();

            @Override
            protected AnyValue fetchNextOrNull() {
                while (true) {
                    if (inner.hasNext()) {
                        return inner.next();
                    } else if (index < lists.length) {
                        inner = lists[index++].iterator();
                    } else {
                        return null;
                    }
                }
            }
        }
    }

    private static final long APPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance(AppendList.class);

    public static final class AppendList extends ListValue {
        private final ListValue base;
        private final AnyValue appended;
        private volatile long size;
        private volatile long memoizedEstimatedHeapUsage;
        private static final long NOT_MEMOIZED = -1L;

        AppendList(ListValue base, AnyValue appended) {
            this.listDepth = base.listDepth() + 1;
            this.base = base;
            this.appended = appended;
            this.size = NOT_MEMOIZED;
            this.memoizedEstimatedHeapUsage = NOT_MEMOIZED;
        }

        private final int listDepth;

        @Override
        protected int listDepth() {
            return listDepth;
        }

        @Override
        public ArrayValue toStorableArray() {
            if (base instanceof ArrayValueListValue) {
                ArrayValue array = ((ArrayValueListValue) base).array;
                if (array.hasCompatibleType(appended)) {
                    return array.copyWithAppended(appended);
                }
            }
            return super.toStorableArray();
        }

        // ReversedList uses random access iteration which results in O(n²) iteration time for a linked list, so
        // this override trades O(n) iteration time for an additional O(n) construction time
        @Override
        public ListValue reverse() {
            return base.reverse().prepend(appended);
        }

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        @Override
        public long actualSize() {
            if (size == NOT_MEMOIZED) {
                long baseSize = base.actualSize();
                try {
                    size = Math.addExact(baseSize, 1L);
                } catch (java.lang.ArithmeticException e) {
                    throw ArithmeticException.numericValueOutOfRangeWithCause(baseSize + "+1", "+", e);
                }
            }
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Value ternaryContains(AnyValue value) {
            boolean undefinedEquality = false;
            ListValue baseList = this;
            while (baseList instanceof AppendList appendList) {
                Equality baseEquality = appendList.appended.ternaryEquals(value);
                if (baseEquality == Equality.TRUE) {
                    return BooleanValue.TRUE;
                } else {
                    if (baseEquality == Equality.UNDEFINED && !undefinedEquality) {
                        undefinedEquality = true;
                    }
                    baseList = appendList.base;
                }
            }
            Value baseContains = baseList.ternaryContains(value);
            if (baseContains == BooleanValue.FALSE) {
                return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
            } else {
                return baseContains;
            }
        }

        @Override
        public AnyValue value(long offset) {
            long size = base.actualSize();
            if (offset < size) {
                return base.value(offset);
            } else if (offset == size) {
                return appended;
            } else {
                throw new IndexOutOfBoundsException(offset + " is outside range " + size);
            }
        }

        @Override
        public void forEach(Consumer<? super AnyValue> consumer) {
            base.forEach(consumer);
            consumer.accept(appended);
        }

        @Override
        public AnyValue last() {
            return appended;
        }

        @Override
        protected long compactInto(AnyValue[] array, int fromInclusive) {
            array[Math.addExact(fromInclusive, base.intSize())] = appended;
            return appended.estimatedHeapUsage() + base.compactInto(array, fromInclusive);
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return switch (base.iterationPreference()) {
                case RANDOM_ACCESS -> randomAccessIterator();
                case ITERATION -> Iterators.appendTo(base.iterator(), appended);
            };
        }

        @Override
        public long estimatedHeapUsage() {
            long tmp = memoizedEstimatedHeapUsage;
            if (tmp == NOT_MEMOIZED) {
                tmp = APPEND_LIST_SHALLOW_SIZE + base.estimatedHeapUsage() + appended.estimatedHeapUsage();
                memoizedEstimatedHeapUsage = tmp;
            }
            return tmp;
        }

        @Override
        public long estimatedHeapUsage(HeapEstimatorCache estimatorCache) {
            long estimate = memoizedEstimatedHeapUsage;
            if (estimate == NOT_MEMOIZED) {
                estimate = APPEND_LIST_SHALLOW_SIZE
                        + base.estimatedHeapUsage(estimatorCache)
                        + appended.estimatedHeapUsage(estimatorCache);
                memoizedEstimatedHeapUsage = estimate;
            }
            return estimatorCache.estimatedHeapUsage(this, estimate);
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            if (base.isEmpty()) {
                return appended.valueRepresentation();
            } else {
                return base.itemValueRepresentation().coerce(appended.valueRepresentation());
            }
        }
    }

    private static final long PREPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance(PrependList.class);

    static final class PrependList extends ListValue {
        private final ListValue base;
        private final AnyValue prepended;
        private volatile long size;
        private volatile long memoizedEstimatedHeapUsage;
        private static final long NOT_MEMOIZED = -1L;

        PrependList(ListValue base, AnyValue prepended) {
            this.listDepth = base.listDepth() + 1;
            this.base = base;
            this.prepended = prepended;
            this.size = NOT_MEMOIZED;
            this.memoizedEstimatedHeapUsage = NOT_MEMOIZED;
        }

        private final int listDepth;

        @Override
        protected int listDepth() {
            return listDepth;
        }

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        // ReversedList uses random access iteration which results in O(n²) iteration time for a linked list, so
        // this override trades O(n) iteration time for an additional O(n) construction time
        @Override
        public ListValue reverse() {
            return base.reverse().append(prepended);
        }

        @Override
        public long actualSize() {
            if (size == NOT_MEMOIZED) {
                long baseSize = base.actualSize();
                try {
                    size = Math.addExact(baseSize, 1L);
                } catch (java.lang.ArithmeticException e) {
                    throw ArithmeticException.numericValueOutOfRangeWithCause(baseSize + "+1", "+", e);
                }
            }
            return size;
        }

        @Override
        public AnyValue head() {
            return prepended;
        }

        @Override
        public void forEach(Consumer<? super AnyValue> consumer) {
            consumer.accept(prepended);
            ListValue baseList = base;
            while (baseList instanceof PrependList prependList) {
                consumer.accept(prependList.prepended);
                baseList = prependList.base;
            }
            baseList.forEach(consumer);
        }

        @Override
        public Value ternaryContains(AnyValue value) {
            boolean undefinedEquality = false;
            ListValue baseList = this;
            while (baseList instanceof PrependList prependList) {
                Equality baseEquality = prependList.prepended.ternaryEquals(value);
                if (baseEquality == Equality.TRUE) {
                    return BooleanValue.TRUE;
                } else {
                    if (baseEquality == Equality.UNDEFINED && !undefinedEquality) {
                        undefinedEquality = true;
                    }
                    baseList = prependList.base;
                }
            }
            Value baseContains = baseList.ternaryContains(value);
            if (baseContains == BooleanValue.FALSE) {
                return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
            } else {
                return baseContains;
            }
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public AnyValue value(long offset) {
            if (offset == 0) {
                return prepended;
            } else if (offset - 1L < base.actualSize()) {
                return base.value(offset - 1);
            } else {
                throw new IndexOutOfBoundsException(offset + " is outside range " + size);
            }
        }

        @Override
        protected long compactInto(AnyValue[] array, int fromInclusive) {
            array[fromInclusive] = prepended;
            return prepended.estimatedHeapUsage() + base.compactInto(array, Math.addExact(fromInclusive, 1));
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return switch (base.iterationPreference()) {
                case RANDOM_ACCESS -> randomAccessIterator();
                case ITERATION -> Iterators.prependTo(base.iterator(), prepended);
            };
        }

        @Override
        public long estimatedHeapUsage() {
            long tmp = memoizedEstimatedHeapUsage;
            if (tmp == NOT_MEMOIZED) {
                tmp = PREPEND_LIST_SHALLOW_SIZE + base.estimatedHeapUsage() + prepended.estimatedHeapUsage();
                memoizedEstimatedHeapUsage = tmp;
            }
            return tmp;
        }

        @Override
        public long estimatedHeapUsage(HeapEstimatorCache estimatorCache) {
            long estimate = memoizedEstimatedHeapUsage;
            if (estimate == NOT_MEMOIZED) {
                estimate = PREPEND_LIST_SHALLOW_SIZE
                        + base.estimatedHeapUsage(estimatorCache)
                        + prepended.estimatedHeapUsage(estimatorCache);
                memoizedEstimatedHeapUsage = estimate;
            }
            return estimatorCache.estimatedHeapUsage(this, estimate);
        }

        @Override
        public ArrayValue toStorableArray() {
            if (base instanceof ArrayValueListValue) {
                ArrayValue array = ((ArrayValueListValue) base).array;
                if (array.hasCompatibleType(prepended)) {
                    return array.copyWithPrepended(prepended);
                }
            }
            return super.toStorableArray();
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            if (base.isEmpty()) {
                return prepended.valueRepresentation();
            } else {
                return base.itemValueRepresentation().coerce(prepended.valueRepresentation());
            }
        }
    }

    public boolean nonEmpty() {
        return !isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(getTypeName()).append('{');
        long size = actualSize();
        long i = 0;
        for (; i < size - 1; i++) {
            sb.append(value(i));
            sb.append(", ");
        }
        if (size > 0) {
            sb.append(value(i));
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean isSequenceValue() {
        return true;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapSequence(this);
    }

    @Override
    public boolean equals(VirtualValue other) {
        return other != null && other.isSequenceValue() && equals((SequenceValue) other);
    }

    protected Iterator<AnyValue> randomAccessIterator() {
        var size = actualSize();
        if (size <= Integer.MAX_VALUE) {
            return new IntRandomAccessIterator(this, (int) size);
        } else {
            return new LongRandomAccessIterator(this, size);
        }
    }

    private static class LongRandomAccessIterator implements Iterator<AnyValue> {
        private final ListValue listValue;
        private final long actualSize;
        private long count;

        private LongRandomAccessIterator(ListValue listValue, long actualSize) {
            this.listValue = listValue;
            this.actualSize = actualSize;
        }

        @Override
        public boolean hasNext() {
            return count < actualSize;
        }

        @Override
        public AnyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return listValue.value(count++);
        }
    }

    private static class IntRandomAccessIterator implements Iterator<AnyValue> {
        private final ListValue listValue;
        private final int actualSize;
        private long count;

        private IntRandomAccessIterator(ListValue listValue, int actualSize) {
            this.listValue = listValue;
            this.actualSize = actualSize;
        }

        @Override
        public boolean hasNext() {
            return count < actualSize;
        }

        @Override
        public AnyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return listValue.value(count++);
        }
    }

    @Override
    public VirtualValueGroup valueGroup() {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int intSize() {
        try {
            return Numbers.safeCastLongToInt(actualSize());
        } catch (java.lang.ArithmeticException e) {
            throw ArithmeticException.numericValueOutOfRangeWithCause(String.valueOf(actualSize()), "Long to Int", e);
        }
    }

    @Override
    public int unsafeCompareTo(VirtualValue other, Comparator<AnyValue> comparator) {
        ListValue otherList = (ListValue) other;
        return compareToSequence(otherList, comparator);
    }

    @Override
    public Comparison unsafeTernaryCompareTo(VirtualValue other, TernaryComparator<AnyValue> comparator) {
        ListValue otherList = (ListValue) other;
        return ternaryCompareToSequence(otherList, comparator);
    }

    public AnyValue[] asArray() {
        return switch (iterationPreference()) {
            case RANDOM_ACCESS -> randomAccessAsArray();
            case ITERATION -> iterationAsArray();
        };
    }

    @Override
    protected int computeHashToMemoize() {
        return switch (iterationPreference()) {
            case RANDOM_ACCESS -> randomAccessComputeHash();
            case ITERATION -> iterationComputeHash();
        };
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        switch (iterationPreference()) {
            case RANDOM_ACCESS -> randomAccessWriteTo(writer);
            case ITERATION -> iterationWriteTo(writer);
        }
    }

    public ListValue slice() {
        return slice(0L, actualSize());
    }

    public ListValue slice(long fromInclusive) {
        return slice(fromInclusive, actualSize());
    }

    public ListValue slice(long fromInclusive, long toExclusive) {
        long f = Math.max(fromInclusive, 0L);
        long t = Math.min(toExclusive, actualSize());

        if (f >= t) {
            return EMPTY_LIST;
        }

        // with Prepend/AppendList we can simply unwrap them to get the inner list rather than creating a view over them
        var list = this;
        while (true) {
            if (list instanceof PrependList prependList && f > 0) {
                // if we are slicing from the tail of the list, we can discard a prepended value.
                list = prependList.base;

                // since we unwrapped a prepended list, we decrement both slice offsets which were relative to the start
                // of it
                f--;
                t--;
            } else if (list instanceof AppendList appendList && t < list.actualSize()) {
                // if we are slicing up to less than the total list length, we can discard an appended value.

                list = appendList.base;
                // unwrapping an appended value does not affect the slice offsets since they are relative to the start
                // of the list, not the end
            } else {
                break;
            }
        }

        if (f == 0 && t == list.actualSize()) {
            return list;
        }

        return new ListSlice(list, f, t);
    }

    public ListValue tail() {
        return drop(1);
    }

    public ListValue drop(long n) {
        return slice(n, actualSize());
    }

    public ListValue take(long n) {
        return slice(0, n);
    }

    @Override
    public ListValue reverse() {
        return new ReversedList(this);
    }

    @Override
    public ListValue asListValue() {
        return this;
    }

    /**
     * Compacts the contents of the list into the provided array starting from the specified index.
     *
     * @param array         The destination array into which list elements will be copied.
     * @param fromInclusive The starting index in the destination array for copying elements.
     * @return The total estimated heap usage of the copied elements in the list.
     */
    protected long compactInto(AnyValue[] array, int fromInclusive) {
        int i = fromInclusive;
        long payloadSize = 0L;
        for (var x : this) {
            array[i] = x;
            payloadSize += x.estimatedHeapUsage();
            i++;
        }
        return payloadSize;
    }

    public ListValue append(AnyValue value) {
        if (listDepth() < LIST_DEPTH_COMPACTION_THRESHOLD) {
            return new AppendList(this, value);
        }
        int total = checkedCompactionLength(actualSize(), 1L);
        var values = new AnyValue[total];
        values[values.length - 1] = value;

        long payloadSize = value.estimatedHeapUsage() + compactInto(values, 0);

        return new ArrayListValue(values, payloadSize, itemValueRepresentation().coerce(value.valueRepresentation()));
    }

    public ListValue prepend(AnyValue value) {
        if (listDepth() < LIST_DEPTH_COMPACTION_THRESHOLD) {
            return new PrependList(this, value);
        }
        int total = checkedCompactionLength(actualSize(), 1L);
        var values = new AnyValue[total];
        values[0] = value;

        long payloadSize = value.estimatedHeapUsage() + compactInto(values, 1);

        return new ArrayListValue(values, payloadSize, itemValueRepresentation().coerce(value.valueRepresentation()));
    }

    public ListValue appendAll(ListValue other) {
        if (other.isEmpty()) {
            return this;
        }

        if (other.actualSize() == 1L) {
            return append(other.head());
        }

        if (Math.max(listDepth(), other.listDepth()) < LIST_DEPTH_COMPACTION_THRESHOLD) {
            return new ConcatList(new ListValue[] {this, other});
        }

        long thisLong = actualSize();
        long otherLong = other.actualSize();
        long candidate;
        try {
            candidate = Math.addExact(thisLong, otherLong);
        } catch (java.lang.ArithmeticException e) {
            throw ArithmeticException.numericValueOutOfRangeWithCause(thisLong + "+" + otherLong, "+", e);
        }
        if (candidate > ArrayUtil.MAX_ARRAY_SIZE) {
            throw InvalidArgumentException.listTooLarge(candidate, ArrayUtil.MAX_ARRAY_SIZE);
        }
        int thisLength = (int) thisLong; // proven bounded: thisLong <= candidate <= MAX_ARRAY_SIZE
        var values = new AnyValue[(int) candidate];

        long thisSize = this.compactInto(values, 0);
        long valueSize = other.compactInto(values, thisLength);

        return new ArrayListValue(
                values, thisSize + valueSize, itemValueRepresentation().coerce(other.itemValueRepresentation()));
    }

    private static int checkedCompactionLength(long baseSize, long extra) {
        long candidate;
        try {
            candidate = Math.addExact(baseSize, extra);
        } catch (java.lang.ArithmeticException e) {
            throw ArithmeticException.numericValueOutOfRangeWithCause(baseSize + "+" + extra, "+", e);
        }
        if (candidate > ArrayUtil.MAX_ARRAY_SIZE) {
            // MAX_ARRAY_SIZE < Integer.MAX_VALUE, so this also covers all int-overflowing candidates.
            throw InvalidArgumentException.listTooLarge(candidate, ArrayUtil.MAX_ARRAY_SIZE);
        }
        return (int) candidate;
    }

    @Override
    public ListValue insertAt(int index, AnyValue value) {
        if (index == 0) {
            return prepend(value);
        } else if (index == actualSize()) {
            return append(value);
        } else {
            return slice(0, index).append(value).appendAll(slice(index, actualSize()));
        }
    }

    @Override
    public ListValue remove(int index) {
        if (index == 0) {
            return slice(1, actualSize());
        } else if (index == actualSize()) {
            return slice(0, actualSize() - 1);
        } else {
            return slice(0, index).appendAll(slice(index + 1, actualSize()));
        }
    }

    public ListValue distinct() {
        long keptValuesHeapSize = 0;
        Set<AnyValue> seen = new HashSet<>();
        List<AnyValue> kept = new ArrayList<>();
        ValueRepresentation representation = ValueRepresentation.ANYTHING;
        for (AnyValue value : this) {
            if (seen.add(value)) {
                kept.add(value);
                keptValuesHeapSize += value.estimatedHeapUsage();
            }
            representation = representation.coerce(value.valueRepresentation());
        }
        return new JavaListListValue(kept, keptValuesHeapSize, representation);
    }

    public ArrayValue toStorableArray() {
        if (isEmpty()) {
            return Values.EMPTY_TEXT_ARRAY;
        } else {
            return itemValueRepresentation().arrayOf(this);
        }
    }

    private AnyValue[] iterationAsArray() {
        List<AnyValue> values = new ArrayList<>();
        for (AnyValue value : this) {
            values.add(value);
        }
        return values.toArray(new AnyValue[0]);
    }

    private AnyValue[] randomAccessAsArray() {
        int size = SequenceValue.checkedArrayLength(this, "asArray");
        AnyValue[] values = new AnyValue[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = value(i);
        }
        return values;
    }

    private int randomAccessComputeHash() {
        int hashCode = 1;
        int size = intSize();
        for (int i = 0; i < size; i++) {
            hashCode = HASH_CONSTANT * hashCode + value(i).hashCode();
        }
        return hashCode;
    }

    private int iterationComputeHash() {
        int hashCode = 1;
        for (AnyValue value : this) {
            hashCode = HASH_CONSTANT * hashCode + value.hashCode();
        }
        return hashCode;
    }

    private <E extends Exception> void randomAccessWriteTo(AnyValueWriter<E> writer) throws E {
        int size = intSize();
        writer.beginList(size);
        for (int i = 0; i < size; i++) {
            value(i).writeTo(writer);
        }
        writer.endList();
    }

    private <E extends Exception> void iterationWriteTo(AnyValueWriter<E> writer) throws E {
        writer.beginList(intSize());
        for (AnyValue value : this) {
            value.writeTo(writer);
        }
        writer.endList();
    }
}
