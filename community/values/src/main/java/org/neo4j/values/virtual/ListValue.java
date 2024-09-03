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
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.github.jamm.Unmetered;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.helpers.Numbers;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
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
            throw new CypherTypeException(
                    "Collections containing relationship values can not be stored in properties.");
        }

        @Override
        public long actualSize() {
            return list.size();
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return ValueRepresentation.ANYTHING;
        }

        @Override
        public AnyValue value(long offset) {
            Objects.checkIndex(0, list.size());
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
        public long actualSize() {
            return values.length;
        }

        @Override
        public AnyValue value(long offset) {
            Objects.checkIndex(0, values.length);
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
            if (values instanceof ArrayList<?>) {
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
            Objects.checkIndex(0, values.size());
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
        private final long from;
        private final long to;

        ListSlice(ListValue inner, long from, long to) {
            assert from >= 0;
            assert to <= inner.actualSize();
            assert from <= to;
            this.inner = inner;
            this.from = from;
            this.to = to;
        }

        @Override
        public IterationPreference iterationPreference() {
            return inner.iterationPreference();
        }

        @Override
        public long actualSize() {
            return to - from;
        }

        @Override
        public AnyValue value(long offset) {
            return inner.value(offset + from);
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return switch (inner.iterationPreference()) {
                case RANDOM_ACCESS -> super.iterator();
                case ITERATION -> new ListSliceIterator();
            };
        }

        private class ListSliceIterator extends PrefetchingIterator<AnyValue> {
            private int count;
            private final Iterator<AnyValue> innerIterator = inner.iterator();

            @Override
            protected AnyValue fetchNextOrNull() {
                // make sure we are at least at first element
                while (count < from && innerIterator.hasNext()) {
                    innerIterator.next();
                    count++;
                }
                // check if we are done
                if (count < from || count >= to || !innerIterator.hasNext()) {
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

        ReversedList(ListValue inner) {
            this.inner = inner;
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

        ConcatList(ListValue[] lists) {
            ValueRepresentation representation = ValueRepresentation.ANYTHING;
            for (ListValue list : lists) {
                representation = representation.coerce(list.itemValueRepresentation());
            }
            this.itemValueRepresentation = representation;
            this.lists = lists;
        }

        @Override
        public IterationPreference iterationPreference() {
            return ITERATION;
        }

        @Override
        public long actualSize() {
            if (size < 0) {
                int s = 0;
                for (ListValue list : lists) {
                    s += list.actualSize();
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
        public ListValue appendAll(ListValue value) {
            var newSize = lists.length + 1;
            var newArray = new ListValue[newSize];
            System.arraycopy(lists, 0, newArray, 0, lists.length);
            newArray[lists.length] = value;
            return new ConcatList(newArray);
        }

        @Override
        public ValueRepresentation itemValueRepresentation() {
            return itemValueRepresentation;
        }
    }

    private static final long APPEND_LIST_SHALLOW_SIZE = shallowSizeOfInstance(AppendList.class);

    public static final class AppendList extends ListValue {
        private final ListValue base;
        private final AnyValue appended;
        private long size;
        private volatile long memoizedEstimatedHeapUsage;
        private static final long NOT_MEMOIZED = -1L;

        AppendList(ListValue base, AnyValue appended) {
            this.base = base;
            this.appended = appended;
            this.size = NOT_MEMOIZED;
            this.memoizedEstimatedHeapUsage = NOT_MEMOIZED;
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

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        @Override
        public long actualSize() {
            if (size == NOT_MEMOIZED) {
                size = base.actualSize() + 1;
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
            } else if (offset < size + 1L) {
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
        public Iterator<AnyValue> iterator() {
            return switch (base.iterationPreference()) {
                case RANDOM_ACCESS -> super.iterator();
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
            this.base = base;
            this.prepended = prepended;
            this.size = NOT_MEMOIZED;
            this.memoizedEstimatedHeapUsage = NOT_MEMOIZED;
        }

        @Override
        public IterationPreference iterationPreference() {
            return base.iterationPreference();
        }

        @Override
        public long actualSize() {
            if (size == NOT_MEMOIZED) {
                size = base.actualSize() + 1;
            }
            return size;
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
            } else if (offset < base.actualSize() + 1) {
                return base.value(offset - 1);
            } else {
                throw new IndexOutOfBoundsException(offset + " is outside range " + size);
            }
        }

        @Override
        public Iterator<AnyValue> iterator() {
            return switch (base.iterationPreference()) {
                case RANDOM_ACCESS -> super.iterator();
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
        int i = 0;
        for (; i < actualSize() - 1; i++) {
            sb.append(value(i));
            sb.append(", ");
        }
        if (actualSize() > 0) {
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

    public AnyValue head() {
        long size = actualSize();
        if (size == 0) {
            throw new NoSuchElementException("head of empty list");
        }
        return value(0);
    }

    public AnyValue last() {
        long size = actualSize();
        if (size == 0) {
            throw new NoSuchElementException("last of empty list");
        }
        return value(size - 1);
    }

    @Override
    public Iterator<AnyValue> iterator() {
        return new ListValueIterator();
    }

    private class ListValueIterator implements Iterator<AnyValue> {
        private long count;

        @Override
        public boolean hasNext() {
            return count < actualSize();
        }

        @Override
        public AnyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return value(count++);
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
            // TODO: STATUS_22003
            throw new ArithmeticException("numeric value out of range", e);
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

    public ListValue slice(int from, int to) {
        int f = Math.max(from, 0);
        int t = Math.min(to, intSize());
        if (f > t) {
            return EMPTY_LIST;
        } else {
            return new ListSlice(this, f, t);
        }
    }

    public ListValue tail() {
        return slice(1, intSize());
    }

    public ListValue drop(long n) {
        long size = actualSize();
        long start = Math.max(0, Math.min(n, size));
        return new ListSlice(this, start, size);
    }

    public ListValue take(int n) {
        long end = Math.max(0, Math.min(n, actualSize()));
        return new ListSlice(this, 0, end);
    }

    public ListValue reverse() {
        return new ReversedList(this);
    }

    public AppendList append(AnyValue value) {
        return new AppendList(this, value);
    }

    public ListValue prepend(AnyValue value) {
        return new PrependList(this, value);
    }

    public ListValue appendAll(ListValue value) {
        return new ConcatList(new ListValue[] {this, value});
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
        int size = 0;
        for (AnyValue value : this) {
            values.add(value);
            size++;
        }
        return values.toArray(new AnyValue[size]);
    }

    private AnyValue[] randomAccessAsArray() {
        int size = intSize();
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
