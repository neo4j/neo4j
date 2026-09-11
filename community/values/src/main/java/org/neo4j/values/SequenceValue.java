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
package org.neo4j.values;

import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Iterator;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;

/**
 * Values that represent sequences of values (such as Lists or Arrays) need to implement this interface.
 * Thus we can get an equality check that is based on the values (e.g. List.equals(ArrayValue) )
 * Values that implement this interface also need to overwrite isSequence() to return true!
 *
 * Note that even though SequenceValue extends Iterable iterating over the sequence using iterator() might not be the
 * most performant method. Branch using iterationPreference() in performance critical code paths.
 */
public interface SequenceValue extends Iterable<AnyValue> {
    /**
     * The preferred way to iterate this sequence. Preferred in this case means the method which is expected to be
     * the most performant.
     */
    enum IterationPreference {
        RANDOM_ACCESS,
        ITERATION
    }

    /**
     * @return the number of elements of the collection.
     */
    long actualSize();

    /**
     * @return the number of elements of the collection.
     * @throws ArithmeticException if the size doesn't fit into an int.
     */
    int intSize();

    /**
     * Checked narrowing of a logical sequence cardinality to a physical Java/storable array length.
     * <p>
     * Logical cardinality lives in {@code [0, Long.MAX_VALUE]}. A Java array (and therefore any storable
     * property array) cannot exceed {@link ArrayUtil#MAX_ARRAY_SIZE}. This helper enforces, in O(1) and
     * before any allocation or iteration:
     * <ol>
     *   <li>exact {@code long -> int} narrowing (no modular wrap, no clamp, no saturation), then</li>
     *   <li>the Neo4j/Java physical array capacity limit.</li>
     * </ol>
     * Success guarantees the returned {@code int} equals the exact logical size. Failure throws a
     * client-facing error that names the exact logical cardinality, never a wrapped {@code int}.
     * Once cardinality exceeds a physical maximum it can never become valid again for larger sizes.
     *
     * @param value the sequence whose logical size should be materialized
     * @param operation GQL operation name used for the numeric-overflow error (e.g. {@code "range()"})
     * @return the exact array length as {@code int}
     */
    static int checkedArrayLength(SequenceValue value, String operation) {
        long logicalSize = value.actualSize();
        if (logicalSize < 0L || logicalSize > Integer.MAX_VALUE) {
            throw ArithmeticException.numericValueOutOfRange(String.valueOf(logicalSize), operation);
        }
        int size = (int) logicalSize; // now mathematically proven exact
        if (size > ArrayUtil.MAX_ARRAY_SIZE) {
            throw InvalidArgumentException.listTooLarge(logicalSize, ArrayUtil.MAX_ARRAY_SIZE);
        }
        return size;
    }

    default boolean isEmpty() {
        return actualSize() == 0L;
    }

    default AnyValue head() {
        if (isEmpty()) {
            return NO_VALUE;
        }

        return value(0);
    }

    default AnyValue last() {
        long size = actualSize();
        if (size == 0L) {
            return NO_VALUE;
        }
        return value(size - 1);
    }

    AnyValue value(long offset);

    @Override
    Iterator<AnyValue> iterator();

    IterationPreference iterationPreference();

    ListValue reverse(); // SequenceValue does not extend AnyValue so ListValue is a better return type

    ListValue asListValue();

    String prettyPrint();

    String getTypeName();

    default boolean equals(SequenceValue other) {
        if (other == null) {
            return false;
        }

        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if (pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS) {
            return equalsUsingRandomAccess(this, other);
        } else {
            return equalsUsingIterators(this, other);
        }
    }

    static boolean equalsUsingRandomAccess(SequenceValue a, SequenceValue b) {
        int i = 0;
        boolean areEqual = a.intSize() == b.intSize();

        while (areEqual && i < a.intSize()) {
            areEqual = a.value(i).equals(b.value(i));
            i++;
        }
        return areEqual;
    }

    static Equality ternaryEqualsUsingRandomAccess(SequenceValue a, SequenceValue b) {
        int length = a.intSize();
        if (length != b.intSize()) {
            return Equality.FALSE;
        }
        int i = 0;
        Equality equivalenceResult = Equality.TRUE;
        while (i < length) {
            Equality areEqual = a.value(i).ternaryEquals(b.value(i));
            if (areEqual == Equality.UNDEFINED) {
                equivalenceResult = Equality.UNDEFINED;
            } else if (areEqual == Equality.FALSE) {
                return Equality.FALSE;
            }
            i++;
        }

        return equivalenceResult;
    }

    static boolean equalsUsingIterators(SequenceValue a, SequenceValue b) {
        boolean areEqual = true;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while (areEqual && aIterator.hasNext() && bIterator.hasNext()) {
            areEqual = aIterator.next().equals(bIterator.next());
        }

        return areEqual && aIterator.hasNext() == bIterator.hasNext();
    }

    static Equality ternaryEqualsUsingIterators(SequenceValue a, SequenceValue b) {
        Equality equivalenceResult = Equality.TRUE;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();
        while (aIterator.hasNext() && bIterator.hasNext()) {
            Equality areEqual = aIterator.next().ternaryEquals(bIterator.next());
            if (areEqual == Equality.UNDEFINED) {
                equivalenceResult = Equality.UNDEFINED;
            } else if (areEqual == Equality.FALSE) {
                return Equality.FALSE;
            }
        }

        return !aIterator.hasNext() && !bIterator.hasNext() ? equivalenceResult : Equality.FALSE;
    }

    default int compareToSequence(SequenceValue other, Comparator<AnyValue> comparator) {
        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if (pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS) {
            return compareUsingRandomAccess(this, other, comparator);
        } else {
            return compareUsingIterators(this, other, comparator);
        }
    }

    default Comparison ternaryCompareToSequence(SequenceValue other, TernaryComparator<AnyValue> comparator) {
        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if (pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS) {
            return ternaryCompareUsingRandomAccess(this, other, comparator);
        } else {
            return ternaryCompareUsingIterators(this, other, comparator);
        }
    }

    static int compareUsingRandomAccess(SequenceValue a, SequenceValue b, Comparator<AnyValue> comparator) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.intSize(), b.intSize());

        while (x == 0 && i < length) {
            x = comparator.compare(a.value(i), b.value(i));
            i++;
        }

        if (x == 0) {
            x = a.intSize() - b.intSize();
        }

        return x;
    }

    static int compareUsingIterators(SequenceValue a, SequenceValue b, Comparator<AnyValue> comparator) {
        int x = 0;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while (x == 0 && aIterator.hasNext() && bIterator.hasNext()) {
            x = comparator.compare(aIterator.next(), bIterator.next());
        }

        if (x == 0) {
            x = Boolean.compare(aIterator.hasNext(), bIterator.hasNext());
        }

        return x;
    }

    static Comparison ternaryCompareUsingRandomAccess(
            SequenceValue a, SequenceValue b, TernaryComparator<AnyValue> comparator) {
        Comparison cmp = Comparison.EQUAL;
        int i = 0;
        int length = Math.min(a.intSize(), b.intSize());
        while (cmp == Comparison.EQUAL && i < length) {
            cmp = comparator.ternaryCompare(a.value(i), b.value(i));
            i++;
        }
        if (cmp == Comparison.EQUAL) {
            cmp = Comparison.from(a.intSize() - b.intSize());
        }

        return cmp;
    }

    static Comparison ternaryCompareUsingIterators(
            SequenceValue a, SequenceValue b, TernaryComparator<AnyValue> comparator) {
        Comparison cmp = Comparison.EQUAL;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while (cmp == Comparison.EQUAL && aIterator.hasNext() && bIterator.hasNext()) {
            cmp = comparator.ternaryCompare(aIterator.next(), bIterator.next());
        }

        if (cmp == Comparison.EQUAL) {
            cmp = Comparison.from(Boolean.compare(aIterator.hasNext(), bIterator.hasNext()));
        }

        return cmp;
    }

    default Equality ternaryEquality(SequenceValue other) {
        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if (pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS) {
            return ternaryEqualsUsingRandomAccess(this, other);
        } else {
            return ternaryEqualsUsingIterators(this, other);
        }
    }

    default ListValue flatten(final int depth) {
        // Note, stack safe implementation.
        // Note, possible to optimize for:
        // - Lists that support fast random access
        // - Lists that do not need flattening (could be checked with ListValue#itemRepresentation)
        final var builder = ListValueBuilder.newListBuilder();
        final var iterators = new ArrayDeque<Iterator<AnyValue>>(2);
        iterators.push(iterator());

        int currentDepth = depth - 1;
        while (!iterators.isEmpty()) {
            currentDepth += 1;
            var iterator = iterators.pop(); // Note, `iterator` reference can change.
            while (iterator.hasNext()) {
                final var value = iterator.next();
                if (currentDepth > 0 && value instanceof SequenceValue sequenceValue) {
                    currentDepth -= 1;
                    iterators.push(iterator);
                    iterator = sequenceValue.iterator();
                } else {
                    builder.add(value);
                }
            }
        }
        return builder.build();
    }

    default ListValue insertAt(int index, AnyValue value) {
        return asListValue().insertAt(index, value);
    }

    default ListValue remove(int index) {
        return asListValue().remove(index);
    }

    default int indexOf(AnyValue value) {
        int index = -1;
        int currentIndex = 0;
        Iterator<AnyValue> iterator = iterator();
        while (index == -1 && iterator.hasNext()) {
            if (iterator.next().ternaryEquals(value) == Equality.TRUE) {
                index = currentIndex;
            }
            currentIndex = currentIndex + 1;
        }
        return index;
    }
}
