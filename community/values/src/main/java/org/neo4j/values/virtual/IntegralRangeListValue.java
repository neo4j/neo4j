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
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Iterator;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

public abstract class IntegralRangeListValue extends ListValue {
    private final long cardinality;

    protected IntegralRangeListValue(long cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public final IterationPreference iterationPreference() {
        return RANDOM_ACCESS;
    }

    @Override
    public ValueRepresentation itemValueRepresentation() {
        return ValueRepresentation.INT64;
    }

    @Override
    public final long actualSize() {
        return cardinality;
    }

    @Override
    public final int intSize() {
        return super.intSize();
    }

    public static IntegralRangeListValue rangeList(long start, long end, long step) {
        if (step == 0L) {
            throw InvalidArgumentException.zeroStepRange();
        }
        if (isInt(start) && isInt(end) && isInt(step)) {
            return new IntRangeListValue((int) start, (int) end, (int) step);
        } else {
            return new LongRangeListValue(start, end, step);
        }
    }

    private static boolean isInt(long value) {
        return (int) value == value;
    }

    /**
     * Exact mathematical cardinality of an inclusive integral range.
     * <p>
     * For {@code step > 0} with {@code start <= end}: {@code floor((end - start) / step) + 1}.
     * For {@code step < 0} with {@code start >= end}: {@code floor((start - end) / abs(step)) + 1}.
     * Otherwise {@code 0}.
     * <p>
     * Endpoint distances can span the full unsigned 64-bit domain {@code [0, 2^64 - 1]} even when the
     * final cardinality fits signed {@code long} (e.g. {@code range(MIN_VALUE, MAX_VALUE, MAX_VALUE)} has
     * distance {@code 2^64 - 1} but size {@code 3}). Therefore subtraction intentionally uses
     * two's-complement wrapping and is interpreted as unsigned, combined with
     * {@link Long#divideUnsigned(long, long)}. The fast path keeps ordinary signed division for the
     * overwhelmingly common case. A mathematical cardinality above {@link Long#MAX_VALUE} is not
     * representable by the logical sequence API or Cypher INTEGER and fails with the canonical numeric
     * overflow error naming the exact cardinality.
     */
    static long exactRangeCardinality(long start, long end, long step) {
        if (step == 0L) {
            throw InvalidArgumentException.zeroStepRange();
        }
        if ((step > 0L && start > end) || (step < 0L && start < end)) {
            return 0L;
        }
        // Wrapping subtraction: interpreted as unsigned it is the exact distance in [0, 2^64 - 1].
        long distance = step > 0L ? end - start : start - end;
        // For Long.MIN_VALUE negation still yields MIN_VALUE as signed, whose unsigned value 2^63 is exact.
        long strideMagnitude = step > 0L ? step : -step;
        long quotient;
        if (distance >= 0L && strideMagnitude > 0L) {
            quotient = distance / strideMagnitude;
        } else {
            quotient = Long.divideUnsigned(distance, strideMagnitude);
        }
        // cardinality = quotient + 1 is valid iff quotient <= Long.MAX_VALUE - 1 (unsigned compare).
        if (Long.compareUnsigned(quotient, Long.MAX_VALUE - 1L) > 0) {
            String exactCardinality =
                    new BigInteger(Long.toUnsignedString(quotient)).add(BigInteger.ONE).toString();
            throw ArithmeticException.numericValueOutOfRange(exactCardinality, "range()");
        }
        return quotient + 1L;
    }

    private static final class IntRangeListValue extends IntegralRangeListValue {
        private static final long INT_RANGE_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(IntRangeListValue.class);

        private final int start;
        private final int end;
        private final int step;

        IntRangeListValue(int start, int end, int step) {
            super(exactRangeCardinality(start, end, step));
            this.start = start;
            this.end = end;
            this.step = step;
        }

        public String toString() {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        @Override
        public ListValue reverse() {
            long n = actualSize();
            if (n == 0L) {
                if (step == Integer.MIN_VALUE) {
                    return rangeList(end, start, -(long) step);
                }
                return new IntRangeListValue(end, start, -step);
            }
            // Supplied `end` is not necessarily an element (e.g. range(5,10,3)=[5,8]).
            // Reverse must start at the actual last generated element, computed with
            // wrapping arithmetic which is exact mod 2^64 (result lies between start/end).
            long last = (long) start + (n - 1L) * (long) step;
            if (step == Integer.MIN_VALUE) {
                // -step does not fit int; widen through the normal factory (becomes long-backed).
                return rangeList(last, start, -(long) step);
            }
            return new IntRangeListValue((int) last, start, -step);
        }

        @Override
        public AnyValue value(long offset) {
            if (offset < 0L || offset >= actualSize()) {
                // TODO: this should be a GQL error and not java.lang.IndexOutOfBoundsException
                //      but changing it is semi-breaking.
                throw new IndexOutOfBoundsException();
            } else {
                return Values.longValue((long) start + offset * (long) step);
            }
        }

        @Override
        public Iterator<AnyValue> iterator() {
            long size = actualSize();
            if (size == 0L) {
                return Collections.emptyIterator();
            } else {
                return new Iterator<>() {
                    private long index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < size;
                    }

                    @Override
                    public AnyValue next() {
                        var result = Values.longValue((long) start + index * (long) step);
                        index++;
                        return result;
                    }
                };
            }
        }

        @Override
        protected int computeHashToMemoize() {
            int hashCode = 1;
            long current = start;
            // If the size is bigger than Integer.MAX_VALUE it will anyway
            // take forever to compute it so let's not bother
            int size = (int) Math.min(actualSize(), Integer.MAX_VALUE);
            for (int i = 0; i < size; i++, current += step) {
                hashCode = HASH_CONSTANT * hashCode + Long.hashCode(current);
            }
            return hashCode;
        }

        @Override
        public long estimatedHeapUsage() {
            return INT_RANGE_LIST_VALUE_SHALLOW_SIZE;
        }

        @Override
        public ArrayValue toStorableArray() {
            int size = SequenceValue.checkedArrayLength(this, "range()");
            long current = start;
            long[] array = new long[size];
            for (int i = 0; i < size; i++, current += step) {
                array[i] = current;
            }
            return Values.longArray(array);
        }
    }

    private static final class LongRangeListValue extends IntegralRangeListValue {
        private static final long LONG_RANGE_LIST_VALUE_SHALLOW_SIZE =
                shallowSizeOfInstance(LongRangeListValue.class);
        private final long start;
        private final long end;
        private final long step;

        LongRangeListValue(long start, long end, long step) {
            super(exactRangeCardinality(start, end, step));
            this.start = start;
            this.end = end;
            this.step = step;
        }

        @Override
        public String toString() {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        @Override
        public ListValue reverse() {
            if (step == Long.MIN_VALUE) {
                // Positive magnitude 2^63 is not representable as signed long; keep lazy reversed view.
                return super.reverse();
            }
            long n = actualSize();
            if (n == 0L) {
                return new LongRangeListValue(end, start, -step);
            }
            // See IntRangeListValue.reverse: start from actual last element, not supplied `end`.
            long last = start + (n - 1L) * step;
            return new LongRangeListValue(last, start, -step);
        }

        @Override
        public Iterator<AnyValue> iterator() {
            long size = actualSize();
            if (size == 0L) {
                return Collections.emptyIterator();
            } else {
                return new Iterator<>() {
                    private long index = 0;

                    @Override
                    public boolean hasNext() {
                        return index < size;
                    }

                    @Override
                    public AnyValue next() {
                        var result = Values.longValue(start + index * step);
                        index++;
                        return result;
                    }
                };
            }
        }

        @Override
        public AnyValue value(long offset) {
            if (offset < 0L || offset >= actualSize()) {
                // TODO: this should be a GQL error and not java.lang.IndexOutOfBoundsException
                //      but changing it is semi-breaking.
                throw new IndexOutOfBoundsException();
            } else {
                return Values.longValue(start + offset * step);
            }
        }

        @Override
        protected int computeHashToMemoize() {
            int hashCode = 1;
            long current = start;
            // If the size is bigger than Integer.MAX_VALUE it will anyway
            // take forever to compute it so let's not bother
            int size = (int) Math.min(actualSize(), Integer.MAX_VALUE);
            for (int i = 0; i < size; i++, current += step) {
                hashCode = HASH_CONSTANT * hashCode + Long.hashCode(current);
            }
            return hashCode;
        }

        @Override
        public long estimatedHeapUsage() {
            return LONG_RANGE_LIST_VALUE_SHALLOW_SIZE;
        }

        @Override
        public ArrayValue toStorableArray() {
            int size = SequenceValue.checkedArrayLength(this, "range()");
            long current = start;
            long[] array = new long[size];
            for (int i = 0; i < size; i++, current += step) {
                array[i] = current;
            }
            return Values.longArray(array);
        }
    }
}
