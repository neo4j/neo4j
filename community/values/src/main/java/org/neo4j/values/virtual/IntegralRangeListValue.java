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

import java.util.Collections;
import java.util.Iterator;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;

public abstract class IntegralRangeListValue extends ListValue {
    @Override
    public final IterationPreference iterationPreference() {
        return RANDOM_ACCESS;
    }

    @Override
    public ValueRepresentation itemValueRepresentation() {
        return ValueRepresentation.INT64;
    }

    public static IntegralRangeListValue rangeList(long start, long end, long step) {
        if (isInt(start) && isInt(end) && isInt(step)) {
            return new IntRangeListValue((int) start, (int) end, (int) step);
        } else {
            return new IntRangeListValue.LongRangeListValue(start, end, step);
        }
    }

    private static boolean isInt(long value) {
        return (int) value == value;
    }

    private static final class IntRangeListValue extends IntegralRangeListValue {
        private static final long INT_RANGE_LIST_VALUE_SHALLOW_SIZE = shallowSizeOfInstance(IntRangeListValue.class);

        private final int start;
        private final int end;
        private final int step;
        private int length = -1;

        IntRangeListValue(int start, int end, int step) {
            this.start = start;
            this.end = end;
            this.step = step;
        }

        public String toString() {
            return "Range(" + start + "..." + end + ", step = " + step + ")";
        }

        @Override
        public long actualSize() {
            return intSize();
        }

        @Override
        public int intSize() {
            if (length == -1) {
                int l = (end - start) / step + 1;
                length = Math.max(l, 0);
            }
            return length;
        }

        @Override
        public AnyValue value(long offset) {
            if (offset >= intSize()) {
                // TODO: this should be a GQL error and not java.lang.IndexOutOfBoundsException
                //      but changing it is semi-breaking.
                throw new IndexOutOfBoundsException();
            } else {
                return Values.longValue(start + offset * step);
            }
        }

        @Override
        public Iterator<AnyValue> iterator() {
            int size = intSize();
            if (size == 0) {
                return Collections.emptyIterator();
            } else {
                return new Iterator<>() {
                    private int index = 0;

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
        protected int computeHashToMemoize() {
            int hashCode = 1;
            int current = start;
            int size = intSize();
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
            int size = intSize();
            if (size < 0) {
                // TODO: STATUS_22003
                throw new ArithmeticException("numeric value out of range");
            }

            int current = start;
            long[] array = new long[size];
            for (int i = 0; i < size; i++, current += step) {
                array[i] = current;
            }
            return Values.longArray(array);
        }

        private static final class LongRangeListValue extends IntegralRangeListValue {
            private static final long LONG_RANGE_LIST_VALUE_SHALLOW_SIZE =
                    shallowSizeOfInstance(LongRangeListValue.class);
            private final long start;
            private final long end;
            private final long step;

            LongRangeListValue(long start, long end, long step) {
                this.start = start;
                this.end = end;
                this.step = step;
            }

            @Override
            public String toString() {
                return "Range(" + start + "..." + end + ", step = " + step + ")";
            }

            @Override
            public long actualSize() {
                long diff = (end - start) / step;
                if (diff < 0L) {
                    return 0L;
                } else {
                    try {
                        return Math.addExact(diff, 1L);
                    } catch (java.lang.ArithmeticException e) {
                        // TODO: STATUS_22003
                        throw new ArithmeticException("numeric value out of range", e);
                    }
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
                            var result = Values.longValue(start + index * step);
                            index++;
                            return result;
                        }
                    };
                }
            }

            @Override
            public AnyValue value(long offset) {
                if (offset >= actualSize()) {
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
                int size = (int) actualSize();
                if (size < 0) {
                    // TODO: STATUS_22003
                    throw new ArithmeticException("numeric value out of range");
                }

                long current = start;
                long[] array = new long[size];
                for (int i = 0; i < size; i++, current += step) {
                    array[i] = current;
                }
                return Values.longArray(array);
            }
        }
    }
}
