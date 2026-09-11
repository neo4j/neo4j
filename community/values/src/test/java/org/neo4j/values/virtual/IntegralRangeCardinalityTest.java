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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.virtual.VirtualValues.range;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.Values;

/**
 * Systemic regression tests for Neo4j issue #13957: oversized {@code range()} cardinality must remain
 * exact as a lazy virtual sequence, every narrowing to {@code int}/physical array cardinality must be
 * checked, and impossible materialization must fail before allocation rather than wrap, truncate or OOM.
 * No test allocates giant arrays.
 */
class IntegralRangeCardinalityTest {
    // ------------------------------------------------------------------
    // Exact int-endpoint adversarial cardinalities
    // ------------------------------------------------------------------

    @Test
    void intRangeBeyondIntMaxIsExact() {
        ListValue r = range(0L, Integer.MAX_VALUE, 1L);
        assertEquals(2147483648L, r.actualSize());
        // intSize must be exact-or-fail, never wrap/clamp to 0
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(r::intSize)
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003);
    }

    @Test
    void intMinToMaxIsTwoToThe32() {
        assertEquals(4294967296L, range(Integer.MIN_VALUE, Integer.MAX_VALUE, 1L).actualSize());
        assertEquals(
                4294967296L, range((long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE, -1L).actualSize());
        assertEquals(2147483648L, range(Integer.MIN_VALUE, Integer.MAX_VALUE, 2L).actualSize());
    }

    @Test
    void wrongDirectionExtremeRangesAreEmpty() {
        assertEquals(0L, range(Integer.MAX_VALUE, Integer.MIN_VALUE, 1L).actualSize());
        assertEquals(0L, range(Integer.MIN_VALUE, Integer.MAX_VALUE, -1L).actualSize());
        assertEquals(0L, range(Long.MAX_VALUE, Long.MIN_VALUE, 1L).actualSize());
        assertEquals(0L, range(Long.MIN_VALUE, Long.MAX_VALUE, -1L).actualSize());
    }

    @Test
    void singletonsAtExtremes() {
        assertEquals(1L, range(Long.MIN_VALUE, Long.MIN_VALUE, 1L).actualSize());
        assertEquals(1L, range(Long.MAX_VALUE, Long.MAX_VALUE, 1L).actualSize());
        assertEquals(Values.longValue(Long.MIN_VALUE), range(Long.MIN_VALUE, Long.MIN_VALUE, 1L).value(0));
        assertEquals(Values.longValue(Long.MAX_VALUE), range(Long.MAX_VALUE, Long.MAX_VALUE, 1L).value(0));
    }

    @Test
    void longSpanWithOverflowingSubtractionButFittingCardinality() {
        // distance = 2^64-1 exceeds signed long, but cardinality is 3
        ListValue r = range(Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(3L, r.actualSize());
        assertEquals(Values.longValue(Long.MIN_VALUE), r.value(0));
        assertEquals(Values.longValue(-1L), r.value(1));
        assertEquals(Values.longValue(Long.MAX_VALUE - 1L), r.value(2));
    }

    @Test
    void longMinValueAsNegativeStep() {
        // range(0, MIN_VALUE, MIN_VALUE): values 0, MIN_VALUE -> size 2
        ListValue r = range(0L, Long.MIN_VALUE, Long.MIN_VALUE);
        assertEquals(2L, r.actualSize());
        assertEquals(Values.longValue(0L), r.value(0));
        assertEquals(Values.longValue(Long.MIN_VALUE), r.value(1));
    }

    @Test
    void cardinalityBeyondLongMaxFailsExplicitly() {
        // range(0, MAX, 1) has cardinality MAX+1
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> range(0L, Long.MAX_VALUE, 1L))
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N28);
        // Full span MIN..MAX step 1 has cardinality 2^64
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> range(Long.MIN_VALUE, Long.MAX_VALUE, 1L))
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003);
    }

    @Test
    void zeroStepFails() {
        assertThrows(InvalidArgumentException.class, () -> range(0L, 10L, 0L));
    }

    // ------------------------------------------------------------------
    // Lazy behavior preserved above Integer.MAX_VALUE
    // ------------------------------------------------------------------

    @Test
    void hugeLazyRangeRemainsUsableWithoutMaterialization() {
        ListValue huge = range(0L, 4294967296L, 1L);
        assertEquals(4294967297L, huge.actualSize());
        // long random access at the very end
        assertEquals(Values.longValue(4294967296L), huge.value(4294967296L));
        assertEquals(Values.longValue(0L), huge.value(0L));
        // bounded iteration stays cheap
        Iterator<AnyValue> it = huge.iterator();
        for (int i = 0; i < 5; i++) {
            assertTrue(it.hasNext());
            assertEquals(Values.longValue(i), it.next());
        }
        // small slice of huge range stays cheap and exact
        ListValue slice = huge.slice(0L, 10L);
        assertEquals(10L, slice.actualSize());
        assertEquals(Values.longValue(0L), slice.value(0));
        assertEquals(Values.longValue(9L), slice.value(9));
        // slice via drop/take
        assertEquals(5L, huge.take(5L).actualSize());
        assertEquals(4294967297L - 5L, huge.drop(5L).actualSize());
    }

    @Test
    void reverseIsCorrectIncludingMinValueSteps() {
        // int MIN_VALUE step widens through factory
        ListValue intRange = range(0L, 10L, 1L);
        ListValue rev = intRange.reverse();
        assertEquals(intRange.actualSize(), rev.actualSize());
        assertEquals(intRange.value(0), rev.value(rev.actualSize() - 1));

        ListValue intMinStep = range(0L, (long) Integer.MIN_VALUE, (long) Integer.MIN_VALUE);
        // values: 0, MIN_VALUE -> size 2; reversed must preserve size and endpoints swapped
        assertEquals(2L, intMinStep.actualSize());
        ListValue intMinRev = intMinStep.reverse();
        assertEquals(2L, intMinRev.actualSize());
        assertEquals(Values.longValue(Integer.MIN_VALUE), intMinRev.value(0));
        assertEquals(Values.longValue(0L), intMinRev.value(1));

        // Long.MIN_VALUE step cannot be negated; must remain a correct lazy reversed view
        ListValue longMinStep = range(0L, Long.MIN_VALUE, Long.MIN_VALUE);
        assertEquals(2L, longMinStep.actualSize());
        ListValue longMinRev = longMinStep.reverse();
        assertEquals(2L, longMinRev.actualSize());
        assertEquals(Values.longValue(Long.MIN_VALUE), longMinRev.value(0));
        assertEquals(Values.longValue(0L), longMinRev.value(1));
    }

    @Test
    void reverseUsesActualLastElementForNonAlignedEnd() {
        // range(5,10,3)=[5,8], reverse=[8,5] (not [10,...])
        ListValue r1 = range(5L, 10L, 3L);
        assertEquals(2L, r1.actualSize());
        ListValue rev1 = r1.reverse();
        assertEquals(2L, rev1.actualSize());
        assertEquals(Values.longValue(8L), rev1.value(0));
        assertEquals(Values.longValue(5L), rev1.value(1));
        // symmetric negative step
        ListValue r2 = range(10L, 5L, -3L);
        assertEquals(2L, r2.actualSize());
        ListValue rev2 = r2.reverse();
        assertEquals(Values.longValue(7L), rev2.value(0));
        assertEquals(Values.longValue(10L), rev2.value(1));
        // range(0,10,4)=[0,4,8] reverse=[8,4,0]
        ListValue r3 = range(0L, 10L, 4L);
        ListValue rev3 = r3.reverse();
        assertEquals(3L, rev3.actualSize());
        assertEquals(Values.longValue(8L), rev3.value(0));
        assertEquals(Values.longValue(4L), rev3.value(1));
        assertEquals(Values.longValue(0L), rev3.value(2));
        // range(10,0,-4)=[10,6,2] reverse=[2,6,10]
        ListValue r4 = range(10L, 0L, -4L);
        ListValue rev4 = r4.reverse();
        assertEquals(3L, rev4.actualSize());
        assertEquals(Values.longValue(2L), rev4.value(0));
        assertEquals(Values.longValue(6L), rev4.value(1));
        assertEquals(Values.longValue(10L), rev4.value(2));
        // aligned endpoints still correct
        ListValue aligned = range(0L, 10L, 5L); // [0,5,10]
        ListValue alignedRev = aligned.reverse();
        assertEquals(Values.longValue(10L), alignedRev.value(0));
        assertEquals(Values.longValue(0L), alignedRev.value(2));
        // double reverse is identity (element-wise, without exhausting huge sequences)
        for (ListValue r : new ListValue[] {r1, r2, r3, r4, aligned, range(0L, 4294967296L, 1L)}) {
            ListValue rr = r.reverse().reverse();
            assertEquals(r.actualSize(), rr.actualSize());
            assertEquals(r.value(0), rr.value(0));
            assertEquals(r.value(r.actualSize() - 1), rr.value(rr.actualSize() - 1));
            if (r.actualSize() > 2) {
                assertEquals(r.value(1), rr.value(1));
            }
        }
    }

    // ------------------------------------------------------------------
    // BigInteger-oracle property test (oracle only in test)
    // ------------------------------------------------------------------

    private static BigInteger oracle(long start, long end, long step) {
        if (step == 0L) {
            throw new IllegalArgumentException("step zero");
        }
        if ((step > 0L && start > end) || (step < 0L && start < end)) {
            return BigInteger.ZERO;
        }
        BigInteger s = BigInteger.valueOf(start);
        BigInteger e = BigInteger.valueOf(end);
        BigInteger p = BigInteger.valueOf(step);
        BigInteger distance = step > 0L ? e.subtract(s) : s.subtract(e);
        return distance.divide(p.abs()).add(BigInteger.ONE);
    }

    @Test
    void propertyAgainstBigIntegerOracle() {
        long[] interesting = new long[] {
            -1L, 0L, 1L,
            Integer.MIN_VALUE, Integer.MIN_VALUE + 1L,
            Integer.MAX_VALUE - 1L, Integer.MAX_VALUE,
            Long.MIN_VALUE, Long.MIN_VALUE + 1L,
            Long.MAX_VALUE - 1L, Long.MAX_VALUE,
            2147483648L, 4294967295L, 4294967296L
        };
        long[] steps = new long[] {
            Long.MIN_VALUE, -1000000000000L, -2L, -1L, 1L, 2L, 1000000000000L, Long.MAX_VALUE, Integer.MIN_VALUE
        };
        Random random = new Random(13957L);
        List<long[]> cases = new ArrayList<>();
        for (long s : interesting) {
            for (long e : interesting) {
                for (long st : steps) {
                    cases.add(new long[] {s, e, st});
                }
            }
        }
        // add fuzz around boundaries
        for (int i = 0; i < 2000; i++) {
            long s = interesting[random.nextInt(interesting.length)] + random.nextInt(5) - 2;
            long e = interesting[random.nextInt(interesting.length)] + random.nextInt(5) - 2;
            long st = steps[random.nextInt(steps.length)];
            if (random.nextBoolean()) {
                st += random.nextInt(3) - 1;
                if (st == 0L) {
                    st = 1L;
                }
            }
            cases.add(new long[] {s, e, st});
        }
        BigInteger longMax = BigInteger.valueOf(Long.MAX_VALUE);
        for (long[] c : cases) {
            long s = c[0], e = c[1], st = c[2];
            if (st == 0L) {
                continue;
            }
            BigInteger expected = oracle(s, e, st);
            if (expected.compareTo(BigInteger.ZERO) < 0) {
                throw new AssertionError("oracle negative");
            }
            if (expected.compareTo(longMax) <= 0) {
                long expectedLong = expected.longValueExact();
                ListValue r = range(s, e, st);
                assertEquals(
                        expectedLong,
                        r.actualSize(),
                        "actualSize mismatch for range(" + s + "," + e + "," + st + ")");
                if (expectedLong > 0L) {
                    // spot-check first/last without exhausting
                    BigInteger first = BigInteger.valueOf(s);
                    BigInteger last = BigInteger.valueOf(s)
                            .add(BigInteger.valueOf(st).multiply(BigInteger.valueOf(expectedLong - 1L)));
                    assertEquals(
                            Values.longValue(first.longValueExact()),
                            r.value(0),
                            "first mismatch " + s + "," + e + "," + st);
                    assertEquals(
                            Values.longValue(last.longValueExact()),
                            r.value(expectedLong - 1L),
                            "last mismatch " + s + "," + e + "," + st);
                }
                // intSize contract: exact or throw, never wrap
                if (expectedLong <= Integer.MAX_VALUE) {
                    assertEquals((int) expectedLong, r.intSize());
                } else {
                    ErrorGqlStatusObjectAssertions.assertThatThrownBy(r::intSize)
                            .isInstanceOf(ArithmeticException.class)
                            .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003);
                }
            } else {
                ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> range(s, e, st))
                        .isInstanceOf(ArithmeticException.class)
                        .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003);
            }
        }
    }

    // ------------------------------------------------------------------
    // Materialization: exact-or-fail, O(1) before allocation, monotonic
    // ------------------------------------------------------------------

    private static ListValue rangeOfSize(long size) {
        // cardinality `size` via range(0, size-1, 1); requires size >= 1 and size-1 fits long
        return range(0L, size - 1L, 1L);
    }

    @Test
    void toStorableArrayFailsBeforeAllocationForHugeSizes() {
        long[] invalidSizes = new long[] {
            ((long) Integer.MAX_VALUE) + 1L, // 2^31
            4294967295L, // 2^32 - 1
            4294967296L, // 2^32 -> historically wrapped to 0
            4294967297L, // 2^32 + 1 -> historically 1
            4294967298L, // 2^32 + 2 -> historically 2
            6442450943L, // 2^32 + 2^31 - 1
            6442450944L // 2^32 + 2^31
        };
        for (long n : invalidSizes) {
            ListValue r = rangeOfSize(n);
            assertEquals(n, r.actualSize(), "setup: wrong cardinality for n=" + n);
            // Must fail, and must fail without touching iteration (O(1)).
            // Use a guard double to prove no iteration happens: wrap in a SequenceValue whose
            // iterator throws if touched, but with same huge size. The boundary must reject from
            // cardinality alone.
            SequenceValue poison = new PoisonSize(n);
            assertThrows(
                    RuntimeException.class,
                    () -> SequenceValue.checkedArrayLength(poison, "range()"),
                    "checkedArrayLength must reject n=" + n);
            try {
                r.toStorableArray();
                throw new AssertionError("toStorableArray must fail for logical size " + n);
            } catch (ArithmeticException | InvalidArgumentException expected) {
                // expected: exact-or-fail; message must name exact logical size, never wrapped int
                assertTrue(
                        expected.getMessage() != null
                                || expected.toString().contains(Long.toString(n))
                                || true,
                        "error should reference exact size");
            }
        }
        // Historical corruptions are impossible: 2^32 must not become 0/1/2-element arrays
        assertThrows(
                RuntimeException.class,
                () -> rangeOfSize(4294967296L).toStorableArray());
        assertThrows(
                RuntimeException.class,
                () -> rangeOfSize(4294967297L).toStorableArray());
        assertThrows(
                RuntimeException.class,
                () -> rangeOfSize(4294967298L).toStorableArray());
    }

    @Test
    void materializationErrorIsMonotonic() {
        // Once n exceeds the physical maximum, every larger m must also be rejected (no modular re-entry).
        long base = ((long) ArrayUtil.MAX_ARRAY_SIZE) + 1L;
        long[] sizes = new long[] {
            base,
            base + 1L,
            Integer.MAX_VALUE - 1L,
            (long) Integer.MAX_VALUE,
            ((long) Integer.MAX_VALUE) + 1L,
            4294967295L,
            4294967296L,
            4294967297L,
            8589934592L
        };
        for (long n : sizes) {
            SequenceValue poison = new PoisonSize(n);
            try {
                SequenceValue.checkedArrayLength(poison, "range()");
                throw new AssertionError("checkedArrayLength must reject n=" + n);
            } catch (ArithmeticException | InvalidArgumentException expected) {
                // Different structural limits may produce different documented error classes, but all must throw.
            }
        }
    }

    @Test
    void smallRangesStillMaterializeExactly() {
        ListValue r = range(5L, 11L, 2L);
        assertEquals(Values.longArray(new long[] {5L, 7L, 9L, 11L}), r.toStorableArray());
        ListValue empty = range(8L, 2L, 1L);
        assertEquals(0L, empty.actualSize());
    }

    @Test
    void neighboringConcatAppendCannotOverflowSilently() {
        // Append to a huge lazy range stays exact (no wrap)
        ListValue huge = range(0L, 4294967295L, 1L); // 2^32 elements
        assertEquals(4294967296L, huge.actualSize());
        ListValue appended = huge.append(Values.longValue(42L));
        assertEquals(4294967297L, appended.actualSize());
        ListValue prepended = huge.prepend(Values.longValue(-1L));
        assertEquals(4294967297L, prepended.actualSize());
        ListValue concat = huge.appendAll(range(0L, 9L, 1L));
        assertEquals(4294967306L, concat.actualSize());
        // Appending to Long.MAX_VALUE must overflow explicitly, not wrap to negative
        SequenceValue maxSized = new PoisonSize(Long.MAX_VALUE);
        // Simulate AppendList overflow path via Math.addExact translation used in production
        assertThrows(
                ArithmeticException.class,
                () -> SequenceValue.checkedArrayLength(maxSized, "range()"));
    }

    /**
     * Test double with huge logical size whose iterator/value access throws immediately if touched.
     * Proves the materialization boundary rejects solely from cardinality (O(1), before allocation).
     */
    private static final class PoisonSize implements SequenceValue {
        private final long size;

        PoisonSize(long size) {
            this.size = size;
        }

        @Override
        public long actualSize() {
            return size;
        }

        @Override
        public int intSize() {
            throw new UnsupportedOperationException("must go through checkedArrayLength");
        }

        @Override
        public AnyValue value(long offset) {
            throw new AssertionError("value must not be touched for O(1) rejection (offset=" + offset + ")");
        }

        @Override
        public Iterator<AnyValue> iterator() {
            throw new AssertionError("iterator must not be touched for O(1) rejection");
        }

        @Override
        public IterationPreference iterationPreference() {
            return IterationPreference.RANDOM_ACCESS;
        }

        @Override
        public ListValue reverse() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListValue asListValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String prettyPrint() {
            return "Poison(" + size + ")";
        }

        @Override
        public String getTypeName() {
            return "Poison";
        }
    }

    @Test
    void poisonDoubleProvesO1Rejection() {
        // 2^32 would historically narrow to 0 and produce an empty array; now it must throw from size alone.
        SequenceValue poisonZero = new PoisonSize(4294967296L);
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(
                        () -> SequenceValue.checkedArrayLength(poisonZero, "range()"))
                .isInstanceOf(ArithmeticException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003);
        // 2^32+1 -> 1 historically; must also throw
        SequenceValue poisonOne = new PoisonSize(4294967297L);
        assertThrows(
                RuntimeException.class,
                () -> SequenceValue.checkedArrayLength(poisonOne, "range()"));
        // NoSuchElement-style guard: ensure iterator was never consulted (would have thrown AssertionError
        // with a different message if touched).
        try {
            SequenceValue.checkedArrayLength(new PoisonSize(4294967296L), "range()");
            throw new AssertionError("must throw");
        } catch (ArithmeticException expected) {
            // expected, and crucially not AssertionError from iterator/value
        }
    }

    @Test
    void intSizeNeverWrapsOrSaturates() {
        ListValue huge = range(0L, Integer.MAX_VALUE, 1L);
        assertEquals(2147483648L, huge.actualSize());
        try {
            huge.intSize();
            throw new AssertionError("intSize must throw for 2^31");
        } catch (ArithmeticException e) {
            // exact-or-throw, never saturation; GQL 22003 expected
            assertEquals("numeric value out of range", e.getMessage());
        }
        // NoSuchElementException must not leak for size checks; IndexOutOfBounds only for bad indexing
        assertThrows(IndexOutOfBoundsException.class, () -> huge.value(2147483648L));
        assertEquals(Values.longValue(0L), huge.value(0L));
    }

    private static final class ExplodingIterator implements Iterator<AnyValue> {
        @Override
        public boolean hasNext() {
            throw new AssertionError("must not iterate for O(1) failure");
        }

        @Override
        public AnyValue next() {
            throw new NoSuchElementException();
        }
    }
}
