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
package org.neo4j.kernel.api.schema.vector;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.LazyIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorTestUtils {
    public static final Iterable<Value> EUCLIDEAN_VALID_VECTORS_FROM_VALUE;
    public static final Iterable<Value> EUCLIDEAN_INVALID_VECTORS_FROM_VALUE;
    public static final Iterable<List<Double>> EUCLIDEAN_VALID_VECTORS_FROM_DOUBLE_LIST;
    public static final Iterable<List<Double>> EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST;
    public static final Iterable<Value> COSINE_VALID_VECTORS_FROM_VALUE;
    public static final Iterable<Value> COSINE_INVALID_VECTORS_FROM_VALUE;
    public static final Iterable<List<Double>> COSINE_VALID_VECTORS_FROM_DOUBLE_LIST;
    public static final Iterable<List<Double>> COSINE_INVALID_VECTORS_FROM_DOUBLE_LIST;

    static {
        // A bit of a mess, but ensures many extreme combinations of:
        //  * valid/invalid vector candidate
        //  * each value source
        //  * each similarity function
        // in structures that can hold all values including null

        final var smallerDoubleThanSmallestFloatButSameValue = extremeSameFloatValue(-Float.MAX_VALUE);
        final var smallerDoubleThanSmallestFloat = Math.nextDown(smallerDoubleThanSmallestFloatButSameValue);
        final var largerDoubleThanLargestFloatButSameValue = extremeSameFloatValue(+Float.MAX_VALUE);
        final var largerDoubleThanLargestFloat = Math.nextUp(largerDoubleThanLargestFloatButSameValue);
        final var squareRootSmallestPositiveFloat = (float) Math.sqrt(Math.nextUp(+0.f));
        final var squareRootLargestFloat = (float) Math.sqrt(Float.MAX_VALUE);
        final var squareRootHalfLargestFloat = (float) Math.sqrt(Float.MAX_VALUE / 2.f);
        final var largerThanSquareRootLargestDouble = Math.nextUp(Math.sqrt(Double.MAX_VALUE));

        // finite non-zero extreme values

        final var floatFiniteNonZeroPositiveExtremePrimitiveFloats = FloatLists.immutable
                .of(Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE)
                .asLazy();

        final var floatFiniteNonZeroExtremePrimitiveFloatArrays = floatFiniteNonZeroPositiveExtremePrimitiveFloats
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        final var floatFiniteNonZeroExtremeBoxedFloatArrays =
                floatFiniteNonZeroExtremePrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var floatFiniteNonZeroExtremeFloatArrays = Lists.mutable
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteNonZeroExtremeBoxedFloatArrays.asLazy().collect(Values::of))
                .asLazy();

        final var floatFiniteNonZeroExtremePrimitiveDoubleArrays = Lists.mutable
                .of(
                        smallerDoubleThanSmallestFloatButSameValue,
                        largerDoubleThanLargestFloatButSameValue,
                        Double.MIN_VALUE,
                        Double.MIN_NORMAL)
                .flatCollect(VectorTestUtils::signPermutations)
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.asLazy().collect(VectorTestUtils::promote))
                .asLazy();

        final var floatFiniteNonZeroExtremeBoxedDoubleArrays =
                floatFiniteNonZeroExtremePrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var floatFiniteNonZeroExtremeDoubleArrays = Lists.mutable
                .withAll(floatFiniteNonZeroExtremePrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteNonZeroExtremeBoxedDoubleArrays.asLazy().collect(Values::of))
                .asLazy();

        // finite zero values

        final var floatFiniteZeroPrimitiveFloatArrays = signPermutations(0.f);

        final var floatFiniteZeroBoxedFloatArrays = floatFiniteZeroPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var floatFiniteZeroFloatArrays = Lists.mutable
                .withAll(floatFiniteZeroPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteZeroBoxedFloatArrays.asLazy().collect(Values::of))
                .asLazy();

        final var floatFiniteZeroPrimitiveDoubleArrays =
                floatFiniteZeroPrimitiveFloatArrays.collect(VectorTestUtils::promote);

        final var floatFiniteZeroBoxedDoubleArrays = floatFiniteZeroPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var floatFiniteZeroDoubleArrays = Lists.mutable
                .withAll(floatFiniteZeroPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteZeroBoxedDoubleArrays.asLazy().collect(Values::of))
                .asLazy();

        // finite non-zero sqrt(extreme) values

        final var floatFiniteNonZeroSqrtExtremePrimitiveFloats = floatFiniteNonZeroPositiveExtremePrimitiveFloats
                .asLazy()
                .collectDouble(Math::sqrt)
                .collectFloat(v -> (float) v);

        // finite square L2 norms

        final var floatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, squareRootLargestFloat),
                        toPrimitive(squareRootLargestFloat, 0.f),
                        toPrimitive(squareRootSmallestPositiveFloat, squareRootSmallestPositiveFloat),
                        toPrimitive(squareRootHalfLargestFloat, squareRootHalfLargestFloat))
                .withAll(floatFiniteNonZeroSqrtExtremePrimitiveFloats.asLazy().collect(VectorTestUtils::toPrimitive))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        final var floatFiniteSquareL2NormBoxedFloatArrays =
                floatFiniteSquareL2NormPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var floatFiniteSquareL2NormFloatArrays = Lists.mutable
                .withAll(floatFiniteSquareL2NormPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteSquareL2NormBoxedFloatArrays.asLazy().collect(Values::of))
                .asLazy();

        final var floatFiniteSquareL2NormPrimitiveDoubleArrays =
                floatFiniteSquareL2NormPrimitiveFloatArrays.collect(VectorTestUtils::promote);

        final var floatFiniteSquareL2NormBoxedDoubleArrays =
                floatFiniteSquareL2NormPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var floatFiniteSquareL2NormDoubleArrays = Lists.mutable
                .withAll(floatFiniteSquareL2NormPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(floatFiniteSquareL2NormBoxedDoubleArrays.asLazy().collect(Values::of))
                .asLazy();

        // non-finite values

        final var nonFloatFinitePrimitiveFloatArrays = Lists.mutable
                .withAll(signPermutations(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY))
                .with(new float[0])
                .with(null)
                .asLazy();

        final var nonFloatFiniteBoxedFloatArrays = nonFloatFinitePrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var nonFloatFiniteFloatArrays = Lists.mutable
                .withAll(nonFloatFinitePrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(nonFloatFiniteBoxedFloatArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .asLazy();

        final var nonFloatFinitePrimitiveDoubleArrays = Lists.mutable
                .of(
                        smallerDoubleThanSmallestFloat,
                        largerDoubleThanLargestFloat,
                        Double.MAX_VALUE,
                        Double.NaN,
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY)
                .flatCollect(VectorTestUtils::signPermutations)
                .withAll(nonFloatFinitePrimitiveFloatArrays.asLazy().collect(VectorTestUtils::promote))
                .with(null)
                .asLazy();

        final var nonFloatFiniteBoxedDoubleArrays = nonFloatFinitePrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var nonFloatFiniteDoubleArrays = Lists.mutable
                .withAll(nonFloatFinitePrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(nonFloatFiniteBoxedDoubleArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .asLazy();

        // non-finite square L2 norms

        final var nonFloatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, 0.f),
                        toPrimitive(squareRootHalfLargestFloat, Math.nextUp(squareRootHalfLargestFloat)),
                        toPrimitive(Float.MAX_VALUE, Float.MAX_VALUE))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        final var nonFloatFiniteSquareL2NormBoxedFloatArrays =
                nonFloatFiniteSquareL2NormPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var nonFloatFiniteSquareL2NormFloatArrays = Lists.mutable
                .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(nonFloatFiniteSquareL2NormBoxedFloatArrays.asLazy().collect(Values::of))
                .asLazy();

        final var nonFloatFiniteSquareL2NormPrimitiveDoubleArrays = Lists.mutable
                .withAll(signPermutations(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble))
                .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays.asLazy().collect(VectorTestUtils::promote))
                .asLazy();

        final var nonFloatFiniteSquareL2NormBoxedDoubleArrays =
                nonFloatFiniteSquareL2NormPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var nonFloatFiniteSquareL2NormDoubleArrays = Lists.mutable
                .withAll(
                        nonFloatFiniteSquareL2NormPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(nonFloatFiniteSquareL2NormBoxedDoubleArrays.asLazy().collect(Values::of))
                .asLazy();

        // now to put them all together
        //   valid = immutable sorted sets
        // invalid = unmodifiable sorted sets (immutable cannot handle null)

        // with some comparators to remove duplicates, but keeping different types
        final var valueComparator = Comparator.nullsFirst(
                Comparator.comparing(Value::valueRepresentation).thenComparing((lhs, rhs) -> {
                    if (!(lhs instanceof final FloatingPointArray flhs)
                            || !(rhs instanceof final FloatingPointArray frhs)) {
                        if (Objects.equals(lhs, rhs)) {
                            return 0;
                        }
                        return Comparator.comparing(o -> o.getClass().descriptorString())
                                .compare(lhs, rhs);
                    }

                    var comparison = Integer.compare(flhs.length(), frhs.length());
                    if (comparison != 0) {
                        return comparison;
                    }

                    for (int i = 0; i < flhs.length(); i++) {
                        comparison = Double.compare(flhs.doubleValue(i), frhs.doubleValue(i));
                        if (comparison != 0) {
                            return comparison;
                        }
                    }
                    return 0;
                }));

        final var listComparator = Comparator.nullsFirst(
                Comparator.<List<Double>>comparingInt(List::size).thenComparing((lhs, rhs) -> {
                    for (int i = 0; i < lhs.size(); i++) {
                        final var comparison = Double.compare(lhs.get(i), rhs.get(i));
                        if (comparison != 0) {
                            return comparison;
                        }
                    }
                    return 0;
                }));

        // set valid Cosine vectors

        COSINE_VALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(floatFiniteSquareL2NormFloatArrays)
                .withAll(floatFiniteSquareL2NormDoubleArrays)
                .toImmutableSortedSet(valueComparator);

        COSINE_VALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(floatFiniteSquareL2NormBoxedDoubleArrays)
                .asLazy()
                .collect(Lists.immutable::of)
                .collect(ImmutableList::castToList)
                .toImmutableSortedSet(listComparator);

        // set valid Euclidean vectors

        EUCLIDEAN_VALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(floatFiniteZeroFloatArrays)
                .withAll(floatFiniteZeroDoubleArrays)
                .withAll(floatFiniteNonZeroExtremeFloatArrays)
                .withAll(floatFiniteNonZeroExtremeDoubleArrays)
                .withAll(COSINE_VALID_VECTORS_FROM_VALUE)
                .toImmutableSortedSet(valueComparator);

        EUCLIDEAN_VALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(floatFiniteZeroBoxedDoubleArrays)
                .collect(Lists.immutable::of)
                .collect(ImmutableList::castToList)
                .withAll(COSINE_VALID_VECTORS_FROM_DOUBLE_LIST)
                .toImmutableSortedSet(listComparator);

        // set invalid Euclidean vectors

        EUCLIDEAN_INVALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(nonFloatFiniteFloatArrays)
                .withAll(nonFloatFiniteDoubleArrays)
                .with(null)
                .toSortedSet(valueComparator)
                .asUnmodifiable();

        EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(nonFloatFiniteBoxedDoubleArrays
                        .asLazy()
                        .collect(Lists.immutable::of)
                        .collect(ImmutableList::castToList))
                .with(null)
                .toSortedSet(listComparator)
                .asUnmodifiable();

        // set invalid Cosine vectors

        COSINE_INVALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(nonFloatFiniteSquareL2NormFloatArrays)
                .withAll(nonFloatFiniteSquareL2NormDoubleArrays)
                .withAll(EUCLIDEAN_INVALID_VECTORS_FROM_VALUE)
                .toSortedSet(valueComparator)
                .asUnmodifiable();

        COSINE_INVALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(nonFloatFiniteSquareL2NormBoxedDoubleArrays
                        .asLazy()
                        .collect(Lists.immutable::of)
                        .collect(ImmutableList::castToList))
                .withAll(EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST)
                .toSortedSet(listComparator)
                .asUnmodifiable();
    }

    // just a convenience method as primitive arrays lack a "constructor"-like interface
    private static float[] toPrimitive(float... array) {
        return array;
    }

    private static double[] promote(float... array) {
        if (array == null) {
            return null;
        }

        final var promoted = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            promoted[i] = array[i];
        }
        return promoted;
    }

    private static LazyIterable<float[]> signPermutations(float... values) {
        if (values == null) {
            return null;
        }

        final var floatSignBit = 0x80000000; // jdk.internal.math.FloatConsts.SIGN_BIT_MASK

        final var n = 1 << values.length;
        final var perms = Lists.mutable.<float[]>withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            final var perm = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                final var set = (p & 1 << i) != 0;
                final var sign = set ? floatSignBit : 0;
                final var value = Float.floatToRawIntBits(values[i]);
                perm[i] = Float.intBitsToFloat(value ^ sign);
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static LazyIterable<double[]> signPermutations(double... values) {
        if (values == null) {
            return null;
        }

        final var doubleSignBit = 0x8000000000000000L; // jdk.internal.math.DoubleConsts.SIGN_BIT_MASK

        final var n = 1 << values.length;
        final var perms = Lists.mutable.<double[]>withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            final var perm = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                final var set = (p & 1 << i) != 0;
                final var sign = set ? doubleSignBit : 0;
                final var value = Double.doubleToRawLongBits(values[i]);
                perm[i] = Double.longBitsToDouble(value ^ sign);
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static double extremeSameFloatValue(double value) {
        final var floatSignificandWidth = 24; // jdk.internal.math.FloatConsts.SIGNIFICAND_WIDTH
        final var doubleSignificandWidth = 53; // jdk.internal.math.DoubleConsts.SIGNIFICAND_WIDTH
        final var mask = (1 << (doubleSignificandWidth - floatSignificandWidth - 1)) - 1;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(value) | mask);
    }
}
