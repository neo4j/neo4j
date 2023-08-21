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

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.DoubleLists;
import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.list.ImmutableList;
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
        final var smallestPositiveFloat = Math.nextUp(+0.f);
        final var squareRootLargestFloat = (float) Math.sqrt(Float.MAX_VALUE);
        final var largerThanSquareRootLargestDouble = Math.nextUp(Math.sqrt(Double.MAX_VALUE));

        // finite non-zero values

        final var finitePrimitiveNonZeroFloats = FloatLists.immutable.of(
                -Float.MAX_VALUE,
                -Float.MIN_NORMAL,
                -Float.MIN_VALUE,
                +Float.MIN_VALUE,
                +Float.MIN_NORMAL,
                +Float.MAX_VALUE);
        final var finitePrimitiveNonZeroFloatArrays = Lists.mutable
                .withAll(finitePrimitiveNonZeroFloats.asLazy().collect(VectorTestUtils::toPrimitive))
                .toImmutableList();

        final var finiteBoxedNonZeroFloatArrays = finitePrimitiveNonZeroFloatArrays.collect(ArrayUtils::toObject);

        final var finiteNonZeroFloatArrays = Lists.mutable
                .withAll(finitePrimitiveNonZeroFloatArrays.asLazy().collect(Values::of))
                .withAll(finiteBoxedNonZeroFloatArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var finitePrimitiveNonZeroDoubleArrays = DoubleLists.mutable
                .of(smallerDoubleThanSmallestFloatButSameValue, largerDoubleThanLargestFloatButSameValue)
                .withAll(finitePrimitiveNonZeroFloats.asLazy().collectDouble(v -> v))
                .collect(VectorTestUtils::toPrimitive)
                .toImmutableList();

        final var finiteBoxedNonZeroDoubleArrays = finitePrimitiveNonZeroDoubleArrays.collect(ArrayUtils::toObject);

        final var finiteNonZeroDoubleArrays = Lists.mutable
                .withAll(finitePrimitiveNonZeroDoubleArrays.asLazy().collect(Values::of))
                .withAll(finiteBoxedNonZeroDoubleArrays.asLazy().collect(Values::of))
                .toImmutableList();

        // finite zero values

        final var finitePrimitiveZeroFloats = FloatLists.immutable.of(-0.f, +0.f);
        final var finitePrimitiveZeroFloatArrays = Lists.mutable
                .withAll(finitePrimitiveZeroFloats.asLazy().collect(VectorTestUtils::toPrimitive))
                .toImmutableList();

        final var finiteBoxedZeroFloatArrays = finitePrimitiveZeroFloatArrays.collect(ArrayUtils::toObject);

        final var finiteZeroFloatArrays = Lists.mutable
                .withAll(finitePrimitiveZeroFloatArrays.asLazy().collect(Values::of))
                .withAll(finiteBoxedZeroFloatArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var finitePrimitiveZeroDoubleArrays = finitePrimitiveZeroFloatArrays.collect(VectorTestUtils::promote);

        final var finiteBoxedZeroDoubleArrays = finitePrimitiveZeroDoubleArrays.collect(ArrayUtils::toObject);

        final var finiteZeroDoubleArrays = Lists.mutable
                .withAll(finitePrimitiveZeroDoubleArrays.asLazy().collect(Values::of))
                .withAll(finiteBoxedZeroDoubleArrays.asLazy().collect(Values::of))
                .toImmutableList();

        // finite L2 norms

        final var finiteL2NormPrimitiveFloatArrays = Lists.immutable.of(
                toPrimitive(smallestPositiveFloat, smallestPositiveFloat),
                toPrimitive(squareRootLargestFloat, squareRootLargestFloat));

        final var finiteL2NormBoxedFloatArrays = finiteL2NormPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var finiteL2NormFloatArrays = Lists.mutable
                .withAll(finiteL2NormPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(finiteL2NormBoxedFloatArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var finiteL2NormPrimitiveDoubleArrays =
                finiteL2NormPrimitiveFloatArrays.collect(VectorTestUtils::promote);

        final var finiteL2NormBoxedDoubleArrays = finiteL2NormPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var finiteL2NormDoubleArrays = Lists.mutable
                .withAll(finiteL2NormPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(finiteL2NormBoxedDoubleArrays.asLazy().collect(Values::of))
                .toImmutableList();

        // non-finite values

        final var nonFinitePrimitiveFloats =
                FloatLists.immutable.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
        final var nonFinitePrimitiveFloatArrays = FloatLists.mutable
                .withAll(nonFinitePrimitiveFloats)
                .collect(VectorTestUtils::toPrimitive)
                .with(new float[0])
                .with(null)
                .toImmutableList();

        final var nonFiniteBoxedFloatArrays = nonFinitePrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var nonFiniteFloatArrays = Lists.mutable
                .withAll(nonFinitePrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(nonFiniteBoxedFloatArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .toImmutableList();

        final var nonFinitePrimitiveDoubleArrays = DoubleLists.mutable
                .of(smallerDoubleThanSmallestFloat, largerDoubleThanLargestFloat)
                .withAll(nonFinitePrimitiveFloats.asLazy().collectDouble(v -> v))
                .collect(VectorTestUtils::toPrimitive)
                .with(null)
                .toImmutableList();

        final var nonFiniteBoxedDoubleArrays = nonFinitePrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var nonFiniteDoubleArrays = Lists.mutable
                .withAll(nonFinitePrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(nonFiniteBoxedDoubleArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .toImmutableList();

        // non-finite L2 norms

        final var nonFiniteL2NormPrimitiveFloatArrays =
                Lists.immutable.of(toPrimitive(-0, -0), toPrimitive(-0, +0), toPrimitive(+0, -0), toPrimitive(+0, +0));

        final var nonFiniteL2NormBoxedFloatArrays = nonFiniteL2NormPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var nonFiniteL2NormFloatArrays = Lists.mutable
                .withAll(nonFiniteL2NormPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(nonFiniteL2NormBoxedFloatArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var nonFiniteL2NormPrimitiveDoubleArrays = Lists.mutable
                .of(toPrimitive(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble))
                .withAll(nonFiniteL2NormPrimitiveFloatArrays.asLazy().collect(VectorTestUtils::promote))
                .toImmutableList();

        final var nonFiniteL2NormBoxedDoubleArrays = nonFiniteL2NormPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        final var nonFiniteL2NormDoubleArrays = Lists.mutable
                .withAll(nonFiniteL2NormPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(nonFiniteL2NormBoxedDoubleArrays.asLazy().collect(Values::of))
                .toImmutableList();

        // now to put them all together

        // set valid Cosine vectors

        COSINE_VALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(finiteNonZeroFloatArrays)
                .withAll(finiteNonZeroDoubleArrays)
                .withAll(finiteL2NormFloatArrays)
                .withAll(finiteL2NormDoubleArrays)
                .toImmutableList();

        COSINE_VALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(finiteBoxedNonZeroDoubleArrays)
                .withAll(finiteL2NormBoxedDoubleArrays)
                .asLazy()
                .collect(Lists.immutable::of)
                .collect(ImmutableList::castToList)
                .toImmutableList();

        // set valid Euclidean vectors

        EUCLIDEAN_VALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(finiteZeroFloatArrays)
                .withAll(finiteZeroDoubleArrays)
                .withAll(COSINE_VALID_VECTORS_FROM_VALUE)
                .toImmutableList();

        EUCLIDEAN_VALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(finiteBoxedZeroDoubleArrays)
                .collect(Lists.immutable::of)
                .collect(ImmutableList::castToList)
                .withAll(COSINE_VALID_VECTORS_FROM_DOUBLE_LIST)
                .toImmutableList();

        // set invalid Euclidean vectors

        EUCLIDEAN_INVALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(nonFiniteFloatArrays)
                .withAll(nonFiniteDoubleArrays)
                .with(null)
                .toImmutableList();

        EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(nonFiniteBoxedDoubleArrays
                        .asLazy()
                        .collect(Lists.immutable::of)
                        .collect(ImmutableList::castToList))
                .with(null)
                .toImmutableList();

        // set invalid Cosine vectors

        COSINE_INVALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(nonFiniteL2NormFloatArrays)
                .withAll(nonFiniteL2NormDoubleArrays)
                .withAll(EUCLIDEAN_INVALID_VECTORS_FROM_VALUE)
                .toImmutableList();

        COSINE_INVALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(nonFiniteL2NormBoxedDoubleArrays
                        .asLazy()
                        .collect(Lists.immutable::of)
                        .collect(ImmutableList::castToList))
                .withAll(EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST)
                .toImmutableList();
    }

    // just a convenience method as primitive arrays lack a "constructor"-like interface
    private static float[] toPrimitive(float... array) {
        return array;
    }

    private static double[] toPrimitive(double... array) {
        return array;
    }

    private static double[] promote(float... array) {
        final var promoted = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            promoted[i] = array[i];
        }
        return promoted;
    }

    private static double extremeSameFloatValue(double value) {
        final var floatSignificandWidth = 24; // jdk.internal.math.FloatConsts.SIGNIFICAND_WIDTH
        final var doubleSignificandWidth = 53; // jdk.internal.math.DoubleConsts.SIGNIFICAND_WIDTH
        final var mask = (1 << (doubleSignificandWidth - floatSignificandWidth - 1)) - 1;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(value) | mask);
    }
}
