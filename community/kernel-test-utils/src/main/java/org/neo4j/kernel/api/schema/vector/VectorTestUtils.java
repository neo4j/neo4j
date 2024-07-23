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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.LazyIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.ByteLists;
import org.eclipse.collections.api.factory.primitive.DoubleLists;
import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.factory.primitive.ShortLists;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunctions;
import org.neo4j.kernel.api.vector.VectorQuantization;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

public class VectorTestUtils {
    public static final RichIterable<AnyValue> EUCLIDEAN_VALID_VECTORS;
    public static final RichIterable<AnyValue> EUCLIDEAN_INVALID_VECTORS;
    public static final RichIterable<AnyValue> SIMPLE_COSINE_VALID_VECTORS;
    public static final RichIterable<AnyValue> SIMPLE_COSINE_INVALID_VECTORS;
    public static final RichIterable<AnyValue> L2_NORM_COSINE_VALID_VECTORS;
    public static final RichIterable<AnyValue> L2_NORM_COSINE_INVALID_VECTORS;

    public static RichIterable<AnyValue> validVectorsFor(VectorSimilarityFunction function) {
        if (function == VectorSimilarityFunctions.EUCLIDEAN) {
            return EUCLIDEAN_VALID_VECTORS;
        } else if (function == VectorSimilarityFunctions.SIMPLE_COSINE) {
            return SIMPLE_COSINE_VALID_VECTORS;
        } else if (function == VectorSimilarityFunctions.L2_NORM_COSINE) {
            return L2_NORM_COSINE_VALID_VECTORS;
        } else {
            throw new IllegalArgumentException("unknown similarity function: %s".formatted(function));
        }
    }

    public static RichIterable<AnyValue> invalidVectorsFor(VectorSimilarityFunction function) {
        if (function == VectorSimilarityFunctions.EUCLIDEAN) {
            return EUCLIDEAN_INVALID_VECTORS;
        } else if (function == VectorSimilarityFunctions.SIMPLE_COSINE) {
            return SIMPLE_COSINE_INVALID_VECTORS;
        } else if (function == VectorSimilarityFunctions.L2_NORM_COSINE) {
            return L2_NORM_COSINE_INVALID_VECTORS;
        } else {
            throw new IllegalArgumentException("unknown similarity function: %s".formatted(function));
        }
    }

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
        final var squareRootSmallestPositiveDouble = Math.sqrt(Math.nextUp(+0.0));
        final var squareRootLargestDouble = Math.sqrt(Double.MAX_VALUE);
        final var squareRootHalfLargestDouble = Math.sqrt(Double.MAX_VALUE / 2.0);
        final var largerThanSquareRootLargestDouble = Math.nextUp(Math.sqrt(Double.MAX_VALUE));

        // non-zero normal values

        final var floatFiniteNonZeroRegularArrays = Lists.immutable
                .of(
                        toArrayValue(toPrimitive((byte) 42)),
                        toArrayValue(toPrimitive((short) -1234)),
                        toArrayValue(toPrimitive(0xdeadbeaf)),
                        toArrayValue(toPrimitive(-1234567890987654321L)),
                        toArrayValue(toPrimitive((float) Math.E)),
                        toArrayValue(toPrimitive(Math.PI)))
                .asLazy();

        // integral non-zero extreme values

        final var floatFiniteNonZeroExtremeIntegralArrays = Lists.mutable
                .withAll(ByteLists.immutable
                        .of((byte) -Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .collect(VectorTestUtils::toArrayValue))
                .withAll(ShortLists.immutable
                        .of((short) -Short.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .collect(VectorTestUtils::toArrayValue))
                .withAll(IntLists.immutable
                        .of(-Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .collect(VectorTestUtils::toArrayValue))
                .withAll(LongLists.immutable
                        .of(-Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .collect(VectorTestUtils::toArrayValue))
                .asLazy();

        // finite non-zero extreme values

        final var floatFiniteNonZeroPositiveExtremePrimitiveFloats = FloatLists.immutable
                .of(Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE)
                .asLazy();

        final var floatFiniteNonZeroExtremePrimitiveFloatArrays =
                floatFiniteNonZeroPositiveExtremePrimitiveFloats.flatCollect(VectorTestUtils::signPermutations);

        final var floatFiniteNonZeroExtremeFloatArrays = Lists.mutable
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.collect(VectorTestUtils::toArrayValue))
                .asLazy();

        final var floatFiniteNonZeroExtremeDoubleArrays = Lists.mutable
                .of(
                        smallerDoubleThanSmallestFloatButSameValue,
                        largerDoubleThanLargestFloatButSameValue,
                        Double.MIN_VALUE,
                        Double.MIN_NORMAL)
                .flatCollect(VectorTestUtils::signPermutations)
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.collect(VectorTestUtils::promote))
                .asLazy()
                .collect(VectorTestUtils::toArrayValue);

        final var doubleFiniteNonZeroPositiveExtremePrimitiveDoubles = DoubleLists.immutable
                .of(Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE)
                .asLazy();

        final var doubleFiniteNonZeroExtremeDoubleArrays = doubleFiniteNonZeroPositiveExtremePrimitiveDoubles
                .flatCollect(VectorTestUtils::signPermutations)
                .collect(VectorTestUtils::toArrayValue);

        // finite zero values

        final var floatFiniteZeroPrimitiveIntegralArrays = Lists.mutable
                .with(toArrayValue(toPrimitive((byte) 0)))
                .with(toArrayValue(toPrimitive((short) 0)))
                .with(toArrayValue(toPrimitive(0)))
                .with(toArrayValue(toPrimitive(0L)))
                .asLazy();

        final var floatFiniteZeroPrimitiveFloatArrays = signPermutations(0.f);

        final var floatFiniteZeroFloatingPointArrays = Lists.mutable
                .withAll(floatFiniteZeroPrimitiveFloatArrays.collect(VectorTestUtils::toArrayValue))
                .withAll(floatFiniteZeroPrimitiveFloatArrays
                        .collect(VectorTestUtils::promote)
                        .collect(VectorTestUtils::toArrayValue))
                .asLazy();

        // finite non-zero sqrt(extreme) values

        final var floatFiniteNonZeroSqrtExtremePrimitiveFloats = floatFiniteNonZeroPositiveExtremePrimitiveFloats
                .collectDouble(Math::sqrt)
                .collectFloat(v -> (float) v);

        final var doubleFiniteNonZeroSqrtExtremePrimitiveDoubles =
                doubleFiniteNonZeroPositiveExtremePrimitiveDoubles.collectDouble(Math::sqrt);

        // finite square L2 norms

        final var floatFiniteSquareL2NormIntegralArrays =
                signPermutations(Long.MAX_VALUE, Long.MAX_VALUE).collect(VectorTestUtils::toArrayValue);

        final var floatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, squareRootLargestFloat),
                        toPrimitive(squareRootLargestFloat, 0.f),
                        toPrimitive(squareRootSmallestPositiveFloat, squareRootSmallestPositiveFloat),
                        toPrimitive(squareRootHalfLargestFloat, squareRootHalfLargestFloat))
                .withAll(floatFiniteNonZeroSqrtExtremePrimitiveFloats.collect(VectorTestUtils::toPrimitive))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        final var floatFiniteSquareL2NormFloatingPointArrays = Lists.mutable
                .withAll(floatFiniteSquareL2NormPrimitiveFloatArrays.collect(VectorTestUtils::toArrayValue))
                .withAll(floatFiniteSquareL2NormPrimitiveFloatArrays
                        .collect(VectorTestUtils::promote)
                        .collect(VectorTestUtils::toArrayValue))
                .asLazy();

        final var doubleFiniteSquareL2NormDoubleArrays = Lists.mutable
                .of(
                        toPrimitive(0.0, squareRootLargestDouble),
                        toPrimitive(squareRootLargestDouble, 0.0),
                        toPrimitive(squareRootSmallestPositiveDouble, squareRootSmallestPositiveDouble),
                        toPrimitive(squareRootHalfLargestDouble, squareRootHalfLargestDouble))
                .withAll(doubleFiniteNonZeroSqrtExtremePrimitiveDoubles.collect(VectorTestUtils::toPrimitive))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations)
                .collect(VectorTestUtils::toArrayValue);

        final var floatFiniteSquareL2NormMixedArrays = signPermutations(
                Values.longValue(Long.MAX_VALUE),
                Values.floatValue(Long.MAX_VALUE),
                Values.doubleValue(Long.MAX_VALUE));

        // non-finite values

        final var nonFloatFiniteIntegralArrays = Lists.mutable
                .of(Values.EMPTY_BYTE_ARRAY, Values.EMPTY_SHORT_ARRAY, Values.EMPTY_INT_ARRAY, Values.EMPTY_LONG_ARRAY)
                .asLazy();

        final var nonFloatFinitePrimitiveFloatArrays = Lists.mutable
                .of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)
                .flatCollect(VectorTestUtils::signPermutations)
                .with(ArrayUtils.EMPTY_FLOAT_ARRAY)
                .asLazy();

        final var nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles = Lists.mutable
                .of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                .asLazy();

        final var nonFloatFiniteFloatingPointArrays = Lists.mutable
                .withAll(nonFloatFinitePrimitiveFloatArrays.collect(VectorTestUtils::toArrayValue))
                .withAll(Lists.mutable
                        .of(smallerDoubleThanSmallestFloat, largerDoubleThanLargestFloat, Double.MAX_VALUE)
                        .withAll(nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles)
                        .flatCollect(VectorTestUtils::signPermutations)
                        .withAll(nonFloatFinitePrimitiveFloatArrays.collect(VectorTestUtils::promote))
                        .collect(VectorTestUtils::toArrayValue));

        final var nonDoubleFiniteDoubleArrays = Lists.mutable
                .withAll(nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles)
                .flatCollect(VectorTestUtils::signPermutations)
                .with(ArrayUtils.EMPTY_DOUBLE_ARRAY)
                .asLazy()
                .collect(VectorTestUtils::toArrayValue);

        // non-finite square L2 norms

        final var nonFloatFiniteSquareL2NormIntegralArrays =
                Lists.mutable.withAll(signPermutations(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)
                        .collect(VectorTestUtils::toArrayValue));

        final var nonFloatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, 0.f),
                        toPrimitive(squareRootHalfLargestFloat, Math.nextUp(squareRootHalfLargestFloat)),
                        toPrimitive(Float.MAX_VALUE, Float.MAX_VALUE))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        final var nonFloatFiniteSquareL2NormFloatingPointArrays = Lists.mutable
                .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays.asLazy().collect(VectorTestUtils::toArrayValue))
                .withAll(Lists.mutable
                        .withAll(signPermutations(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble))
                        .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays.collect(VectorTestUtils::promote))
                        .collect(VectorTestUtils::toArrayValue))
                .asLazy();

        final var nonFloatFiniteSquareL2NormMixedArrays = signPermutations(
                Values.longValue(Long.MAX_VALUE),
                Values.floatValue(Long.MAX_VALUE),
                Values.doubleValue(Long.MAX_VALUE),
                Values.longValue(Long.MAX_VALUE));

        final var nonDoubleFiniteZeroSquareL2NormIntegralArrays = Lists.mutable
                .of(
                        toArrayValue(toPrimitive((byte) 0, (byte) 0)),
                        toArrayValue(toPrimitive((short) 0, (short) 0)),
                        toArrayValue(toPrimitive(0, 0)),
                        toArrayValue(toPrimitive(0L, 0L)))
                .asLazy();

        final var nonDoubleFiniteSquareL2NormDoubleArrays = Lists.mutable
                .of(
                        toPrimitive(0.0, 0.0),
                        toPrimitive(squareRootHalfLargestDouble, Math.nextUp(squareRootHalfLargestDouble)),
                        toPrimitive(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble),
                        toPrimitive(Double.MAX_VALUE, Double.MAX_VALUE))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations)
                .collect(VectorTestUtils::toArrayValue);

        // wrong types

        final var nonNumericArrays = Lists.mutable
                .<AnyValue>of(toArrayValue(ArrayUtils.toArray("clearly", "not", "numeric")))
                .withAll(Lists.mutable
                        .withAll(floatFiniteNonZeroRegularArrays)
                        .withAll(floatFiniteNonZeroExtremeIntegralArrays)
                        .collect(VectorTestUtils::convertEvenElementsToStringValues))
                .asLazy();

        // now to put them all together
        //   valid = immutable sorted sets
        // invalid = unmodifiable sorted sets (immutable cannot handle null)

        // with some comparators to remove duplicates, but keeping different types
        final var objectComparator = Comparator.nullsLast((lhs, rhs) -> {
            if (Objects.equals(lhs, rhs)) {
                return 0;
            }
            return Comparator.comparing(o -> o.getClass().descriptorString()).compare(lhs, rhs);
        });

        final var valueComparator = Comparator.nullsLast(
                Comparator.comparing(AnyValue::valueRepresentation).thenComparing((lhs, rhs) -> {
                    if (!(lhs instanceof final SequenceValue lhsSequence)
                            || !(rhs instanceof final SequenceValue rhsSequence)) {
                        return objectComparator.compare(lhs, rhs);
                    }

                    var comparison = Integer.compare(lhsSequence.length(), rhsSequence.length());
                    if (comparison != 0) {
                        return comparison;
                    }

                    for (int i = 0; i < lhsSequence.length(); i++) {
                        final var lhsElement = lhsSequence.value(i);
                        final var rhsElement = rhsSequence.value(i);

                        if (!(lhsElement instanceof final Value lhsValue)
                                || !(rhsElement instanceof final Value rhsValue)) {
                            return objectComparator.compare(lhsElement, rhsElement);
                        }

                        comparison = Values.COMPARATOR.compare(lhsValue, rhsValue);
                        if (comparison != 0) {
                            return comparison;
                        }
                    }
                    return 0;
                }));

        // set valid Cosine vectors

        SIMPLE_COSINE_VALID_VECTORS = addListValueVersions(Lists.mutable
                        .<AnyValue>withAll(floatFiniteNonZeroRegularArrays)
                        .withAll(floatFiniteNonZeroExtremeIntegralArrays)
                        .withAll(floatFiniteSquareL2NormMixedArrays)
                        .withAll(floatFiniteSquareL2NormIntegralArrays)
                        .withAll(floatFiniteSquareL2NormFloatingPointArrays)
                        .toSortedSet(valueComparator))
                .toImmutableSortedSet(valueComparator);

        L2_NORM_COSINE_VALID_VECTORS = addListValueVersions(Lists.mutable
                        .withAll(SIMPLE_COSINE_VALID_VECTORS)
                        .withAll(doubleFiniteSquareL2NormDoubleArrays)
                        .toSortedSet(valueComparator))
                .toImmutableSortedSet(valueComparator);

        // set valid Euclidean vectors

        EUCLIDEAN_VALID_VECTORS = addListValueVersions(Lists.mutable
                        .withAll(SIMPLE_COSINE_VALID_VECTORS)
                        .withAll(floatFiniteZeroPrimitiveIntegralArrays)
                        .withAll(floatFiniteZeroFloatingPointArrays)
                        .withAll(floatFiniteNonZeroExtremeFloatArrays)
                        .withAll(floatFiniteNonZeroExtremeDoubleArrays)
                        .toSortedSet(valueComparator))
                .toImmutableSortedSet(valueComparator);

        // set invalid Euclidean vectors

        EUCLIDEAN_INVALID_VECTORS = addListValueVersions(Lists.mutable
                        .<AnyValue>with(Values.NO_VALUE)
                        .with(null)
                        .withAll(nonFloatFiniteIntegralArrays)
                        .withAll(nonFloatFiniteFloatingPointArrays)
                        .withAll(nonNumericArrays)
                        .toSortedSet(valueComparator))
                .toSortedSet(valueComparator)
                .asUnmodifiable();

        // set invalid Cosine vectors

        L2_NORM_COSINE_INVALID_VECTORS = addListValueVersions(Lists.mutable
                        .<AnyValue>with(Values.NO_VALUE)
                        .with(null)
                        .withAll(doubleFiniteNonZeroExtremeDoubleArrays)
                        .withAll(nonDoubleFiniteZeroSquareL2NormIntegralArrays)
                        .withAll(nonDoubleFiniteSquareL2NormDoubleArrays)
                        .withAll(nonDoubleFiniteDoubleArrays)
                        .withAll(nonNumericArrays)
                        .toSortedSet(valueComparator))
                .toSortedSet(valueComparator)
                .asUnmodifiable();

        SIMPLE_COSINE_INVALID_VECTORS = addListValueVersions(Lists.mutable
                        .withAll(EUCLIDEAN_INVALID_VECTORS)
                        .withAll(L2_NORM_COSINE_INVALID_VECTORS)
                        .withAll(floatFiniteNonZeroExtremeFloatArrays)
                        .withAll(nonFloatFiniteSquareL2NormIntegralArrays)
                        .withAll(nonFloatFiniteSquareL2NormFloatingPointArrays)
                        .withAll(nonFloatFiniteSquareL2NormMixedArrays)
                        .toSortedSet(valueComparator))
                .toSortedSet(valueComparator)
                .asUnmodifiable();
    }

    private static ArrayValue toArrayValue(Object array) {
        return Values.of(array) instanceof final ArrayValue arrayValue ? arrayValue : null;
    }

    private static ListValue convertEvenElementsToStringValues(ArrayValue arrayValue) {
        final var array = new AnyValue[arrayValue.length()];
        for (int i = 0; i < array.length; i++) {
            final var value = (Value) arrayValue.value(i);
            array[i] = (i & 1) == 0 ? Values.stringValue(value.prettyPrint()) : value;
        }
        return VirtualValues.list(array);
    }

    private static RichIterable<AnyValue> addListValueVersions(RichIterable<? extends AnyValue> values) {
        // converter to ListValue implementations, but alternate between different sources
        final var converter = new ListValueConverter();
        return Lists.mutable
                .<AnyValue>withAll(values)
                .withAll(values.asLazy().selectInstancesOf(ArrayValue.class).collect(converter::toListValue));
    }

    private static class ListValueConverter {
        private int counter = -1;

        ListValue toListValue(ArrayValue arrayValue) {
            counter++;
            counter %= ListValueType.VALUES.length;
            return ListValueType.VALUES[counter].toListValue(arrayValue);
        }

        private enum ListValueType {
            PRIMITIVE_ARRAY {
                @Override
                ListValue toListValue(ArrayValue arrayValue) {
                    final var array = new AnyValue[arrayValue.length()];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = arrayValue.value(i);
                    }
                    return VirtualValues.list(array);
                }
            },

            LIST {
                @Override
                ListValue toListValue(ArrayValue arrayValue) {
                    final var list = new ArrayList<AnyValue>(arrayValue.length());
                    for (final var element : arrayValue) {
                        list.add(element);
                    }
                    return VirtualValues.fromList(list);
                }
            },

            ARRAY_VALUE {
                @Override
                ListValue toListValue(ArrayValue arrayValue) {
                    return VirtualValues.fromArray(arrayValue);
                }
            };

            static final ListValueType[] VALUES = values();

            abstract ListValue toListValue(ArrayValue arrayValue);
        }
    }

    private static double extremeSameFloatValue(double value) {
        final var floatSignificandWidth = 24; // jdk.internal.math.FloatConsts.SIGNIFICAND_WIDTH
        final var doubleSignificandWidth = 53; // jdk.internal.math.DoubleConsts.SIGNIFICAND_WIDTH
        final var mask = (1 << (doubleSignificandWidth - floatSignificandWidth - 1)) - 1;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(value) | mask);
    }

    // just a convenience method as primitive arrays lack a "constructor"-like interface
    private static byte[] toPrimitive(byte... array) {
        return array;
    }

    private static short[] toPrimitive(short... array) {
        return array;
    }

    private static int[] toPrimitive(int... array) {
        return array;
    }

    private static long[] toPrimitive(long... array) {
        return array;
    }

    private static float[] toPrimitive(float... array) {
        return array;
    }

    private static double[] toPrimitive(double... array) {
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

    private static LazyIterable<long[]> signPermutations(long... values) {
        if (values == null) {
            return null;
        }
        final var n = 1 << values.length;
        final var perms = Lists.mutable.<long[]>withInitialCapacity(n);
        for (int p = 0; p < n; p++) {
            final var perm = new long[values.length];
            for (int i = 0; i < values.length; i++) {
                final var value = values[i];
                final var flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static LazyIterable<float[]> signPermutations(float... values) {
        if (values == null) {
            return null;
        }

        final var n = 1 << values.length;
        final var perms = Lists.mutable.<float[]>withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            final var perm = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                final var value = values[i];
                final var flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static LazyIterable<double[]> signPermutations(double... values) {
        if (values == null) {
            return null;
        }

        final var n = 1 << values.length;
        final var perms = Lists.mutable.<double[]>withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            final var perm = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                final var value = values[i];
                final var flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static LazyIterable<ListValue> signPermutations(NumberValue... values) {
        if (values == null) {
            return null;
        }

        final var n = 1 << values.length;
        final var perms = Lists.mutable.<ListValue>withInitialCapacity(n);

        final var zero = Values.longValue(0);

        for (int p = 0; p < n; p++) {
            final var perm = new NumberValue[values.length];
            for (int i = 0; i < values.length; i++) {
                final var value = values[i];
                final var flip = (p & 1 << i) != 0;
                perm[i] = flip ? zero.minus(value) : value;
            }
            perms.add(VirtualValues.list(perm));
        }
        return perms.asLazy();
    }

    public static class VectorIndexSettings {
        private final MutableMap<IndexSetting, Object> settings = Maps.mutable.empty();

        private VectorIndexSettings() {}

        public static VectorIndexSettings create() {
            return new VectorIndexSettings();
        }

        public static VectorIndexSettings from(Map<IndexSetting, Object> settings) {
            final var vectorIndexSettings = create();
            settings.forEach(vectorIndexSettings::set);
            return vectorIndexSettings;
        }

        public static VectorIndexSettings from(IndexConfig config) {
            return from(IndexSettingUtil.toIndexSettingObjectMapFromIndexConfig(config));
        }

        public VectorIndexSettings set(IndexSetting setting, Object value) {
            settings.put(setting, value);
            return this;
        }

        public VectorIndexSettings unset(IndexSetting setting) {
            settings.remove(setting);
            return this;
        }

        public VectorIndexSettings withDimensions(int dimensions) {
            return set(IndexSetting.vector_Dimensions(), dimensions);
        }

        public VectorIndexSettings withSimilarityFunction(VectorSimilarityFunction similarityFunction) {
            return withSimilarityFunction(similarityFunction.name());
        }

        public VectorIndexSettings withSimilarityFunction(String similarityFunction) {
            return set(IndexSetting.vector_Similarity_Function(), similarityFunction);
        }

        public VectorIndexSettings withQuantization(VectorQuantization quantization) {
            return withQuantization(quantization.name());
        }

        public VectorIndexSettings withQuantization(String quantization) {
            return set(IndexSetting.vector_Quantization(), quantization);
        }

        public VectorIndexSettings withHnswM(int M) {
            return set(IndexSetting.vector_Hnsw_M(), M);
        }

        public VectorIndexSettings withHnswEfConstruction(int efConstruction) {
            return set(IndexSetting.vector_Hnsw_Ef_Construction(), efConstruction);
        }

        public IndexConfig toIndexConfig() {
            return IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap(settings);
        }

        public IndexConfig toIndexConfigWith(VectorIndexVersion version) {
            return version.indexSettingValidator()
                    .validateToVectorIndexConfig(toSettingsAccessor())
                    .config();
        }

        public Map<IndexSetting, Object> toMap() {
            return settings.asUnmodifiable();
        }

        public Map<IndexSetting, Object> toMapWith(VectorIndexVersion version) {
            return from(toIndexConfigWith(version)).toMap();
        }

        public SortedMap<String, Object> toStringObjectMap() {
            return settings.keyValuesView()
                    .toSortedMap(
                            String.CASE_INSENSITIVE_ORDER, kv -> kv.getOne().getSettingName(), Pair::getTwo)
                    .asUnmodifiable();
        }

        public SortedMap<String, Object> toStringObjectMapWith(VectorIndexVersion version) {
            return from(toIndexConfigWith(version)).toStringObjectMap();
        }

        public MapValue toMapValue() {
            final var mapBuilder = new MapValueBuilder(settings.size());
            settings.keyValuesView()
                    .forEach(kv -> mapBuilder.add(kv.getOne().getSettingName(), Values.of(kv.getTwo())));
            return mapBuilder.build();
        }

        public MapValue toMapValueWith(VectorIndexVersion version) {
            return from(toIndexConfigWith(version)).toMapValue();
        }

        public SettingsAccessor toSettingsAccessor() {
            return new IndexSettingObjectMapAccessor(toMap());
        }

        public SettingsAccessor toSettingsAccessorWith(VectorIndexVersion version) {
            return new IndexSettingObjectMapAccessor(toMapWith(version));
        }
    }
}
