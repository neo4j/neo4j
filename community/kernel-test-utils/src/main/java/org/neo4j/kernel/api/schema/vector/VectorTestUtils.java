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

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.LazyDoubleIterable;
import org.eclipse.collections.api.LazyFloatIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.factory.primitive.ByteLists;
import org.eclipse.collections.api.factory.primitive.DoubleLists;
import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.factory.primitive.ShortLists;
import org.eclipse.collections.api.list.MutableList;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingUtil;
import org.neo4j.internal.helpers.NameUtil;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexSettingObjectMapAccessor;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorQuantizationType;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.test.LatestVersions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.AnyValueWriter.EntityMode;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.utils.PrettyPrinter;
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

    public static RichIterable<AnyValue> validVectorsFor(Neo4jVectorSimilarityFunction function) {
        return switch (function) {
            case EUCLIDEAN -> EUCLIDEAN_VALID_VECTORS;
            case SIMPLE_COSINE -> SIMPLE_COSINE_VALID_VECTORS;
            case L2_NORM_COSINE -> L2_NORM_COSINE_VALID_VECTORS;
        };
    }

    public static RichIterable<AnyValue> invalidVectorsFor(Neo4jVectorSimilarityFunction function) {
        return switch (function) {
            case EUCLIDEAN -> EUCLIDEAN_INVALID_VECTORS;
            case SIMPLE_COSINE -> SIMPLE_COSINE_INVALID_VECTORS;
            case L2_NORM_COSINE -> L2_NORM_COSINE_INVALID_VECTORS;
        };
    }

    static {
        // A bit of a mess, but ensures many extreme combinations of:
        //  * valid/invalid vector candidate
        //  * each value source
        //  * each similarity function
        // in structures that can hold all values including null

        double smallerDoubleThanSmallestFloatButSameValue = extremeSameFloatValue(-Float.MAX_VALUE);
        double smallerDoubleThanSmallestFloat = Math.nextDown(smallerDoubleThanSmallestFloatButSameValue);
        double largerDoubleThanLargestFloatButSameValue = extremeSameFloatValue(+Float.MAX_VALUE);
        double largerDoubleThanLargestFloat = Math.nextUp(largerDoubleThanLargestFloatButSameValue);
        float squareRootSmallestPositiveFloat = (float) Math.sqrt(Math.nextUp(+0.f));
        float squareRootLargestFloat = (float) Math.sqrt(Float.MAX_VALUE);
        float squareRootHalfLargestFloat = (float) Math.sqrt(Float.MAX_VALUE / 2.f);
        double squareRootSmallestPositiveDouble = Math.sqrt(Math.nextUp(+0.0));
        double squareRootLargestDouble = Math.sqrt(Double.MAX_VALUE);
        double squareRootHalfLargestDouble = Math.sqrt(Double.MAX_VALUE / 2.0);
        double largerThanSquareRootLargestDouble = Math.nextUp(Math.sqrt(Double.MAX_VALUE));

        // non-zero normal values

        Iterable<Value> floatFiniteNonZeroRegularArrays = Lists.mutable
                .withAll(toArrayAndVectorValues(toPrimitive((byte) 42)))
                .withAll(toArrayAndVectorValues(toPrimitive((short) -1234)))
                .withAll(toArrayAndVectorValues(toPrimitive(0xdeadbeaf)))
                .withAll(toArrayAndVectorValues(toPrimitive(-1234567890987654321L)))
                .withAll(toArrayAndVectorValues(toPrimitive((float) Math.E)))
                .withAll(toArrayAndVectorValues(toPrimitive(Math.PI)))
                .asLazy();

        // integral non-zero extreme values

        Iterable<Value> floatFiniteNonZeroExtremeIntegralArrays = Lists.mutable
                .withAll(ByteLists.immutable
                        .of((byte) -Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(ShortLists.immutable
                        .of((short) -Short.MAX_VALUE, Short.MIN_VALUE, Short.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(IntLists.immutable
                        .of(-Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(LongLists.immutable
                        .of(-Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE)
                        .asLazy()
                        .collect(VectorTestUtils::toPrimitive)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .asLazy();

        // finite non-zero extreme values

        LazyFloatIterable floatFiniteNonZeroPositiveExtremePrimitiveFloats = FloatLists.immutable
                .of(Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE)
                .asLazy();

        RichIterable<float[]> floatFiniteNonZeroExtremePrimitiveFloatArrays =
                floatFiniteNonZeroPositiveExtremePrimitiveFloats.flatCollect(VectorTestUtils::signPermutations);

        Iterable<Value> floatFiniteNonZeroExtremeFloatArrays = Lists.mutable
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.flatCollect(
                        VectorTestUtils::toArrayAndVectorValues))
                .asLazy();

        Iterable<Value> floatFiniteNonZeroExtremeDoubleArrays = Lists.mutable
                .of(
                        smallerDoubleThanSmallestFloatButSameValue,
                        largerDoubleThanLargestFloatButSameValue,
                        Double.MIN_VALUE,
                        Double.MIN_NORMAL)
                .flatCollect(VectorTestUtils::signPermutations)
                .withAll(floatFiniteNonZeroExtremePrimitiveFloatArrays.collect(VectorTestUtils::promote))
                .asLazy()
                .flatCollect(VectorTestUtils::toArrayAndVectorValues);

        LazyDoubleIterable doubleFiniteNonZeroPositiveExtremePrimitiveDoubles = DoubleLists.immutable
                .of(Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE)
                .asLazy();

        Iterable<Value> doubleFiniteNonZeroExtremeDoubleArrays = doubleFiniteNonZeroPositiveExtremePrimitiveDoubles
                .flatCollect(VectorTestUtils::signPermutations)
                .flatCollect(VectorTestUtils::toArrayAndVectorValues);

        // finite zero values

        Iterable<Value> floatFiniteZeroPrimitiveIntegralArrays = Lists.mutable
                .withAll(toArrayAndVectorValues(toPrimitive((byte) 0)))
                .withAll(toArrayAndVectorValues(toPrimitive((short) 0)))
                .withAll(toArrayAndVectorValues(toPrimitive(0)))
                .withAll(toArrayAndVectorValues(toPrimitive(0L)))
                .asLazy();

        RichIterable<float[]> floatFiniteZeroPrimitiveFloatArrays = signPermutations(0.f);

        Iterable<Value> floatFiniteZeroFloatingPointArrays = Lists.mutable
                .withAll(floatFiniteZeroPrimitiveFloatArrays.flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(floatFiniteZeroPrimitiveFloatArrays
                        .collect(VectorTestUtils::promote)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .asLazy();

        // finite non-zero sqrt(extreme) values

        LazyFloatIterable floatFiniteNonZeroSqrtExtremePrimitiveFloats =
                floatFiniteNonZeroPositiveExtremePrimitiveFloats
                        .collectDouble(Math::sqrt)
                        .collectFloat(v -> (float) v);

        LazyDoubleIterable doubleFiniteNonZeroSqrtExtremePrimitiveDoubles =
                doubleFiniteNonZeroPositiveExtremePrimitiveDoubles.collectDouble(Math::sqrt);

        // finite square L2 norms

        Iterable<Value> floatFiniteSquareL2NormIntegralArrays =
                signPermutations(Long.MAX_VALUE, Long.MAX_VALUE).flatCollect(VectorTestUtils::toArrayAndVectorValues);

        RichIterable<float[]> floatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, squareRootLargestFloat),
                        toPrimitive(squareRootLargestFloat, 0.f),
                        toPrimitive(squareRootSmallestPositiveFloat, squareRootSmallestPositiveFloat),
                        toPrimitive(squareRootHalfLargestFloat, squareRootHalfLargestFloat))
                .withAll(floatFiniteNonZeroSqrtExtremePrimitiveFloats.collect(VectorTestUtils::toPrimitive))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        Iterable<Value> floatFiniteSquareL2NormFloatingPointArrays = Lists.mutable
                .withAll(floatFiniteSquareL2NormPrimitiveFloatArrays.flatCollect(
                        VectorTestUtils::toArrayAndVectorValues))
                .withAll(floatFiniteSquareL2NormPrimitiveFloatArrays
                        .collect(VectorTestUtils::promote)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .asLazy();

        Iterable<Value> doubleFiniteSquareL2NormDoubleArrays = Lists.mutable
                .of(
                        toPrimitive(0.0, squareRootLargestDouble),
                        toPrimitive(squareRootLargestDouble, 0.0),
                        toPrimitive(squareRootSmallestPositiveDouble, squareRootSmallestPositiveDouble),
                        toPrimitive(squareRootHalfLargestDouble, squareRootHalfLargestDouble))
                .withAll(doubleFiniteNonZeroSqrtExtremePrimitiveDoubles.collect(VectorTestUtils::toPrimitive))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations)
                .flatCollect(VectorTestUtils::toArrayAndVectorValues);

        Iterable<ListValue> floatFiniteSquareL2NormMixedArrays = signPermutations(
                Values.longValue(Long.MAX_VALUE),
                Values.floatValue(Long.MAX_VALUE),
                Values.doubleValue(Long.MAX_VALUE));

        // non-finite values

        Iterable<ArrayValue> nonFloatEmptyIntegralArrays = Lists.mutable
                .of(Values.EMPTY_BYTE_ARRAY, Values.EMPTY_SHORT_ARRAY, Values.EMPTY_INT_ARRAY, Values.EMPTY_LONG_ARRAY)
                .asLazy();

        RichIterable<float[]> nonFloatFinitePrimitiveFloatArrays = Lists.mutable
                .of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)
                .flatCollect(VectorTestUtils::signPermutations)
                .asLazy();

        RichIterable<Double> nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles = Lists.mutable
                .of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                .asLazy();

        MutableList<Value> nonFloatFiniteFloatingPointArrays = Lists.mutable
                .withAll(nonFloatFinitePrimitiveFloatArrays.flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(Lists.mutable
                        .of(smallerDoubleThanSmallestFloat, largerDoubleThanLargestFloat, Double.MAX_VALUE)
                        .withAll(nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles)
                        .flatCollect(VectorTestUtils::signPermutations)
                        .withAll(nonFloatFinitePrimitiveFloatArrays.collect(VectorTestUtils::promote))
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues));

        Iterable<Value> nonDoubleFiniteDoubleArrays = Lists.mutable
                .withAll(nonDoubleFiniteNonZeroPositiveExtremePrimitiveDoubles)
                .flatCollect(VectorTestUtils::signPermutations)
                .asLazy()
                .flatCollect(VectorTestUtils::toArrayAndVectorValues);

        // non-finite square L2 norms

        MutableList<Value> nonFloatFiniteSquareL2NormIntegralArrays =
                Lists.mutable.withAll(signPermutations(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues));

        RichIterable<float[]> nonFloatFiniteSquareL2NormPrimitiveFloatArrays = Lists.mutable
                .of(
                        toPrimitive(0.f, 0.f),
                        toPrimitive(squareRootHalfLargestFloat, Math.nextUp(squareRootHalfLargestFloat)),
                        toPrimitive(Float.MAX_VALUE, Float.MAX_VALUE))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations);

        Iterable<Value> nonFloatFiniteSquareL2NormFloatingPointArrays = Lists.mutable
                .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays
                        .asLazy()
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .withAll(Lists.mutable
                        .withAll(signPermutations(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble))
                        .withAll(nonFloatFiniteSquareL2NormPrimitiveFloatArrays.collect(VectorTestUtils::promote))
                        .flatCollect(VectorTestUtils::toArrayAndVectorValues))
                .asLazy();

        Iterable<ListValue> nonFloatFiniteSquareL2NormMixedArrays = signPermutations(
                Values.longValue(Long.MAX_VALUE),
                Values.floatValue(Long.MAX_VALUE),
                Values.doubleValue(Long.MAX_VALUE),
                Values.longValue(Long.MAX_VALUE));

        Iterable<Value> nonDoubleFiniteZeroSquareL2NormIntegralArrays = Lists.mutable
                .withAll(toArrayAndVectorValues(toPrimitive((byte) 0, (byte) 0)))
                .withAll(toArrayAndVectorValues(toPrimitive((short) 0, (short) 0)))
                .withAll(toArrayAndVectorValues(toPrimitive(0, 0)))
                .withAll(toArrayAndVectorValues(toPrimitive(0L, 0L)))
                .asLazy();

        Iterable<Value> nonDoubleFiniteSquareL2NormDoubleArrays = Lists.mutable
                .of(
                        toPrimitive(0.0, 0.0),
                        toPrimitive(squareRootHalfLargestDouble, Math.nextUp(squareRootHalfLargestDouble)),
                        toPrimitive(largerThanSquareRootLargestDouble, largerThanSquareRootLargestDouble),
                        toPrimitive(Double.MAX_VALUE, Double.MAX_VALUE))
                .asLazy()
                .flatCollect(VectorTestUtils::signPermutations)
                .flatCollect(VectorTestUtils::toArrayAndVectorValues);

        // wrong types

        Iterable<AnyValue> nonNumericArrays = Lists.mutable
                .<AnyValue>withAll(toArrayAndVectorValues(ArrayUtils.toArray("clearly", "not", "numeric")))
                .withAll(Lists.mutable
                        .withAll(floatFiniteNonZeroRegularArrays)
                        .withAll(floatFiniteNonZeroExtremeIntegralArrays)
                        .flatCollect(VectorTestUtils::convertEvenElementsToStringValues))
                .asLazy();

        // now to put them all together
        //   valid = immutable sorted sets
        // invalid = unmodifiable sorted sets (immutable cannot handle null)

        // with some comparators to remove duplicates, but keeping different types
        Comparator<Object> objectComparator = Comparator.nullsLast((lhs, rhs) -> {
            if (Objects.equals(lhs, rhs)) {
                return 0;
            }
            return Comparator.comparing(o -> o.getClass().descriptorString()).compare(lhs, rhs);
        });

        Comparator<AnyValue> valueComparator = Comparator.nullsLast(Comparator.comparing(AnyValue::valueRepresentation)
                .thenComparing((lhs, rhs) -> {
                    if (lhs instanceof SequenceValue lhsSequence && rhs instanceof SequenceValue rhsSequence) {
                        int comparison = Integer.compare(lhsSequence.intSize(), rhsSequence.intSize());
                        if (comparison != 0) {
                            return comparison;
                        }

                        for (int i = 0; i < lhsSequence.intSize(); i++) {
                            AnyValue lhsElement = lhsSequence.value(i);
                            AnyValue rhsElement = rhsSequence.value(i);

                            if (!(lhsElement instanceof Value lhsValue) || !(rhsElement instanceof Value rhsValue)) {
                                return objectComparator.compare(lhsElement, rhsElement);
                            }

                            comparison = Values.COMPARATOR.compare(lhsValue, rhsValue);
                            if (comparison != 0) {
                                return comparison;
                            }
                        }
                        return 0;
                    } else if (lhs instanceof VectorValue lhsVector && rhs instanceof VectorValue rhsVector) {
                        return Values.COMPARATOR.compare(lhsVector, rhsVector);
                    } else {
                        return objectComparator.compare(lhs, rhs);
                    }
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
                        .withAll(nonFloatEmptyIntegralArrays)
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
        return Values.of(array) instanceof ArrayValue arrayValue ? arrayValue : null;
    }

    private static Iterable<Value> toArrayAndVectorValues(Object array) {
        return Lists.immutable.of(toArrayValue(array), toVectorValue(array)).asLazy();
    }

    private static VectorValue toVectorValue(Object vector) {
        // TODO: Vector - Since the codomain of the vector is finite, we could remove
        //  consider to remove tests of these values. But most likely, these tests are still
        //  needed e.g. float lists.
        return switch (vector) {
            case byte[] array -> Values.int8Vector(array);
            case short[] array -> Values.int16Vector(array);
            case int[] array -> Values.int32Vector(array);
            case long[] array -> Values.int64Vector(array);
            case float[] array -> {
                VectorValue.ensureValidDimensions(array.length);
                yield Values.uncheckedFloat32Vector(array);
            }
            case double[] array -> {
                VectorValue.ensureValidDimensions(array.length);
                yield Values.uncheckedFloat64Vector(array);
            }
            default -> null;
        };
    }

    private static Iterable<ListValue> convertEvenElementsToStringValues(Value v) {
        if (v instanceof ArrayValue arrayValue) {
            AnyValue[] array = new AnyValue[arrayValue.intSize()];
            for (int i = 0; i < array.length; i++) {
                Value value = (Value) arrayValue.value(i);
                array[i] = (i & 1) == 0 ? Values.stringValue(value.prettyPrint()) : value;
            }
            return Lists.immutable.of(VirtualValues.list(array)).asLazy();
        } else {
            return Lists.immutable.empty();
        }
    }

    private static RichIterable<AnyValue> addListValueVersions(RichIterable<? extends AnyValue> values) {
        // converter to ListValue implementations, but alternate between different sources
        ListValueConverter converter = new ListValueConverter();
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
                    AnyValue[] array = new AnyValue[arrayValue.intSize()];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = arrayValue.value(i);
                    }
                    return VirtualValues.list(array);
                }
            },

            LIST {
                @Override
                ListValue toListValue(ArrayValue arrayValue) {
                    List<AnyValue> list = new ArrayList<>(arrayValue.intSize());
                    for (AnyValue element : arrayValue) {
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
        int floatSignificandWidth = 24; // jdk.internal.math.FloatConsts.SIGNIFICAND_WIDTH
        int doubleSignificandWidth = 53; // jdk.internal.math.DoubleConsts.SIGNIFICAND_WIDTH
        int mask = (1 << (doubleSignificandWidth - floatSignificandWidth - 1)) - 1;
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

        double[] promoted = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            promoted[i] = array[i];
        }
        return promoted;
    }

    private static RichIterable<long[]> signPermutations(long... values) {
        if (values == null) {
            return null;
        }
        int n = 1 << values.length;
        MutableList<long[]> perms = Lists.mutable.withInitialCapacity(n);
        for (int p = 0; p < n; p++) {
            long[] perm = new long[values.length];
            for (int i = 0; i < values.length; i++) {
                long value = values[i];
                boolean flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static RichIterable<float[]> signPermutations(float... values) {
        if (values == null) {
            return null;
        }

        int n = 1 << values.length;
        MutableList<float[]> perms = Lists.mutable.withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            float[] perm = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                float value = values[i];
                boolean flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static RichIterable<double[]> signPermutations(double... values) {
        if (values == null) {
            return null;
        }

        int n = 1 << values.length;
        MutableList<double[]> perms = Lists.mutable.withInitialCapacity(n);

        for (int p = 0; p < n; p++) {
            double[] perm = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                double value = values[i];
                boolean flip = (p & 1 << i) != 0;
                perm[i] = flip ? -value : value;
            }
            perms.add(perm);
        }
        return perms.asLazy();
    }

    private static RichIterable<ListValue> signPermutations(NumberValue... values) {
        if (values == null) {
            return null;
        }

        int n = 1 << values.length;
        MutableList<ListValue> perms = Lists.mutable.withInitialCapacity(n);

        LongValue zero = Values.longValue(0);

        for (int p = 0; p < n; p++) {
            NumberValue[] perm = new NumberValue[values.length];
            for (int i = 0; i < values.length; i++) {
                NumberValue value = values[i];
                boolean flip = (p & 1 << i) != 0;
                perm[i] = flip ? zero.minus(value) : value;
            }
            perms.add(VirtualValues.list(perm));
        }
        return perms.asLazy();
    }

    public static VectorIndexVersion max(VectorIndexVersion... versions) {
        return Sets.mutable.of(versions).max();
    }

    public static Set<VectorIndexVersion> inclusiveVersionRangeFrom(VectorIndexVersion from) {
        return inclusiveVersionRange(from, LatestVersions.LATEST_VECTOR_INDEX_VERSION);
    }

    public static Set<VectorIndexVersion> inclusiveVersionRange(VectorIndexVersion from, VectorIndexVersion to) {
        int comp = from.compareTo(to);
        if (comp == 0) {
            return VectorIndexVersion.KNOWN_VERSIONS.contains(from) ? Set.of(from) : Set.of();
        } else if (comp > 0) {
            return Set.of();
        }

        Set<VectorIndexVersion> inclusiveVersions = EnumSet.noneOf(VectorIndexVersion.class);
        for (VectorIndexVersion version : VectorIndexVersion.KNOWN_VERSIONS) {
            if (from.compareTo(version) <= 0 && version.compareTo(to) <= 0) {
                inclusiveVersions.add(version);
            }
        }
        return inclusiveVersions;
    }

    public static class VectorIndexSettings {
        private final Map<IndexSetting, Object> settings = new TreeMap<>(INDEX_SETTING_COMPARATOR);

        private VectorIndexSettings() {}

        private VectorIndexSettings(Map<IndexSetting, Object> settings) {
            this.settings.putAll(settings);
        }

        public static VectorIndexSettings create() {
            return new VectorIndexSettings();
        }

        public static VectorIndexSettings from(Map<IndexSetting, Object> settings) {
            return new VectorIndexSettings(settings);
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
            return withSimilarityFunction(similarityFunction.functionName());
        }

        public VectorIndexSettings withSimilarityFunction(String similarityFunction) {
            return set(IndexSetting.vector_Similarity_Function(), similarityFunction);
        }

        public VectorIndexSettings withDefaultSearchExpansionFactor(double expansionFactor) {
            return set(IndexSetting.vector_Default_Search_Expansion_Factor(), expansionFactor);
        }

        public VectorIndexSettings withQuantizationEnabled() {
            return withQuantizationEnabled(true);
        }

        public VectorIndexSettings withQuantizationDisabled() {
            return withQuantizationEnabled(false);
        }

        public VectorIndexSettings withQuantizationEnabled(boolean quantizationEnabled) {
            return set(IndexSetting.vector_Quantization_Enabled(), quantizationEnabled);
        }

        public VectorIndexSettings withQuantizationType(VectorQuantizationType quantizationType) {
            return withQuantizationType(quantizationType.name());
        }

        public VectorIndexSettings withQuantizationType(String quantizationType) {
            return set(IndexSetting.vector_Quantization_Type(), quantizationType);
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
                    .validateToTypedConfig(toSettingsAccessor())
                    .config();
        }

        public Map<IndexSetting, Object> toMap() {
            return Collections.unmodifiableMap(settings);
        }

        public Map<IndexSetting, Object> toMapWith(VectorIndexVersion version) {
            return from(toIndexConfigWith(version)).toMap();
        }

        public SortedMap<String, Object> toStringObjectMap() {
            SortedMap<String, Object> map = new TreeMap<>(CASE_INSENSITIVE_ORDER);
            settings.forEach((setting, value) -> map.put(setting.getSettingName(), value));
            return Collections.unmodifiableSortedMap(map);
        }

        public SortedMap<String, Object> toStringObjectMapWith(VectorIndexVersion version) {
            return from(toIndexConfigWith(version)).toStringObjectMap();
        }

        public SortedMap<String, Value> toStringValueMap() {
            return toIndexConfig().asMap();
        }

        public SortedMap<String, Value> toStringValueMapWith(VectorIndexVersion version) {
            return toIndexConfigWith(version).asMap();
        }

        public MapValue toMapValue() {
            MapValueBuilder mapBuilder = new MapValueBuilder(settings.size());
            settings.forEach(
                    (setting, value) -> mapBuilder.add(setting.getSettingName(), Values.unsafeOf(value, true)));
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

        @Override
        public String toString() {
            PrettyPrinter pp = new PrettyPrinter();
            writeTo(pp);
            return pp.value();
        }

        public String toStringWith(VectorIndexVersion version) {
            PrettyPrinter pp = new PrettyPrinter("'", EntityMode.FULL);
            write(pp, toStringValueMapWith(version));
            return pp.value();
        }

        public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
            write(writer, toStringValueMap());
        }

        private static <E extends Exception> void write(AnyValueWriter<E> writer, Map<String, Value> map) throws E {
            writer.beginMap(map.size());
            for (Entry<String, Value> entry : map.entrySet()) {
                writer.writeString(NameUtil.forceEscapeName(entry.getKey()));
                entry.getValue().writeTo(writer);
            }
            writer.endMap();
        }
    }
}
