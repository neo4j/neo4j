/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema.vector;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.DoubleLists;
import org.eclipse.collections.api.factory.primitive.FloatLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.impl.index.LuceneSettings;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorTestUtils {

    // subject to change
    public static final Map<Setting<?>, Object> SUGGESTED_SETTINGS =
            Map.of(LuceneSettings.lucene_merge_factor, 1000, LuceneSettings.lucene_population_ram_buffer_size, 1.0);

    public static final Iterable<List<Double>> VALID_VECTORS_FROM_DOUBLE_LIST;
    public static final Iterable<List<Double>> INVALID_VECTORS_FROM_DOUBLE_LIST;
    public static final Iterable<Value> VALID_VECTORS_FROM_VALUE;
    public static final Iterable<Value> INVALID_VECTORS_FROM_VALUE;

    public static Iterable<List<Double>> validVectorsFromDoubleList() {
        return VALID_VECTORS_FROM_DOUBLE_LIST;
    }

    public static Iterable<List<Double>> invalidVectorsFromDoubleList() {
        return INVALID_VECTORS_FROM_DOUBLE_LIST;
    }

    public static Iterable<Value> validVectorsFromValue() {
        return VALID_VECTORS_FROM_VALUE;
    }

    public static Iterable<Value> invalidVectorsFromValue() {
        return INVALID_VECTORS_FROM_VALUE;
    }

    static {
        final var smallerDoubleThanSmallestFloatButSameValue = extremeSameFloatValue(-Float.MAX_VALUE);
        final var smallerDoubleThanSmallestFloat = Math.nextDown(smallerDoubleThanSmallestFloatButSameValue);
        final var largerDoubleThanLargestFloatButSameValue = extremeSameFloatValue(+Float.MAX_VALUE);
        final var largerDoubleThanLargestFloat = Math.nextUp(largerDoubleThanLargestFloatButSameValue);

        final var validPrimitiveFloats = FloatLists.immutable.of(
                -Float.MAX_VALUE,
                -Float.MIN_NORMAL,
                -Float.MIN_VALUE,
                -0.f,
                +0.f,
                +Float.MIN_VALUE,
                +Float.MIN_NORMAL,
                +Float.MAX_VALUE);
        final var validPrimitiveFloatArrays = validPrimitiveFloats.collect(VectorTestUtils::toPrimitive);

        final var invalidPrimitiveFloats =
                FloatLists.immutable.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
        final var invalidPrimitiveFloatArrays = FloatLists.mutable
                .withAll(invalidPrimitiveFloats)
                .collect(VectorTestUtils::toPrimitive)
                .with(new float[0])
                .with(null)
                .toImmutableList();

        final var validBoxedFloatArrays = validPrimitiveFloatArrays.collect(ArrayUtils::toObject);
        final var invalidBoxedFloatArrays = invalidPrimitiveFloatArrays.collect(ArrayUtils::toObject);

        final var validFloatArrays = Lists.mutable
                .withAll(validPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(validBoxedFloatArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var invalidFloatArrays = Lists.mutable
                .withAll(invalidPrimitiveFloatArrays.asLazy().collect(Values::of))
                .withAll(invalidBoxedFloatArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .toImmutableList();

        final var validPrimitiveDoubleArrays = DoubleLists.mutable
                .of(smallerDoubleThanSmallestFloatButSameValue, largerDoubleThanLargestFloatButSameValue)
                .withAll(validPrimitiveFloats.asLazy().collectDouble(v -> v))
                .asLazy()
                .collect(VectorTestUtils::toPrimitive)
                .toImmutableList();

        final var invalidPrimitiveDoubleArrays = DoubleLists.mutable
                .of(smallerDoubleThanSmallestFloat, largerDoubleThanLargestFloat)
                .withAll(invalidPrimitiveFloats.asLazy().collectDouble(v -> v))
                .collect(VectorTestUtils::toPrimitive)
                .with(null)
                .toImmutableList();

        final var validBoxedDoubleArrays = validPrimitiveDoubleArrays.collect(ArrayUtils::toObject);
        final var invalidBoxedDoubleArrays = invalidPrimitiveDoubleArrays.collect(ArrayUtils::toObject);

        VALID_VECTORS_FROM_DOUBLE_LIST = validBoxedDoubleArrays
                .asLazy()
                .collect(Lists.immutable::of)
                .collect(ImmutableList::castToList)
                .toImmutableList();

        INVALID_VECTORS_FROM_DOUBLE_LIST = Lists.mutable
                .withAll(invalidBoxedDoubleArrays
                        .asLazy()
                        .collect(Lists.immutable::of)
                        .collect(ImmutableList::castToList))
                .with(null)
                .toImmutableList();

        final var validDoubleArrays = Lists.mutable
                .withAll(validPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(validBoxedDoubleArrays.asLazy().collect(Values::of))
                .toImmutableList();

        final var invalidDoubleArrays = Lists.mutable
                .withAll(invalidPrimitiveDoubleArrays.asLazy().collect(Values::of))
                .withAll(invalidBoxedDoubleArrays.asLazy().collect(Values::of))
                .with(NO_VALUE)
                .with(null)
                .toImmutableList();

        VALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(validFloatArrays)
                .withAll(validDoubleArrays)
                .toImmutableList();

        INVALID_VECTORS_FROM_VALUE = Lists.mutable
                .withAll(invalidFloatArrays)
                .withAll(invalidDoubleArrays)
                .with(null)
                .toImmutableList();
    }

    // just a convenience method as primitive arrays lack a "constructor"-like interface
    private static float[] toPrimitive(float... array) {
        return array;
    }

    private static double[] toPrimitive(double... array) {
        return array;
    }

    private static double extremeSameFloatValue(double value) {
        final var floatSignificandWidth = 24; // jdk.internal.math.FloatConsts.SIGNIFICAND_WIDTH
        final var doubleSignificandWidth = 53; // jdk.internal.math.DoubleConsts.SIGNIFICAND_WIDTH
        final var mask = (1 << (doubleSignificandWidth - floatSignificandWidth - 1)) - 1;
        return Double.longBitsToDouble(Double.doubleToRawLongBits(value) | mask);
    }

    private VectorTestUtils() {}
}
