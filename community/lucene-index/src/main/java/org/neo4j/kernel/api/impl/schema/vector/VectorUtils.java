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
package org.neo4j.kernel.api.impl.schema.vector;

import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class VectorUtils {
    public static final int MAX_DIMENSIONS = 2048;

    public static int vectorDimensionsFrom(IndexConfig config) {
        final var setting = IndexSetting.vector_Dimensions();
        final var dimensions =
                VectorUtils.<IntegralValue>getExpectedFrom(config, setting).intValue();
        if (dimensions <= 0) {
            throw new IllegalArgumentException(
                    "Invalid %s provided.".formatted(IndexConfig.class.getSimpleName()),
                    new AssertionError("'%s' is expected to be positive. Provided: %d"
                            .formatted(setting.getSettingName(), dimensions)));
        }
        return dimensions;
    }

    public static VectorSimilarityFunction vectorSimilarityFunctionFrom(IndexConfig config) {
        try {
            return VectorSimilarityFunctions.fromName(
                    VectorUtils.<TextValue>getExpectedFrom(config, IndexSetting.vector_Similarity_Function())
                            .stringValue());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid %s provided.".formatted(IndexConfig.class.getSimpleName()), e);
        }
    }

    private static <T extends Value> T getExpectedFrom(IndexConfig config, IndexSetting setting) {
        final var name = setting.getSettingName();
        return config.getOrThrow(
                name,
                () -> new IllegalArgumentException(
                        "Invalid %s provided.".formatted(IndexConfig.class.getSimpleName()),
                        new AssertionError("'%s' is expected to have been set".formatted(name))));
    }

    public static FloatingPointArray maybeToFloatingPointArray(AnyValue candidate) {
        if (candidate == null) {
            return null;
        }

        if (candidate instanceof final FloatingPointArray floatingPointArray) {
            return floatingPointArray;
        }

        if (candidate instanceof final SequenceValue list) {
            return maybeToFloatingPointArray(list);
        }

        return null;
    }

    public static FloatingPointArray maybeToFloatingPointArray(SequenceValue candidate) {
        if (candidate == null) {
            return null;
        }

        final var array = new double[candidate.length()];
        for (int i = 0; i < array.length; i++) {
            if (!(candidate.value(i) instanceof final NumberValue number)) {
                return null;
            }
            array[i] = number.doubleValue();
        }
        return Values.doubleArray(array);
    }

    private VectorUtils() {}
}
