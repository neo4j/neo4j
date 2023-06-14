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

import java.util.List;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

public class VectorUtils {

    public static int vectorDimensionsFrom(IndexConfig config) {
        final var setting = IndexSetting.vector_Dimensions();
        final var dimensions =
                VectorUtils.<IntegralValue>getExpectedFrom(config, setting).intValue();
        if (dimensions <= 0) {
            throw new IllegalArgumentException(
                    "Invalid %s provided.".formatted(IndexConfig.class.getSimpleName()),
                    new AssertionError("'%s' is expected to be non-negative".formatted(setting.getSettingName())));
        }
        return dimensions;
    }

    public static VectorSimilarityFunction vectorSimilarityFunctionFrom(IndexConfig config) {
        try {
            return VectorSimilarityFunction.fromName(
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

    // TODO VECTOR: perhaps some unrolling and/or vector api (when available) could be used here

    public static float[] maybeToValidVector(Value candidate) {
        if (!(candidate instanceof final FloatingPointArray array)) {
            return null;
        }
        return maybeToValidVector(array);
    }

    public static float[] maybeToValidVector(FloatingPointArray candidate) {
        if (candidate == null || candidate.isEmpty()) {
            return null;
        }

        final var dimensions = candidate.length();
        final var vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            final var element = candidate.floatValue(i);
            if (!Float.isFinite(element)) {
                return null;
            }
            vector[i] = element;
        }
        return vector;
    }

    public static float[] maybeToValidVector(List<Double> candidate) {
        if (candidate == null || candidate.isEmpty()) {
            return null;
        }

        final var dimensions = candidate.size();
        final var vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            final var rawElement = candidate.get(i);
            final float element;
            if (rawElement == null || !Float.isFinite(element = rawElement.floatValue())) {
                return null;
            }
            vector[i] = element;
        }
        return vector;
    }

    private VectorUtils() {}
}
