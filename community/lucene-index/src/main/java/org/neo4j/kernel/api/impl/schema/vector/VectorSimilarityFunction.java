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

import java.util.EnumSet;
import java.util.Locale;

public enum VectorSimilarityFunction {
    EUCLIDEAN,
    COSINE;

    static final EnumSet<VectorSimilarityFunction> SUPPORTED = EnumSet.allOf(VectorSimilarityFunction.class);

    public float compare(float[] vector1, float[] vector2) {
        return toLucene().compare(vector1, vector2);
    }

    public static VectorSimilarityFunction fromName(String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "'%s' is an unsupported vector similarity function. Supported: %s".formatted(name, SUPPORTED), e);
        }
    }

    org.apache.lucene.index.VectorSimilarityFunction toLucene() {
        return switch (this) {
            case EUCLIDEAN -> org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
            case COSINE -> org.apache.lucene.index.VectorSimilarityFunction.COSINE;
        };
    }
}
