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
package org.neo4j.kernel.api.vector;

import java.util.List;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.VectorValue;

public class VectorUtil {
    private static final VectorUtilSupport IMPL = VectorSupportUtilHolder.INSTANCE;

    private VectorUtil() {}

    public static float magnitude(VectorCandidate vector) {
        return l2Norm(vector);
    }

    public static float dotProduct(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("dotProduct", vector1, vector2);
        float r = IMPL.dotProduct(vector1, vector2);
        assert Float.isFinite(r);
        return r;
    }

    public static float cosine(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("cosine", vector1, vector2);
        float r = IMPL.cosine(vector1, vector2);
        assert Float.isFinite(r);
        return r;
    }

    public static float l1Distance(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("l1Distance", vector1, vector2);
        float r = IMPL.l1Distance(vector1, vector2);
        assert Float.isFinite(r);
        return r;
    }

    public static float l1Norm(VectorCandidate vector) {
        preconditions(vector);
        float r = IMPL.l1Norm(vector);
        assert Float.isFinite(r);
        return r;
    }

    public static VectorValue l1NormalizedVector(VectorCandidate vector) {
        preconditions(vector);
        float scale = 1.0f / IMPL.l1Norm(vector);
        assert Float.isFinite(scale);
        return IMPL.scale(vector, scale);
    }

    public static float squareL2Distance(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("squareL2Distance", vector1, vector2);
        float r = IMPL.squareL2Distance(vector1, vector2);
        assert Float.isFinite(r);
        return r;
    }

    public static float l2Distance(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("l2Distance", vector1, vector2);
        float r = (float) Math.sqrt(IMPL.squareL2Distance(vector1, vector2));
        assert Float.isFinite(r);
        return r;
    }

    public static float l2Norm(VectorCandidate vector) {
        preconditions(vector);
        float r = (float) Math.sqrt(IMPL.squareL2Norm(vector));
        assert Float.isFinite(r);
        return r;
    }

    public static VectorValue l2NormalizedVector(VectorCandidate vector) {
        preconditions(vector);
        float scale = (float) (1.0 / Math.sqrt(IMPL.squareL2Norm(vector)));
        assert Float.isFinite(scale);
        return IMPL.scale(vector, scale);
    }

    public static int hammingDistance(VectorCandidate vector1, VectorCandidate vector2) {
        preconditions("hammingDistance", vector1, vector2);
        return IMPL.hammingDistance(vector1, vector2);
    }

    static boolean valid(VectorCandidate vector) {
        int dimensions;
        if (vector == null || (dimensions = vector.dimensions()) <= 0) {
            return false;
        }
        for (int i = 0; i < dimensions; i++) {
            if (!Float.isFinite(vector.floatValue(i))) {
                return false;
            }
        }
        return true;
    }

    static boolean origin(VectorCandidate vector) {
        preconditions(vector);
        int dimensions = vector.dimensions();
        for (int i = 0; i < dimensions; i++) {
            if (vector.floatValue(i) != 0.0f) {
                return false;
            }
        }
        return true;
    }

    private static void preconditions(VectorCandidate vector) {
        if (vector == null) {
            ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                    .withParam(GqlParams.StringParam.input, "null")
                    .withParam(GqlParams.StringParam.context, "vector")
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22004)
                            .build())
                    .build();
            throw new InvalidArgumentException(gql, "Vector cannot be null.");
        }

        int dimensions = vector.dimensions();
        if (dimensions < 1) {
            ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22003)
                    .withParam(GqlParams.StringParam.value, String.valueOf(dimensions))
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N86)
                            .build())
                    .build();
            throw new InvalidArgumentException(
                    gql, String.format("Vector dimensions requires positive integer argument, got `%d`.", dimensions));
        }
    }

    private static void preconditions(String function, VectorCandidate vector1, VectorCandidate vector2) {
        preconditions(vector1);
        preconditions(vector2);
        int dimension1 = vector1.dimensions();
        int dimension2 = vector2.dimensions();
        if (dimension1 != dimension2) {
            ErrorGqlStatusObject gql = GqlHelper.getGql22N38_22N04(
                    function,
                    "(%dd-vector, %dd-vector)".formatted(dimension1, dimension2),
                    function,
                    List.of("vectors withs the same dimensions"));
            throw new InvalidArgumentException(
                    gql, "Vector dimensions are required to be the same: %d != %d.".formatted(dimension1, dimension2));
        }
    }
}
