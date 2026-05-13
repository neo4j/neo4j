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
package org.neo4j.internal.schema.constraints;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.neo4j.graphdb.Vector.CoordinateType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.values.storable.VectorValue;

public final class VectorType implements ConstrainableType {
    private static final String CYPHER_USER_DESCRIPTION = "VECTOR<%s>(%d)";

    private final CoordinateType coordinateType;
    private final int dimensions;
    private final Ordering order;

    private VectorType(CoordinateType coordinateType, int dimensions) {
        if (dimensions < VectorValue.MIN_VECTOR_DIMENSIONS || dimensions > VectorValue.MAX_VECTOR_DIMENSIONS) {
            throw new IllegalArgumentException("Required %d <= %d <= %d"
                    .formatted(VectorValue.MIN_VECTOR_DIMENSIONS, dimensions, VectorValue.MAX_VECTOR_DIMENSIONS));
        }
        this.coordinateType = coordinateType;
        this.order = toOrdering(coordinateType);
        this.dimensions = dimensions;
    }

    public int dimensions() {
        return dimensions;
    }

    public CoordinateType coordinateType() {
        return coordinateType;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[inner=%s, dimensions=%d]".formatted(coordinateType, dimensions);
    }

    @Override
    public boolean equals(Object other) {
        // Since we intern values, we only consider identity when testing for equality.
        return super.equals(other);
    }

    @Override
    public Ordering order() {
        return order;
    }

    @Override
    public String userDescription() {
        return CYPHER_USER_DESCRIPTION.formatted(coordinateType.normalizedCypherString(), dimensions);
    }

    public static VectorType int8Vector(int dimension) {
        return getInterned(CoordinateType.INTEGER8, dimension);
    }

    public static VectorType int16Vector(int dimension) {
        return getInterned(CoordinateType.INTEGER16, dimension);
    }

    public static VectorType int32Vector(int dimension) {
        return getInterned(CoordinateType.INTEGER32, dimension);
    }

    public static VectorType int64Vector(int dimension) {
        return getInterned(CoordinateType.INTEGER64, dimension);
    }

    public static VectorType float32Vector(int dimension) {
        return getInterned(CoordinateType.FLOAT32, dimension);
    }

    public static VectorType float64Vector(int dimension) {
        return getInterned(CoordinateType.FLOAT64, dimension);
    }

    @Override
    public PropertyType toPublicApi() {
        return PropertyType.VECTOR;
    }

    private static Ordering toOrdering(CoordinateType type) {
        return switch (type) {
            case INTEGER8 -> Ordering.VECTOR_INT8_ORDER;
            case INTEGER16 -> Ordering.VECTOR_INT16_ORDER;
            case INTEGER32 -> Ordering.VECTOR_INT32_ORDER;
            case INTEGER64 -> Ordering.VECTOR_INT64_ORDER;
            case FLOAT32 -> Ordering.VECTOR_FLOAT32_ORDER;
            case FLOAT64 -> Ordering.VECTOR_FLOAT64_ORDER;
        };
    }

    private static final ImmutableMap<CoordinateType, ConcurrentHashMap<Integer, VectorType>> INTERNED =
            Lists.fixedSize.with(CoordinateType.values()).toImmutableMap(c -> c, c -> new ConcurrentHashMap<>());

    private static VectorType getInterned(CoordinateType inner, int dimension) {
        /* To avoid that each Value has a distinct VectorType we intern the created values. */
        Map<Integer, VectorType> map = INTERNED.get(inner);
        assert map != null; // Created statically from all InnerTypes
        return map.computeIfAbsent(dimension, (ignored) -> new VectorType(inner, dimension));
    }

    private static final String SERIALIZE_PATTERN = "VECTOR[coordinate=%s, dimensions=%d]";
    private static final Pattern DESERIALIZE_PATTERN =
            Pattern.compile("VECTOR\\[coordinate=(\\w+), dimensions=(\\d+)\\]");

    @Override
    public String serialize() {
        return SERIALIZE_PATTERN.formatted(coordinateType, dimensions);
    }

    public static VectorType deserialize(String str) throws IllegalArgumentException {
        Matcher matcher = DESERIALIZE_PATTERN.matcher(str);
        if (matcher.matches()) {
            return getInterned(CoordinateType.valueOf(matcher.group(1)), Integer.parseInt(matcher.group(2)));
        }
        throw new IllegalArgumentException(str);
    }
}
