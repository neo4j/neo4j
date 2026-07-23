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
package org.neo4j.server.queryapi.request.typed.common.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.neo4j.driver.Value;
import org.neo4j.server.queryapi.exception.UnsupportedTypeException;
import org.neo4j.server.queryapi.request.QueryRequestCypherValue;
import org.neo4j.server.queryapi.types.CypherVectorTypes;

@JsonTypeName(value = "Vector")
public class TypedJsonVectorQueryRequestCypherValue extends TypedJsonQueryRequestCypherValue {

    protected TypedJsonVectorQueryRequestCypherValue(Value value) {
        super(value);
    }

    @JsonCreator
    public static TypedJsonVectorQueryRequestCypherValue of(
            @JsonProperty(value = "_value", required = true) TypedJsonVectorCoordinatesQueryRequestCypherValue value,
            @JsonProperty("$type") String ignored) {
        return new TypedJsonVectorQueryRequestCypherValue(value.value());
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            property = "coordinatesType",
            defaultImpl = TypedJsonVectorCoordinatesNotAvailableQueryRequestCypherValue.class,
            visible = true)
    @JsonSubTypes({
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesINT8QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesINT16QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesINT32QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesINT64QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesFLOAT32QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesFLOAT64QueryRequestCypherValue.class),
        @JsonSubTypes.Type(TypedJsonVectorCoordinatesNotAvailableQueryRequestCypherValue.class),
    })
    public abstract static class TypedJsonVectorCoordinatesQueryRequestCypherValue implements QueryRequestCypherValue {
        private final Value value;

        protected TypedJsonVectorCoordinatesQueryRequestCypherValue(Value value) {
            this.value = value;
        }

        @Override
        public Value value() {
            return value;
        }
    }

    @JsonTypeName(value = "INT8")
    protected static class TypedJsonVectorCoordinatesINT8QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesINT8QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesINT8QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesINT8QueryRequestCypherValue(CypherVectorTypes.INT8.read(coordinates));
        }
    }

    @JsonTypeName(value = "INT16")
    protected static class TypedJsonVectorCoordinatesINT16QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesINT16QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesINT16QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesINT16QueryRequestCypherValue(
                    CypherVectorTypes.INT16.read(coordinates));
        }
    }

    @JsonTypeName(value = "INT32")
    protected static class TypedJsonVectorCoordinatesINT32QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesINT32QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesINT32QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesINT32QueryRequestCypherValue(
                    CypherVectorTypes.INT32.read(coordinates));
        }
    }

    @JsonTypeName(value = "INT64")
    protected static class TypedJsonVectorCoordinatesINT64QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesINT64QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesINT64QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesINT64QueryRequestCypherValue(
                    CypherVectorTypes.INT64.read(coordinates));
        }
    }

    @JsonTypeName(value = "FLOAT32")
    protected static class TypedJsonVectorCoordinatesFLOAT32QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesFLOAT32QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesFLOAT32QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesFLOAT32QueryRequestCypherValue(
                    CypherVectorTypes.FLOAT32.read(coordinates));
        }
    }

    @JsonTypeName(value = "FLOAT64")
    protected static class TypedJsonVectorCoordinatesFLOAT64QueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesFLOAT64QueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator
        public static TypedJsonVectorCoordinatesFLOAT64QueryRequestCypherValue of(
                @JsonProperty(value = "coordinates", required = true) String[] coordinates,
                @JsonProperty("coordinatesType") String ignored) {
            return new TypedJsonVectorCoordinatesFLOAT64QueryRequestCypherValue(
                    CypherVectorTypes.FLOAT64.read(coordinates));
        }
    }

    protected static class TypedJsonVectorCoordinatesNotAvailableQueryRequestCypherValue
            extends TypedJsonVectorCoordinatesQueryRequestCypherValue {
        protected TypedJsonVectorCoordinatesNotAvailableQueryRequestCypherValue(Value value) {
            super(value);
        }

        @JsonCreator()
        public static TypedJsonVectorCoordinatesNotAvailableQueryRequestCypherValue of(
                @JsonProperty("coordinatesType") String coordinatesType,
                @JsonProperty("coordinates") String[] coordinates) {
            throw new UnsupportedTypeException(
                    coordinates != null ? String.join(", ", coordinates) : "null",
                    CypherVectorTypes.getTypeNames(),
                    coordinatesType);
        }
    }
}
