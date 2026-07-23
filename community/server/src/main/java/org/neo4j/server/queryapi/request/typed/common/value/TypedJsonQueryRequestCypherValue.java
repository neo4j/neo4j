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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.neo4j.driver.Value;
import org.neo4j.server.queryapi.exception.UnsupportedTypeException;
import org.neo4j.server.queryapi.request.QueryRequestCypherValue;
import org.neo4j.server.queryapi.types.CypherTypes;

/**
 * Describes the base shape of the value on Typed JSON.
 * <p/>
 * This defines the `$type` property as the identifier of types used to select the subtypes.
 * This property still being sent to the subtype since it is needed by default implementation
 * {@link TypedJsonQueryRequestCypherValue.UnsupportedValue} for returning information related
 * to the unsupported type exception.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "$type",
        defaultImpl = TypedJsonQueryRequestCypherValue.UnsupportedValue.class,
        visible = true)
public abstract class TypedJsonQueryRequestCypherValue implements QueryRequestCypherValue {
    private final Value value;

    protected TypedJsonQueryRequestCypherValue(Value value) {
        this.value = value;
    }

    public static TypedJsonQueryRequestCypherValue ofValue(Value value) {
        return new TypedJsonQueryRequestCypherValue(value) {};
    }

    @Override
    public Value value() {
        return this.value;
    }

    protected static class UnsupportedValue extends TypedJsonQueryRequestCypherValue {
        private static final List<String> supportedTypes =
                Stream.of(CypherTypes.values()).map(CypherTypes::name).toList();

        protected UnsupportedValue(Value value) {
            super(value);
        }

        @JsonCreator()
        public static TypedJsonQueryRequestCypherValue.UnsupportedValue of(
                @JsonProperty("$type") String type, @JsonProperty("_value") Object valueString) {
            throw new UnsupportedTypeException(Objects.toString(valueString), supportedTypes, type);
        }
    }
}
