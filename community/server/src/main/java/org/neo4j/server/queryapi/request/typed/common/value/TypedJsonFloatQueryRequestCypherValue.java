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
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.neo4j.driver.Value;
import org.neo4j.server.queryapi.types.CypherTypes;

@JsonTypeName(value = "Float")
public class TypedJsonFloatQueryRequestCypherValue extends TypedJsonQueryRequestCypherValue {

    protected TypedJsonFloatQueryRequestCypherValue(Value value) {
        super(value);
    }

    @JsonCreator
    public static TypedJsonFloatQueryRequestCypherValue of(
            @JsonProperty(required = true, value = "_value") String value, @JsonProperty("$type") String ignored) {
        return new TypedJsonFloatQueryRequestCypherValue(
                CypherTypes.Float.getReader().apply(value));
    }
}
