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
package org.neo4j.server.queryapi.request;

import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.server.queryapi.response.format.CypherTypes;
import org.neo4j.server.queryapi.response.format.Fieldnames;

public class ValueDeserializer extends StdDeserializer<Value> {

    public ValueDeserializer() {
        super(Value.class);
    }

    @Override
    public Value deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        var name = p.nextFieldName();

        if (name.equals(Fieldnames.CYPHER_TYPE)) {
            var typeString = p.nextTextValue();
            var nextToken = p.nextToken();

            if (nextToken.equals(JsonToken.FIELD_NAME) && p.currentName().equals(Fieldnames.CYPHER_VALUE)) {
                p.nextToken();
                if (typeString.equals(CypherTypes.List.name())) {
                    var listValue = p.readValueAs(ListValue.class);
                    p.nextToken();
                    return listValue;
                } else if (typeString.equals(CypherTypes.Map.name())) {
                    var mapValue = p.readValueAs(MapValue.class);
                    p.nextToken();
                    return mapValue;
                } else if (typeString.equals(CypherTypes.Boolean.name())) {
                    var boolValue = BooleanValue.fromBoolean(p.getBooleanValue());
                    p.nextToken();
                    return boolValue;
                } else if (typeString.equals(CypherTypes.Null.name())) {
                    if (p.currentToken().equals(JsonToken.VALUE_NULL)) {
                        p.nextToken();
                        return NullValue.NULL;
                    } else {
                        throw new JsonParseException("Expected 'null' value");
                    }

                } else {
                    var parser = CypherTypes.valueOf(typeString).getReader();

                    if (parser != null) {
                        var stringValue = p.getValueAsString();
                        p.nextToken();
                        return parser.apply(stringValue);
                    } else {
                        throw new JsonParseException(format("Type %s is not a valid parameter type.", typeString));
                    }
                }
            } else {
                throw new JsonParseException(format("Expecting field %s", Fieldnames.CYPHER_VALUE));
            }
        } else {
            throw new JsonParseException("Expected a typed value.");
        }
    }
}
