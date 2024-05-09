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
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.NullValue;

public class ParameterDeserializer extends StdDeserializer<Map<String, Object>> {

    public ParameterDeserializer() {
        super(TypeFactory.defaultInstance().constructMapType(Map.class, String.class, Object.class));
    }

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var map = new HashMap<String, Object>();

        var t = p.currentToken();
        if (t != JsonToken.START_OBJECT && t != JsonToken.FIELD_NAME) {
            throw new JsonParseException("Unexpected token");
        }

        String keyString;
        if (p.isExpectedStartObjectToken()) {
            keyString = p.nextFieldName();
        } else {
            var token = p.currentToken();
            if (token == JsonToken.END_OBJECT) {
                return map;
            }
            if (token != JsonToken.FIELD_NAME) {
                ctxt.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
            }
            keyString = p.currentName();
        }

        for (; keyString != null; keyString = p.nextFieldName()) {
            JsonToken nextToken = p.nextToken();
            try {
                if (nextToken == JsonToken.VALUE_NULL) {
                    map.put(keyString, NullValue.NULL);
                    continue;
                }
                var value = p.readValueAs(Value.class);
                map.put(keyString, value);
            } catch (Exception e) {
                throw new JsonParseException(format("Unable to read value for field %s", keyString));
            }
        }

        return map;
    }
}
