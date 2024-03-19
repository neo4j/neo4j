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
package org.neo4j.server.httpv2.request;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.ListValue;

public class ListValueDeserializer extends StdDeserializer<ListValue> {

    public ListValueDeserializer() {
        super(ListValue.class);
    }

    @Override
    public ListValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var valueList = new ArrayList<Value>();

        if (p.isExpectedStartArrayToken()) {
            while (p.nextToken() != JsonToken.END_ARRAY) {
                try {
                    valueList.add(p.readValueAs(Value.class));
                } catch (Exception e) {
                    throw new JsonParseException("Unable to read value for list.");
                }
            }
        } else {
            throw new JsonParseException("Expected start array");
        }

        return new ListValue(valueList.toArray(new Value[] {}));
    }
}
