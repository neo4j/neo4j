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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.MapValue;

public class MapValueDeserializer extends StdDeserializer<MapValue> {

    public MapValueDeserializer() {
        super(MapValue.class);
    }

    @Override
    public MapValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Map<String, Value> map = p.readValueAs(new TypeReference<Map<String, Value>>() {});
        return new MapValue(map);
    }
}
