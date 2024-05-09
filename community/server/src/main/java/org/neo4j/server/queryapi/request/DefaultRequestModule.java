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

import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;

public class DefaultRequestModule extends SimpleModule {
    public DefaultRequestModule() {
        this.addDeserializer(Map.class, new ParameterDeserializer());
        this.addDeserializer(Value.class, new ValueDeserializer());
        this.addDeserializer(ListValue.class, new ListValueDeserializer());
        this.addDeserializer(MapValue.class, new MapValueDeserializer());
    }
}
