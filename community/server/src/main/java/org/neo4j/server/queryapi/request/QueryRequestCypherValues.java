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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the Map<String, Object> ingested by the QueryAPI.
 * <p/>
 * This class should be used in fields which expects values to be typed
 * or untyped depending on the {@link org.neo4j.server.queryapi.QueryMimeTypes}.
 */
public final class QueryRequestCypherValues {
    private final Map<String, Object> values;

    private QueryRequestCypherValues(Map<String, Object> values) {
        this.values = values;
    }

    public static QueryRequestCypherValues of(Map<String, QueryRequestCypherValue> values) {
        var valuesMap = new HashMap<String, Object>(values.size());

        for (Map.Entry<String, QueryRequestCypherValue> entry : values.entrySet()) {
            valuesMap.put(entry.getKey(), entry.getValue().value());
        }

        return new QueryRequestCypherValues(valuesMap);
    }

    public Map<String, Object> values() {
        return values;
    }
}
