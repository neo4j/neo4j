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
package org.neo4j.fleetmanagement.queries.model;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.kernel.api.query.ExecutingQuery;

public class AggregatedQueriesTimeSlice {
    public static final int META_JSON_SIZE = 400; // Approx size of QueryAggregationMeta marshaled into json

    private final Map<UniqueKey, QueryAggregationMeta> aggregations = new HashMap<>();
    private final long creationTime;
    private int cumulativeQueryTextSize;

    public AggregatedQueriesTimeSlice() {
        creationTime = System.currentTimeMillis();
        cumulativeQueryTextSize = 0;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void add(ExecutingQuery query, ErrorGqlStatusObject errorGqlStatusObject) {
        var obfuscatedText = query.fullyObfuscatedQueryText();
        var key = new UniqueKey(obfuscatedText, errorGqlStatusObject, query.queryLanguage());
        aggregations
                .computeIfAbsent(key, k -> {
                    cumulativeQueryTextSize += obfuscatedText.length() + META_JSON_SIZE;
                    return new QueryAggregationMeta();
                })
                .addFromExecutingQuery(query);
    }

    public int size() {
        return aggregations.size();
    }

    public int cumulativeQueryTextSize() {
        return cumulativeQueryTextSize;
    }

    public Map<UniqueKey, QueryAggregationMeta> getAggregations() {
        return aggregations;
    }
}
