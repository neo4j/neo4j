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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Optional;

public class QueryRequest {
    private final String statement;
    private final QueryRequestCypherValues parameters;
    private final boolean includeCounters;

    @JsonCreator
    public QueryRequest(
            @JsonProperty("statement") String statement,
            @JsonProperty("parameters") QueryRequestCypherValues parameters,
            @JsonProperty("includeCounters") boolean includeCounters) {
        this.statement = statement;
        this.parameters = parameters;
        this.includeCounters = includeCounters;
        ;
    }

    public QueryRequest(String statement) {
        this(statement, null, false);
    }

    public QueryRequest() {
        this(null, null, false);
    }

    public String statement() {
        return statement;
    }

    public QueryRequestCypherValues parameters() {
        return parameters;
    }

    public boolean includeCounters() {
        return includeCounters;
    }

    public Optional<Map<String, Object>> maybeParameters() {
        return Optional.ofNullable(parameters).map(QueryRequestCypherValues::values);
    }
}
