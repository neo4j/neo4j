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

import java.util.List;
import java.util.Map;
import java.util.Optional;

public record QueryRequest(
        String statement,
        QueryRequestCypherValues parameters,
        boolean includeCounters,
        AccessMode accessMode,
        int maxExecutionTime,
        List<String> bookmarks,
        String impersonatedUser,
        String txType,
        QueryRequestCypherValues txMetadata) {

    public QueryRequest(String statement, List<String> bookmarks) {
        this(statement, null, false, AccessMode.WRITE, 0, bookmarks, null, null, null);
    }

    public QueryRequest(String statement) {
        this(statement, null, false, AccessMode.WRITE, 0, List.of(), null, null, null);
    }

    public QueryRequest() {
        this(null, null, false, AccessMode.WRITE, 0, List.of(), null, null, null);
    }

    public Optional<Map<String, Object>> maybeParameters() {
        return Optional.ofNullable(parameters).map(QueryRequestCypherValues::values);
    }

    public Optional<Map<String, Object>> maybeTxMetadata() {
        return Optional.ofNullable(txMetadata).map(QueryRequestCypherValues::values);
    }
}
