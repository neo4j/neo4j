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

public record QueryRequest(
        String statement,
        Map<String, Object> parameters,
        boolean includeCounters,
        AccessMode accessMode,
        int maxExecutionTime,
        List<String> bookmarks,
        String impersonatedUser) {

    public QueryRequest(String statement, List<String> bookmarks) {
        this(statement, Map.of(), false, AccessMode.WRITE, 0, bookmarks, null);
    }

    public QueryRequest(String statement) {
        this(statement, Map.of(), false, AccessMode.WRITE, 0, List.of(), null);
    }
}
