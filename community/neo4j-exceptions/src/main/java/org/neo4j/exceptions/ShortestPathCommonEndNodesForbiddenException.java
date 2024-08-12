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
package org.neo4j.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;

public class ShortestPathCommonEndNodesForbiddenException extends CypherExecutionException {
    private static final String ERROR_MSG =
            "The shortest path algorithm does not work when the start and end nodes are the same. This can happen if you\n"
                    + "perform a shortestPath search after a cartesian product that might have the same start and end nodes for some\n"
                    + "of the rows passed to shortestPath. If you would rather not experience this exception, and can accept the\n"
                    + "possibility of missing results for those rows, disable this in the Neo4j configuration by setting\n"
                    + "`dbms.cypher.forbid_shortestpath_common_nodes` to false. If you cannot accept missing results, and really want the\n"
                    + "shortestPath between two common nodes, then re-write the query using a standard Cypher variable length pattern\n"
                    + "expression followed by ordering by path length and limiting to one result.";

    public ShortestPathCommonEndNodesForbiddenException() {
        super(ERROR_MSG);
    }

    public ShortestPathCommonEndNodesForbiddenException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject, ERROR_MSG);
    }
}
