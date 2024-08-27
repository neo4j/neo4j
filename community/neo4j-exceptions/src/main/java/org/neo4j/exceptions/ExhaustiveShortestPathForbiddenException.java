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

public class ExhaustiveShortestPathForbiddenException extends CypherExecutionException {
    public static final String ERROR_MSG =
            "Shortest path fallback has been explicitly disabled. That means that no full path enumeration is performed in\n"
                    + "case shortest path algorithms cannot be used. This might happen in case of existential predicates on the path,\n"
                    + "e.g., when searching for the shortest path containing a node with property 'name=Emil'. The problem is that\n"
                    + "graph algorithms work only on universal predicates, e.g., when searching for the shortest where all nodes have\n"
                    + "label 'Person'. In case this is an unexpected error please either disable the runtime error in the Neo4j\n"
                    + "configuration or please improve your query by consulting the Neo4j manual.  In order to avoid planning the\n"
                    + "shortest path fallback a WITH clause can be introduced to separate the MATCH describing the shortest paths and\n"
                    + "the existential predicates on the path; note though that in this case all shortest paths are computed before\n"
                    + "start filtering.";

    @Deprecated
    public ExhaustiveShortestPathForbiddenException() {
        super(ERROR_MSG);
    }

    public ExhaustiveShortestPathForbiddenException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject, ERROR_MSG);
    }
}
