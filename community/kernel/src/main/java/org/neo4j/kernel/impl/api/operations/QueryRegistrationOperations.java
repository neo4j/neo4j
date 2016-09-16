/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.operations;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.query.QuerySource;

/**
 * Query execution monitoring operations.
 *
 * @see org.neo4j.kernel.impl.api.OperationsFacade
 */
public interface QueryRegistrationOperations
{
    Stream<ExecutingQuery> executingQueries( KernelStatement statement );

    ExecutingQuery startQueryExecution(
        KernelStatement statement,
        QuerySource descriptor,
        String queryText,
        Map<String, Object> queryParameters
    );

    void registerExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery );
    void unregisterExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery );
}
