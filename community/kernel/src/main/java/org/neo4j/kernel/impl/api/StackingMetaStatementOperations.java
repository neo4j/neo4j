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
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.impl.api.operations.MetaStatementOperations;
import org.neo4j.kernel.impl.util.MonotonicCounter;

public class StackingMetaStatementOperations implements MetaStatementOperations
{
    public static final MonotonicCounter LAST_QUERY_ID = MonotonicCounter.newAtomicMonotonicCounter();

    private final MonotonicCounter lastQueryId;

    public StackingMetaStatementOperations( MonotonicCounter lastQueryId )
    {
        this.lastQueryId = lastQueryId;
    }

    @Override
    public Stream<ExecutingQuery> executingQueries( KernelStatement statement)
    {
        return statement.executingQueries();
    }

    @Override
    public ExecutingQuery startQueryExecution(
            KernelStatement statement, String queryText, Map<String,Object> queryParameters )
    {
        ExecutingQuery executingQuery = createExecutingQuery( statement, queryText, queryParameters );
        statement.startQueryExecution( executingQuery );
        return executingQuery;
    }

    @Override
    public void stopQueryExecution( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.stopQueryExecution( executingQuery );
    }

    private ExecutingQuery createExecutingQuery(
            KernelStatement statement,
            String queryText,
            Map<String,Object> queryParameters )
    {
        long queryId = lastQueryId.incrementAndGet();
        return new ExecutingQuery( queryId, statement.authSubjectName(), queryText, queryParameters );
    }

}

