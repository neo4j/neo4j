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

import java.time.Clock;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.query.QuerySource;
import org.neo4j.kernel.impl.util.MonotonicCounter;

public class StackingQueryRegistrationOperations implements QueryRegistrationOperations
{

    private final MonotonicCounter lastQueryId = MonotonicCounter.newAtomicMonotonicCounter();
    private final Clock clock;

    public StackingQueryRegistrationOperations( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    public Stream<ExecutingQuery> executingQueries( KernelStatement statement)
    {
        return statement.executingQueryList().queries();
    }

    @Override
    public void registerExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.startQueryExecution( executingQuery );
    }

    @Override
    public ExecutingQuery startQueryExecution(
        KernelStatement statement,
        QuerySource querySource,
        String queryText,
        Map<String,Object> queryParameters
    )
    {
        long queryId = lastQueryId.incrementAndGet();
        ExecutingQuery executingQuery =
                new ExecutingQuery( queryId, querySource, statement.username(), queryText, queryParameters,
                        clock.millis(), statement.getTransaction().getMetaData() );
        registerExecutingQuery( statement, executingQuery );
        return executingQuery;
    }

    @Override
    public void unregisterExecutingQuery( KernelStatement statement, ExecutingQuery executingQuery )
    {
        statement.stopQueryExecution( executingQuery );
    }

}

