/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class StatementQueryRegistry implements QueryRegistry
{
    private final KernelStatement statement;
    private final ExecutingQueryFactory factory;

    StatementQueryRegistry( KernelStatement statement, SystemNanoClock clock, AtomicReference<CpuClock> cpuClockRef, Config config )
    {
        this.statement = statement;
        this.factory = new ExecutingQueryFactory( clock, cpuClockRef, config );
    }

    @Override
    public Optional<ExecutingQuery> executingQuery()
    {
        return statement.executingQuery();
    }

    @Override
    public void registerExecutingQuery( ExecutingQuery executingQuery )
    {
        statement.startQueryExecution( executingQuery );
    }

    @Override
    public ExecutingQuery startQueryExecution( String queryText, MapValue queryParameters )
    {
        ExecutingQuery executingQuery = factory.createForStatement( statement, queryText, queryParameters );
        registerExecutingQuery( executingQuery );
        return executingQuery;
    }

    @Override
    public void startQueryExecution( ExecutingQuery executingQuery )
    {
        factory.bindToStatement( executingQuery, statement );
        registerExecutingQuery( executingQuery );
    }

    @Override
    public void unregisterExecutingQuery( ExecutingQuery executingQuery )
    {
        statement.stopQueryExecution( executingQuery );
    }
}

