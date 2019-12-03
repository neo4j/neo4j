/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.javacompat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.result.EagerQuerySubscription;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.storable.Values;

/**
 * A query execution that streams from a given already materialized eager result.
 */
class EagerQueryExecution extends EagerQuerySubscription implements QueryExecution
{
    private final QueryExecution innerExecution;
    private final List<String> fieldNames;
    private final QueryStatistics queryStatistics;
    private final List<Map<String,Object>> queryResult;

    EagerQueryExecution( QuerySubscriber subscriber, QueryExecution innerExecution, QueryStatistics queryStatistics, List<Map<String, Object>> queryResult )
    {
        super( subscriber );
        this.innerExecution = innerExecution;
        this.fieldNames = Arrays.asList( innerExecution.fieldNames() );
        this.queryStatistics = queryStatistics;
        this.queryResult = queryResult;

        try
        {
            subscriber.onResult( fieldNames.size() );
        }
        catch ( Exception e )
        {
            error = e;
        }
    }

    @Override
    public QueryExecutionType executionType()
    {
        return innerExecution.executionType();
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        return innerExecution.executionPlanDescription();
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return innerExecution.getNotifications();
    }

    @Override
    public String[] fieldNames()
    {
        return innerExecution.fieldNames();
    }

    @Override
    protected QueryStatistics queryStatistics()
    {
        return queryStatistics;
    }

    @Override
    protected int resultSize()
    {
        return queryResult.size();
    }

    @Override
    protected void materializeIfNecessary()
    {
        // Result is already materialized
    }

    @Override
    protected void streamRecordToSubscriber( int servedRecords ) throws Exception
    {
        Map<String,Object> currentRow = queryResult.get( servedRecords );
        int fieldNamesSize = fieldNames.size();
        for ( int i = 0; i < fieldNamesSize; i++ )
        {
            subscriber.onField( i, Values.of( currentRow.get( fieldNames.get( i ) ) ) );
        }
    }

    @Override
    public boolean isVisitable()
    {
        return false;
    }

    @Override
    public <VisitationException extends Exception> QueryStatistics accept( Result.ResultVisitor<VisitationException> visitor )
    {
        throw new IllegalStateException( "EagerQueryExecution is not visitable" );
    }
}
