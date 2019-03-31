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
package org.neo4j.server.http.cypher.format.api;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;

/**
 * An event that marks an end of statement's execution output stream.
 */
public class StatementEndEvent implements OutputEvent
{
    private final QueryExecutionType queryExecutionType;
    private final QueryStatistics queryStatistics;
    private final ExecutionPlanDescription executionPlanDescription;
    private final Iterable<Notification> notifications;

    public StatementEndEvent( QueryExecutionType queryExecutionType, QueryStatistics queryStatistics, ExecutionPlanDescription executionPlanDescription,
            Iterable<Notification> notifications )
    {
        this.queryExecutionType = queryExecutionType;
        this.queryStatistics = queryStatistics;
        this.executionPlanDescription = executionPlanDescription;
        this.notifications = notifications;
    }

    @Override
    public Type getType()
    {
        return Type.STATEMENT_END;
    }

    public QueryStatistics getQueryStatistics()
    {
        return queryStatistics;
    }

    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        return executionPlanDescription;
    }

    public Iterable<Notification> getNotifications()
    {
        return notifications;
    }

    public QueryExecutionType getQueryExecutionType()
    {
        return queryExecutionType;
    }
}
