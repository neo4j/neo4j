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
package org.neo4j.fabric.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;

public class LocalExecutionSummary implements Summary {
    private final QueryExecution queryExecution;
    private final QueryStatistics queryStatistics;

    public LocalExecutionSummary(QueryExecution queryExecution, QueryStatistics queryStatistics) {
        this.queryExecution = queryExecution;
        this.queryStatistics = queryStatistics;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription() {
        return queryExecution.executionPlanDescription();
    }

    @Override
    public Collection<Notification> getNotifications() {
        List<Notification> notifications = new ArrayList<>();
        queryExecution.getNotifications().forEach(notifications::add);
        return notifications;
    }

    @Override
    public Collection<GqlStatusObject> getGqlStatusObjects() {
        List<GqlStatusObject> gqlStatusObjects = new ArrayList<>();
        queryExecution.getGqlStatusObjects().forEach(gqlStatusObjects::add);
        return gqlStatusObjects;
    }

    @Override
    public QueryStatistics getQueryStatistics() {
        return queryStatistics;
    }
}
