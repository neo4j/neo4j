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
package org.neo4j.fabric.stream.summary;

import static org.neo4j.graphdb.QueryStatistics.EMPTY;

import java.util.Collection;
import java.util.Collections;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;

public class EmptySummary implements Summary {
    @Override
    public ExecutionPlanDescription executionPlanDescription() {
        return new EmptyExecutionPlanDescription();
    }

    @Override
    public Collection<Notification> getNotifications() {
        return Collections.emptyList();
    }

    @Override
    public Collection<GqlStatusObject> getGqlStatusObjects() {
        return Collections.emptyList();
    }

    @Override
    public QueryStatistics getQueryStatistics() {
        return EMPTY;
    }
}
