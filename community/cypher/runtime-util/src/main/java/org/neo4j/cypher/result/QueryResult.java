/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.result;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;

/**
 * The public Result API of Cypher
 */
public interface QueryResult
{
    String[] fieldNames();

    <E extends Exception> void accept( QueryResultVisitor<E> visitor )
            throws E;

    interface QueryResultVisitor<E extends Exception>
    {
        boolean visit( Record row ) throws E;
    }

    interface Record
    {
        AnyValue[] fields();
    }

    QueryExecutionType executionType();

    QueryStatistics queryStatistics();

    ExecutionPlanDescription executionPlanDescription();

    Iterable<Notification> getNotifications();

    void close();
}
