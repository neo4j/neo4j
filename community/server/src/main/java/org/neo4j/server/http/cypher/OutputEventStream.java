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
package org.neo4j.server.http.cypher;

import java.net.URI;
import java.util.List;
import java.util.function.Function;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;

/**
 * An output stream that can be used to create and send {@link OutputEvent}s.
 */
interface OutputEventStream
{
    void writeStatementStart( Statement statement, List<String> columns );

    void writeStatementEnd( QueryExecutionType queryExecutionType, QueryStatistics queryStatistics, ExecutionPlanDescription executionPlanDescription,
            Iterable<Notification> notifications );

    void writeRecord( List<String> columns, Function<String,Object> valueSupplier );

    void writeTransactionInfo( TransactionNotificationState notification, URI commitUri, long expirationTimestamp );

    void writeFailure( Status status, String message );
}
