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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;

class OutputEventStreamImpl implements OutputEventSource, OutputEventStream
{

    private final Consumer<OutputEventStream> startListener;
    private final Map<String,Object> parameters;
    private final TransactionHandle transactionHandle;
    private final TransactionUriScheme uriInfo;
    private Consumer<OutputEvent> eventListener;

    OutputEventStreamImpl( Map<String,Object> parameters, TransactionHandle transactionHandle, TransactionUriScheme uriInfo,
            Consumer<OutputEventStream> startListener )
    {
        this.parameters = parameters;
        this.transactionHandle = transactionHandle;
        this.startListener = startListener;
        this.uriInfo = uriInfo;
    }

    @Override
    public void produceEvents( Consumer<OutputEvent> eventListener )
    {
        this.eventListener = eventListener;
        startListener.accept( this );
    }

    @Override
    public Map<String,Object> getParameters()
    {
        return parameters;
    }

    @Override
    public void writeStatementStart( Statement statement, List<String> columns )
    {
        notifyListener( new StatementStartEvent( statement, columns ) );
    }

    @Override
    public void writeStatementEnd( QueryExecutionType queryExecutionType, QueryStatistics queryStatistics, ExecutionPlanDescription executionPlanDescription,
            Iterable<Notification> notifications )
    {
        notifyListener( new StatementEndEvent( queryExecutionType, queryStatistics, executionPlanDescription, notifications ) );
    }

    @Override
    public void writeRecord( List<String> columns, Function<String,Object> valueSupplier )
    {
        notifyListener( new RecordEvent( columns, valueSupplier ) );
    }

    @Override
    public void writeTransactionInfo( TransactionNotificationState notification, URI commitUri, long expirationTimestamp )
    {
        notifyListener( new TransactionInfoEvent( notification, commitUri, expirationTimestamp ) );
    }

    @Override
    public void writeFailure( Status status, String message )
    {
        notifyListener( new FailureEvent( status, message ) );
    }

    private void notifyListener( OutputEvent event )
    {
        eventListener.accept( event );
    }

    @Override
    public TransactionUriScheme getUriInfo()
    {
        return uriInfo;
    }

    @Override
    public TransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }
}
