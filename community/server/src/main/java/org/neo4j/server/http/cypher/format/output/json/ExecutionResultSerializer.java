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
package org.neo4j.server.http.cypher.format.output.json;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.server.http.cypher.TransactionStateChecker;
import org.neo4j.server.http.cypher.TransitionalPeriodTransactionMessContainer;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.common.Neo4jJsonCodec;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;

import static org.neo4j.server.http.cypher.format.api.TransactionNotificationState.OPEN;
import static org.neo4j.server.rest.domain.JsonHelper.writeValue;

/**
 * A stateful serializer that serializes event stream produced  by {@link OutputEventSource} into JSON.
 * The serialization methods are expected to be invoked in order which corresponds to the legal ordering of the event stream events
 * as described in {@link OutputEvent}.
 */
class ExecutionResultSerializer
{

    private State currentState = State.EMPTY;

    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM );
    private final JsonGenerator out;
    private final URI baseUri;
    private final TransitionalPeriodTransactionMessContainer container;
    private final List<Notification> notifications = new ArrayList<>();
    private final List<FailureEvent> errors = new ArrayList<>();
    private final OutputStream output;

    private ResultDataContentWriter writer;
    private InputStatement inputStatement;

    ExecutionResultSerializer( OutputStream output, URI baseUri, TransitionalPeriodTransactionMessContainer container )
    {
        this.baseUri = baseUri;
        this.container = container;
        this.output = output;
        JSON_FACTORY.setCodec( new Neo4jJsonCodec( container ) );
        JsonGenerator generator;
        try
        {
            generator = JSON_FACTORY.createJsonGenerator( output );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Failed to create JSON generator", e );
        }
        this.out = generator;
    }

    void writeStatementStart( StatementStartEvent statementStartEvent, InputStatement inputStatement )
    {
        this.inputStatement = inputStatement;
        this.writer = configureWriters( inputStatement.resultDataContents() );
        try
        {
            ensureResultsFieldOpen();
            out.writeStartObject();
            Iterable<String> columns = statementStartEvent.getColumns();
            writeColumns( columns );
            out.writeArrayFieldStart( "data" );
            currentState = State.STATEMENT_OPEN;
        }
        catch ( JsonGenerationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the connection", e );
        }
    }

    void writeRecord( RecordEvent recordEvent )
    {
        try
        {
            out.writeStartObject();

            try ( TransactionStateChecker txStateChecker = TransactionStateChecker.create( container ) )
            {
                writer.write( out, recordEvent, txStateChecker, container.getDb() );
            }
            finally
            {
                out.writeEndObject();
            }
            flush();
        }
        catch ( JsonGenerationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the connection", e );
        }
    }

    private ResultDataContentWriter configureWriters( List<ResultDataContent> specifiers )
    {
        if ( specifiers == null || specifiers.size() == 0 )
        {
            return ResultDataContent.row.writer( baseUri ); // default
        }
        if ( specifiers.size() == 1 )
        {
            return specifiers.get( 0 ).writer( baseUri );
        }
        ResultDataContentWriter[] writers = new ResultDataContentWriter[specifiers.size()];
        for ( int i = 0; i < specifiers.size(); i++ )
        {
            writers[i] = specifiers.get( i ).writer( baseUri );
        }
        return new AggregatingWriter( writers );
    }

    void writeStatementEnd( StatementEndEvent statementEndEvent )
    {
        try
        {
            out.writeEndArray();
            if ( inputStatement.includeStats() )
            {
                writeStats( statementEndEvent.getQueryStatistics() );
            }
            if ( statementEndEvent.getQueryExecutionType().requestedExecutionPlanDescription() )
            {
                writeRootPlanDescription( statementEndEvent.getExecutionPlanDescription() );
            }

            out.writeEndObject(); // </result>
            currentState = State.RESULTS_OPEN;

            statementEndEvent.getNotifications().forEach( notifications::add );
        }
        catch ( JsonGenerationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the connection", e );
        }
    }

    void writeTransactionInfo( TransactionInfoEvent transactionInfoEvent )
    {
        try
        {
            ensureDocumentOpen();
            ensureResultsFieldClosed();
            writeNotifications( notifications );
            writeErrors();
            if ( transactionInfoEvent.getCommitUri() != null )
            {
                out.writeStringField( "commit", transactionInfoEvent.getCommitUri().toString() );
            }
            if ( transactionInfoEvent.getNotification() == OPEN )
            {
                out.writeObjectFieldStart( "transaction" );
                if ( transactionInfoEvent.getExpirationTimestamp() >= 0 )
                {
                    String expires = Instant.ofEpochMilli( transactionInfoEvent.getExpirationTimestamp() )
                            .atZone( ZoneId.of( "GMT" ) )
                            .format( DateTimeFormatter.RFC_1123_DATE_TIME );
                    out.writeStringField( "expires", expires );
                }
                out.writeEndObject();
            }
            out.writeEndObject();
            flush();
        }
        catch ( JsonGenerationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the connection", e );
        }
    }

    void writeFailure( FailureEvent failureEvent )
    {
        try
        {
            errors.add( failureEvent );
            ensureStatementFieldClosed();
        }
        catch ( JsonGenerationException e )
        {
            throw new IllegalStateException( e );
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the connection", e );
        }
    }

    private void writeNotifications( Iterable<Notification> notifications ) throws IOException
    {
        //don't add anything if notifications are empty
        if ( !notifications.iterator().hasNext() )
        {
            return;
        }

        try
        {
            ensureResultsFieldClosed();

            out.writeArrayFieldStart( "notifications" );
            try
            {
                for ( Notification notification : notifications )
                {
                    out.writeStartObject();
                    try
                    {
                        out.writeStringField( "code", notification.getCode() );
                        out.writeStringField( "severity", notification.getSeverity().toString() );
                        out.writeStringField( "title", notification.getTitle() );
                        out.writeStringField( "description", notification.getDescription() );
                        writePosition( notification.getPosition() );
                    }
                    finally
                    {
                        out.writeEndObject();
                    }
                }
            }
            finally
            {
                out.writeEndArray();
            }
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the response stream", e );
        }
    }

    private void writePosition( InputPosition position ) throws IOException
    {
        //do not add position if empty
        if ( position == InputPosition.empty )
        {
            return;
        }

        out.writeObjectFieldStart( "position" );
        try
        {
            out.writeNumberField( "offset", position.getOffset() );
            out.writeNumberField( "line", position.getLine() );
            out.writeNumberField( "column", position.getColumn() );
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeStats( QueryStatistics stats ) throws IOException
    {
        out.writeObjectFieldStart( "stats" );
        try
        {
            out.writeBooleanField( "contains_updates", stats.containsUpdates() );
            out.writeNumberField( "nodes_created", stats.getNodesCreated() );
            out.writeNumberField( "nodes_deleted", stats.getNodesDeleted() );
            out.writeNumberField( "properties_set", stats.getPropertiesSet() );
            out.writeNumberField( "relationships_created", stats.getRelationshipsCreated() );
            out.writeNumberField( "relationship_deleted", stats.getRelationshipsDeleted() );
            out.writeNumberField( "labels_added", stats.getLabelsAdded() );
            out.writeNumberField( "labels_removed", stats.getLabelsRemoved() );
            out.writeNumberField( "indexes_added", stats.getIndexesAdded() );
            out.writeNumberField( "indexes_removed", stats.getIndexesRemoved() );
            out.writeNumberField( "constraints_added", stats.getConstraintsAdded() );
            out.writeNumberField( "constraints_removed", stats.getConstraintsRemoved() );
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeRootPlanDescription( ExecutionPlanDescription planDescription ) throws IOException
    {
        out.writeObjectFieldStart( "plan" );
        try
        {
            out.writeObjectFieldStart( "root" );
            try
            {
                writePlanDescriptionObjectBody( planDescription );
            }
            finally
            {
                out.writeEndObject();
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writePlanDescriptionObjectBody( ExecutionPlanDescription planDescription ) throws IOException
    {
        out.writeStringField( "operatorType", planDescription.getName() );
        writePlanArgs( planDescription );
        writePlanIdentifiers( planDescription );

        List<ExecutionPlanDescription> children = planDescription.getChildren();
        out.writeArrayFieldStart( "children" );
        try
        {
            for ( ExecutionPlanDescription child : children )
            {
                out.writeStartObject();
                try
                {
                    writePlanDescriptionObjectBody( child );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writePlanArgs( ExecutionPlanDescription planDescription ) throws IOException
    {
        for ( Map.Entry<String,Object> entry : planDescription.getArguments().entrySet() )
        {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            out.writeFieldName( fieldName );
            writeValue( out, fieldValue );
        }
    }

    private void writePlanIdentifiers( ExecutionPlanDescription planDescription ) throws IOException
    {
        out.writeArrayFieldStart( "identifiers" );
        for ( String id : planDescription.getIdentifiers() )
        {
            out.writeString( id );
        }
        out.writeEndArray();
    }

    private void writeErrors()
    {
        try
        {
            ensureDocumentOpen();
            out.writeArrayFieldStart( "errors" );
            try
            {
                for ( FailureEvent error : errors )
                {
                    try
                    {
                        out.writeStartObject();
                        out.writeObjectField( "code", error.getStatus().code().serialize() );
                        out.writeObjectField( "message", error.getMessage() );
                    }
                    finally
                    {
                        out.writeEndObject();
                    }
                }
            }
            finally
            {
                out.writeEndArray();
                currentState = State.ERRORS_WRITTEN;
            }
        }
        catch ( IOException e )
        {
            throw new ConnectionException( "Failed to write to the response stream", e );
        }
    }

    private void ensureDocumentOpen() throws IOException
    {
        if ( currentState == State.EMPTY )
        {
            out.writeStartObject();
            currentState = State.DOCUMENT_OPEN;
        }
    }

    private void ensureResultsFieldOpen() throws IOException
    {
        ensureDocumentOpen();
        if ( currentState == State.DOCUMENT_OPEN )
        {
            out.writeArrayFieldStart( "results" );
            currentState = State.RESULTS_OPEN;
        }
    }

    private void ensureResultsFieldClosed() throws IOException
    {
        ensureResultsFieldOpen();
        if ( currentState == State.RESULTS_OPEN )
        {
            out.writeEndArray();
            currentState = State.RESULTS_CLOSED;
        }
    }

    private void ensureStatementFieldClosed() throws IOException
    {
        if ( currentState == State.STATEMENT_OPEN )
        {
            out.writeEndArray();
            out.writeEndObject();
            currentState = State.RESULTS_OPEN;
        }
    }

    private void writeColumns( Iterable<String> columns ) throws IOException
    {
        try
        {
            out.writeArrayFieldStart( "columns" );
            for ( String key : columns )
            {
                out.writeString( key );
            }
        }
        finally
        {
            out.writeEndArray(); // </columns>
        }
    }

    private void flush() throws IOException
    {
        out.flush();
        output.flush();
    }

    private enum State
    {
        EMPTY,
        DOCUMENT_OPEN,
        RESULTS_OPEN,
        STATEMENT_OPEN,
        RESULTS_CLOSED,
        ERRORS_WRITTEN
    }
}
