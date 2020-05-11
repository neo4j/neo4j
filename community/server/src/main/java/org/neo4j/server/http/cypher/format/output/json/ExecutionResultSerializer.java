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
package org.neo4j.server.http.cypher.format.output.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.ObjectCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
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
import org.neo4j.server.http.cypher.TransactionHandle;
import org.neo4j.server.http.cypher.TransactionStateChecker;
import org.neo4j.server.http.cypher.format.api.ConnectionException;
import org.neo4j.server.http.cypher.format.api.FailureEvent;
import org.neo4j.server.http.cypher.format.api.OutputEvent;
import org.neo4j.server.http.cypher.format.api.OutputEventSource;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.http.cypher.format.api.StatementEndEvent;
import org.neo4j.server.http.cypher.format.api.StatementStartEvent;
import org.neo4j.server.http.cypher.format.api.TransactionInfoEvent;
import org.neo4j.server.http.cypher.format.input.json.InputStatement;
import org.neo4j.server.http.cypher.format.input.json.JsonMessageBodyReader;

import static org.neo4j.server.http.cypher.format.api.TransactionNotificationState.OPEN;
import static org.neo4j.server.rest.domain.JsonHelper.writeValue;

/**
 * A stateful serializer that serializes event stream produced  by {@link OutputEventSource} into JSON. The serialization methods are expected to be invoked in
 * order which corresponds to the legal ordering of the event stream events as described in {@link OutputEvent}.
 */
class ExecutionResultSerializer
{

    private State currentState = State.EMPTY;

    private final JsonGenerator jsonGenerator;
    private final URI baseUri;
    private final TransactionHandle transactionHandle;
    private final List<Notification> notifications = new ArrayList<>();
    private final List<FailureEvent> errors = new ArrayList<>();
    private final OutputStream output;
    /**
     * THe original parameters from the {@link org.neo4j.server.http.cypher.format.api.OutputEventSource}.
     */
    private final Map<String,Object> parameters;

    private ResultDataContentWriter writer;
    private InputStatement inputStatement;

    // The idea behind passing in the JSON Factory as well as the codec to use is as follows:
    // This stateful serializer alone shall be responsible for creating a stateful JSON generator
    // from the JSON Factory in such a way that the state management does not leak into other classes.
    // For example, if we would rely on the JAX-RS facing JSON Body writer, we would have no meaningful
    // test whether the state handling works or not.
    ExecutionResultSerializer( TransactionHandle transactionHandle, Map<String,Object> parameters, URI baseUri,
                               Class<? extends ObjectCodec> classOfCodec, JsonFactory jsonFactory, OutputStream output )
    {
        this.parameters = parameters;
        this.baseUri = baseUri;
        this.transactionHandle = transactionHandle;
        this.output = output;

        ObjectCodec codec = instantiateCodec( transactionHandle, classOfCodec );
        this.jsonGenerator = createGenerator( jsonFactory, codec, output );
    }

    public final void handleEvent( OutputEvent event )
    {
        switch ( event.getType() )
        {
        case STATEMENT_START:
            StatementStartEvent statementStartEvent = (StatementStartEvent) event;
            InputStatement inputStatement = JsonMessageBodyReader.getInputStatement( parameters, statementStartEvent.getStatement() );
            writeStatementStart( statementStartEvent, inputStatement );
            break;
        case RECORD:
            writeRecord( (RecordEvent) event );
            break;
        case STATEMENT_END:
            StatementEndEvent statementEndEvent = (StatementEndEvent) event;
            writeStatementEnd( statementEndEvent );
            break;
        case FAILURE:
            FailureEvent failureEvent = (FailureEvent) event;
            writeFailure( failureEvent );
            break;
        case TRANSACTION_INFO:
            TransactionInfoEvent transactionInfoEvent = (TransactionInfoEvent) event;
            writeTransactionInfo( transactionInfoEvent );
            break;
        default:
            throw new IllegalStateException( "Unsupported event encountered:" + event.getType() );
        }
    }

    private static ObjectCodec instantiateCodec( TransactionHandle transactionHandle, Class<? extends ObjectCodec> classOfCodec )
    {
        try
        {
            var ctor = classOfCodec.getConstructor( TransactionHandle.class );
            return ctor.newInstance( transactionHandle );
        }
        catch ( NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e )
        {
            throw new IllegalStateException( "Failed to create result mapper", e );
        }
    }

    private static JsonGenerator createGenerator( JsonFactory jsonFactory, ObjectCodec codec, OutputStream output )
    {
        try
        {
            return jsonFactory.copy().setCodec( codec ).createGenerator( output );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Failed to create JSON generator", e );
        }
    }

    void writeStatementStart( StatementStartEvent statementStartEvent, InputStatement inputStatement )
    {
        this.inputStatement = inputStatement;
        this.writer = configureWriters( inputStatement.resultDataContents() );
        try
        {
            ensureResultsFieldOpen();
            jsonGenerator.writeStartObject();
            Iterable<String> columns = statementStartEvent.getColumns();
            writeColumns( columns );
            jsonGenerator.writeArrayFieldStart( "data" );
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
            TransactionStateChecker txStateChecker = TransactionStateChecker.create( transactionHandle.getContext() );

            jsonGenerator.writeStartObject();
            try
            {
                writer.write( jsonGenerator, recordEvent, txStateChecker );
            }
            finally
            {
                jsonGenerator.writeEndObject();
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
        if ( specifiers == null || specifiers.isEmpty() )
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
            jsonGenerator.writeEndArray();
            if ( inputStatement.includeStats() )
            {
                writeStats( statementEndEvent.getQueryStatistics() );
            }
            if ( statementEndEvent.getQueryExecutionType().requestedExecutionPlanDescription() )
            {
                writeRootPlanDescription( statementEndEvent.getExecutionPlanDescription() );
            }

            jsonGenerator.writeEndObject(); // </result>
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
                jsonGenerator.writeStringField( "commit", transactionInfoEvent.getCommitUri().toString() );
            }
            if ( transactionInfoEvent.getNotification() == OPEN )
            {
                jsonGenerator.writeObjectFieldStart( "transaction" );
                if ( transactionInfoEvent.getExpirationTimestamp() >= 0 )
                {
                    String expires = Instant.ofEpochMilli( transactionInfoEvent.getExpirationTimestamp() )
                                            .atZone( ZoneId.of( "GMT" ) )
                                            .format( DateTimeFormatter.RFC_1123_DATE_TIME );
                    jsonGenerator.writeStringField( "expires", expires );
                }
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndObject();
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

    private void writeNotifications( Iterable<Notification> notifications )
    {
        //don't add anything if notifications are empty
        if ( !notifications.iterator().hasNext() )
        {
            return;
        }

        try
        {
            ensureResultsFieldClosed();

            jsonGenerator.writeArrayFieldStart( "notifications" );
            try
            {
                for ( Notification notification : notifications )
                {
                    jsonGenerator.writeStartObject();
                    try
                    {
                        jsonGenerator.writeStringField( "code", notification.getCode() );
                        jsonGenerator.writeStringField( "severity", notification.getSeverity().toString() );
                        jsonGenerator.writeStringField( "title", notification.getTitle() );
                        jsonGenerator.writeStringField( "description", notification.getDescription() );
                        writePosition( notification.getPosition() );
                    }
                    finally
                    {
                        jsonGenerator.writeEndObject();
                    }
                }
            }
            finally
            {
                jsonGenerator.writeEndArray();
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

        jsonGenerator.writeObjectFieldStart( "position" );
        try
        {
            jsonGenerator.writeNumberField( "offset", position.getOffset() );
            jsonGenerator.writeNumberField( "line", position.getLine() );
            jsonGenerator.writeNumberField( "column", position.getColumn() );
        }
        finally
        {
            jsonGenerator.writeEndObject();
        }
    }

    private void writeStats( QueryStatistics stats ) throws IOException
    {
        jsonGenerator.writeObjectFieldStart( "stats" );
        try
        {
            jsonGenerator.writeBooleanField( "contains_updates", stats.containsUpdates() );
            jsonGenerator.writeNumberField( "nodes_created", stats.getNodesCreated() );
            jsonGenerator.writeNumberField( "nodes_deleted", stats.getNodesDeleted() );
            jsonGenerator.writeNumberField( "properties_set", stats.getPropertiesSet() );
            jsonGenerator.writeNumberField( "relationships_created", stats.getRelationshipsCreated() );
            jsonGenerator.writeNumberField( "relationship_deleted", stats.getRelationshipsDeleted() );
            jsonGenerator.writeNumberField( "labels_added", stats.getLabelsAdded() );
            jsonGenerator.writeNumberField( "labels_removed", stats.getLabelsRemoved() );
            jsonGenerator.writeNumberField( "indexes_added", stats.getIndexesAdded() );
            jsonGenerator.writeNumberField( "indexes_removed", stats.getIndexesRemoved() );
            jsonGenerator.writeNumberField( "constraints_added", stats.getConstraintsAdded() );
            jsonGenerator.writeNumberField( "constraints_removed", stats.getConstraintsRemoved() );
            jsonGenerator.writeBooleanField( "contains_system_updates", stats.containsSystemUpdates() );
            jsonGenerator.writeNumberField( "system_updates", stats.getSystemUpdates() );
        }
        finally
        {
            jsonGenerator.writeEndObject();
        }
    }

    private void writeRootPlanDescription( ExecutionPlanDescription planDescription ) throws IOException
    {
        jsonGenerator.writeObjectFieldStart( "plan" );
        try
        {
            jsonGenerator.writeObjectFieldStart( "root" );
            try
            {
                writePlanDescriptionObjectBody( planDescription );
            }
            finally
            {
                jsonGenerator.writeEndObject();
            }
        }
        finally
        {
            jsonGenerator.writeEndObject();
        }
    }

    private void writePlanDescriptionObjectBody( ExecutionPlanDescription planDescription ) throws IOException
    {
        jsonGenerator.writeStringField( "operatorType", planDescription.getName() );
        writePlanArgs( planDescription );
        writePlanIdentifiers( planDescription );

        List<ExecutionPlanDescription> children = planDescription.getChildren();
        jsonGenerator.writeArrayFieldStart( "children" );
        try
        {
            for ( ExecutionPlanDescription child : children )
            {
                jsonGenerator.writeStartObject();
                try
                {
                    writePlanDescriptionObjectBody( child );
                }
                finally
                {
                    jsonGenerator.writeEndObject();
                }
            }
        }
        finally
        {
            jsonGenerator.writeEndArray();
        }
    }

    private void writePlanArgs( ExecutionPlanDescription planDescription ) throws IOException
    {
        for ( Map.Entry<String,Object> entry : planDescription.getArguments().entrySet() )
        {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            jsonGenerator.writeFieldName( fieldName );
            writeValue( jsonGenerator, fieldValue );
        }
    }

    private void writePlanIdentifiers( ExecutionPlanDescription planDescription ) throws IOException
    {
        jsonGenerator.writeArrayFieldStart( "identifiers" );
        for ( String id : planDescription.getIdentifiers() )
        {
            jsonGenerator.writeString( id );
        }
        jsonGenerator.writeEndArray();
    }

    private void writeErrors()
    {
        try
        {
            ensureDocumentOpen();
            jsonGenerator.writeArrayFieldStart( "errors" );
            try
            {
                for ( FailureEvent error : errors )
                {
                    try
                    {
                        jsonGenerator.writeStartObject();
                        jsonGenerator.writeObjectField( "code", error.getStatus().code().serialize() );
                        jsonGenerator.writeObjectField( "message", error.getMessage() );
                    }
                    finally
                    {
                        jsonGenerator.writeEndObject();
                    }
                }
            }
            finally
            {
                jsonGenerator.writeEndArray();
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
            jsonGenerator.writeStartObject();
            currentState = State.DOCUMENT_OPEN;
        }
    }

    private void ensureResultsFieldOpen() throws IOException
    {
        ensureDocumentOpen();
        if ( currentState == State.DOCUMENT_OPEN )
        {
            jsonGenerator.writeArrayFieldStart( "results" );
            currentState = State.RESULTS_OPEN;
        }
    }

    private void ensureResultsFieldClosed() throws IOException
    {
        ensureResultsFieldOpen();
        if ( currentState == State.RESULTS_OPEN )
        {
            jsonGenerator.writeEndArray();
            currentState = State.RESULTS_CLOSED;
        }
    }

    private void ensureStatementFieldClosed() throws IOException
    {
        if ( currentState == State.STATEMENT_OPEN )
        {
            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
            currentState = State.RESULTS_OPEN;
        }
    }

    private void writeColumns( Iterable<String> columns ) throws IOException
    {
        try
        {
            jsonGenerator.writeArrayFieldStart( "columns" );
            for ( String key : columns )
            {
                jsonGenerator.writeString( key );
            }
        }
        finally
        {
            jsonGenerator.writeEndArray(); // </columns>
        }
    }

    private void flush() throws IOException
    {
        jsonGenerator.flush();
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
