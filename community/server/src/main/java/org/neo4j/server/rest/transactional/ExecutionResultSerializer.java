/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Exceptions;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.repr.util.RFC1123;
import org.neo4j.server.rest.transactional.error.Neo4jError;

import static org.neo4j.server.rest.domain.JsonHelper.writeValue;

/**
 * Writes directly to an output stream, therefore implicitly stateful. Methods must be invoked in the correct
 * order, as follows:
 * <ul>
 * <li>{@link #transactionCommitUri(URI) transactionId}{@code ?}</li>
 * <li>{@link #statementResult(org.neo4j.graphdb.Result, boolean, ResultDataContent...) statementResult}{@code *}</li>
 * <li>{@link #errors(Iterable) errors}{@code ?}</li>
 * <li>{@link #transactionStatus(long expiryDate)}{@code ?}</li>
 * <li>{@link #finish() finish}</li>
 * </ul>
 * <p>
 * Where {@code ?} means invoke at most once, and {@code *} means invoke zero or more times.
 */
public class ExecutionResultSerializer
{
    public ExecutionResultSerializer( OutputStream output, URI baseUri, LogProvider logProvider )
    {
        this.baseUri = baseUri;
        this.log = logProvider.getLog( getClass() );
        JsonGenerator generator = null;
        try
        {
            generator = JSON_FACTORY.createJsonGenerator( output );
        }
        catch ( IOException e )
        {
            loggedIOException( e );
        }
        this.out = generator;
    }

    /**
     * Will always get called at most once, and is the first method to get called. This method is not allowed
     * to throw exceptions. If there are network errors or similar, the handler should take appropriate action,
     * but never fail this method.
     */
    public void transactionCommitUri( URI commitUri )
    {
        try
        {
            ensureDocumentOpen();
            out.writeStringField( "commit", commitUri.toString() );
        }
        catch ( IOException e )
        {
            loggedIOException( e );
        }
    }

    /**
     * Will get called at most once per statement. Throws IOException so that upstream executor can decide whether
     * to execute further statements.
     */
    public void statementResult( Result result, boolean includeStats, ResultDataContent... resultDataContents )
            throws IOException
    {
        try
        {
            ensureResultsFieldOpen();
            out.writeStartObject();
            try
            {
                Iterable<String> columns = result.columns();
                writeColumns( columns );
                writeRows( columns, result, configureWriters( resultDataContents ) );
                if ( includeStats )
                {
                    writeStats( result.getQueryStatistics() );
                }
                if ( result.getQueryExecutionType().requestedExecutionPlanDescription() )
                {
                    writeRootPlanDescription( result.getExecutionPlanDescription() );
                }
            }
            finally
            {
                out.writeEndObject(); // </result>
            }
        }
        catch ( IOException e )
        {
            throw loggedIOException( e );
        }
    }

    public void notifications( Iterable<Notification> notifications ) throws IOException
    {
        //don't add anything if notifications are empty
        if ( !notifications.iterator().hasNext() ) return;

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
            throw loggedIOException( e );
        }
    }

    private void writePosition( InputPosition position ) throws IOException
    {
        //do not add position if empty
        if ( position == InputPosition.empty ) return;

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
            for (ExecutionPlanDescription child : children )
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
        for ( Map.Entry<String, Object> entry : planDescription.getArguments().entrySet() )
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

    /**
     * Will get called once if any errors occurred, after {@link #statementResult(org.neo4j.graphdb.Result, boolean, ResultDataContent...)}  statementResults}
     * has been called This method is not allowed to throw exceptions. If there are network errors or similar, the
     * handler should take appropriate action, but never fail this method.
     * @param errors the errors to write
     */
    public void errors( Iterable<? extends Neo4jError> errors )
    {
        try
        {
            ensureDocumentOpen();
            ensureResultsFieldClosed();
            out.writeArrayFieldStart( "errors" );
            try
            {
                for ( Neo4jError error : errors )
                {
                    try
                    {
                        out.writeStartObject();
                        out.writeObjectField( "code", error.status().code().serialize() );
                        out.writeObjectField( "message", error.getMessage() );
                        if ( error.shouldSerializeStackTrace() )
                        {
                            out.writeObjectField( "stackTrace", error.getStackTraceAsString() );
                        }
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
            loggedIOException( e );
        }
    }

    public void transactionStatus( long expiryDate )
    {
        try
        {
            ensureDocumentOpen();
            ensureResultsFieldClosed();
            out.writeObjectFieldStart( "transaction" );
            out.writeStringField( "expires", RFC1123.formatDate( new Date( expiryDate ) ) );
            out.writeEndObject();
        }
        catch ( IOException e )
        {
            loggedIOException( e );
        }
    }

    /**
     * This method must be called exactly once, and no method must be called after calling this method.
     * This method may not fail.
     */
    public void finish()
    {
        try
        {
            ensureDocumentOpen();
            if ( currentState != State.ERRORS_WRITTEN )
            {
                errors( Collections.<Neo4jError>emptyList() );
            }
            out.writeEndObject();
            out.flush();
        }
        catch ( IOException e )
        {
            loggedIOException( e );
        }
    }

    private ResultDataContentWriter configureWriters( ResultDataContent[] specifiers )
    {
        if ( specifiers == null || specifiers.length == 0 )
        {
            return ResultDataContent.row.writer( baseUri ); // default
        }
        if ( specifiers.length == 1 )
        {
            return specifiers[0].writer( baseUri );
        }
        ResultDataContentWriter[] writers = new ResultDataContentWriter[specifiers.length];
        for ( int i = 0; i < specifiers.length; i++ )
        {
            writers[i] = specifiers[i].writer( baseUri );
        }
        return new AggregatingWriter( writers );
    }

    private enum State
    {
        EMPTY, DOCUMENT_OPEN, RESULTS_OPEN, RESULTS_CLOSED, ERRORS_WRITTEN
    }

    private State currentState = State.EMPTY;

    private static final JsonFactory JSON_FACTORY = new JsonFactory( new Neo4jJsonCodec() ).disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM );
    private final JsonGenerator out;
    private final URI baseUri;
    private final Log log;

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

    private void writeRows( final Iterable<String> columns, Result data, final ResultDataContentWriter writer )
            throws IOException
    {
        out.writeArrayFieldStart( "data" );
        try
        {
            data.accept( new Result.ResultVisitor<IOException>()
            {
                @Override
                public boolean visit( Result.ResultRow row ) throws IOException
                {
                    out.writeStartObject();
                    try
                    {
                        writer.write( out, columns, row );
                    }
                    finally
                    {
                        out.writeEndObject();
                    }
                    return true;
                }
            } );
        }
        finally
        {
            out.writeEndArray(); // </data>
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

    private IOException loggedIOException( IOException exception )
    {
        if(Exceptions.contains(exception, "Broken pipe", IOException.class ))
        {
            log.error( "Unable to reply to request, because the client has closed the connection (Broken pipe)." );
        }
        else
        {
            log.error( "Failed to generate JSON output.", exception );
        }
        return exception;
    }
}
