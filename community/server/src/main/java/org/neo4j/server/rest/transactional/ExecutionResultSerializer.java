/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.repr.util.RFC1123;
import org.neo4j.server.rest.transactional.error.Neo4jError;

/**
 * Writes directly to an output stream, therefore implicitly stateful. Methods must be invoked in the correct
 * order, as follows:
 * <ul>
 * <li>{@link #transactionCommitUri(URI) transactionId}{@code ?}</li>
 * <li>{@link #statementResult(ExecutionResult, ResultDataContent...) statementResult}{@code *}</li>
 * <li>{@link #errors(Iterable) errors}{@code ?}</li>
 * <li>{@link #transactionStatus(long expiryDate)}{@code ?}</li>
 * <li>{@link #finish() finish}</li>
 * </ul>
 * <p/>
 * Where {@code ?} means invoke at most once, and {@code *} means invoke zero or more times.
 */
public class ExecutionResultSerializer
{
    public ExecutionResultSerializer( OutputStream output, StringLogger log )
    {
        this.log = log;
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
     * Will always get called at most once once, and is the first method to get called. This method is not allowed
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
    public void statementResult( ExecutionResult result, ResultDataContent... resultDataContents ) throws IOException
    {
        try
        {
            ensureResultsFieldOpen();
            out.writeStartObject();
            try
            {
                Iterable<String> columns = result.columns();
                writeColumns( columns );
                writeRows( columns, result.iterator(), configureWriters( resultDataContents ) );
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

    /**
     * Will get called once if any errors occurred, after {@link #statementResult(ExecutionResult, ResultDataContent...)}  statementResults}
     * has been called This method is not allowed to throw exceptions. If there are network errors or similar, the
     * handler should take appropriate action, but never fail this method.
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
                        out.writeObjectField( "code", error.getStatusCode().getCode() );
                        out.writeObjectField( "status", error.getStatusCode().name() );
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
            return ResultDataContent.row.writer(); // default
        }
        if ( specifiers.length == 1 )
        {
            return specifiers[0].writer();
        }
        ResultDataContentWriter[] writers = new ResultDataContentWriter[specifiers.length];
        for ( int i = 0; i < specifiers.length; i++ )
        {
            writers[i] = specifiers[i].writer();
        }
        return new AggregatingWriter( writers );
    }

    private enum State
    {
        EMPTY, DOCUMENT_OPEN, RESULTS_OPEN, RESULTS_CLOSED, ERRORS_WRITTEN
    }

    private State currentState = State.EMPTY;

    private static final JsonFactory JSON_FACTORY = new JsonFactory( new Neo4jJsonCodec() );
    private final JsonGenerator out;
    private final StringLogger log;

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

    private void writeRows( Iterable<String> columns, Iterator<Map<String, Object>> data,
                            ResultDataContentWriter writer ) throws IOException
    {
        out.writeArrayFieldStart( "data" );
        try
        {
            while ( data.hasNext() )
            {
                Map<String, Object> row = data.next();
                out.writeStartObject();
                try
                {
                    writer.write( out, columns, row );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
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
        log.error( "Failed to generate JSON output.", exception );
        return exception;
    }
}
