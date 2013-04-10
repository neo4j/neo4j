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
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.UnknownStatementError;
import org.neo4j.server.rest.web.TransactionUriScheme;

public class ExecutionResultSerializer implements TransactionalActions.ResultHandler
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec( new Neo4jJsonCodec() );

    private final JsonGenerator out;
    private final TransactionUriScheme scheme;

    public ExecutionResultSerializer( OutputStream output, TransactionUriScheme scheme )
    {
        this.scheme = scheme;
        JsonGenerator generator = null;
        try
        {
            generator = JSON_FACTORY.createJsonGenerator( output );
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
        out = generator;
    }

    @Override
    public void prologue( long txId )
    {
        try
        {
            out.writeStartObject();
            out.writeStringField( "commit", scheme.txCommitUri( txId ).toString() );
            out.writeArrayFieldStart( "results" );
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }

    @Override
    public void prologue( )
    {
        prologue( true );
    }


    private void prologue( boolean writeResults )
    {
        try
        {
            out.writeStartObject();
            if ( writeResults )
                out.writeArrayFieldStart( "results" );
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }


    @Override
    public void visitStatementResult( ExecutionResult result ) throws Neo4jError
    {
        try
        {
            try
            {
                out.writeStartObject();

                Iterable<String> columns = result.columns();

                writeColumns( columns );
                writeRows( columns, result.iterator() );
            }
            finally
            {
                out.writeEndObject(); // </result>
            }
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }

    private void writeRows( Iterable<String> columns, Iterator<Map<String, Object>> data ) throws IOException,
            UnknownStatementError
    {
        try
        {
            out.writeArrayFieldStart( "data" );
            while ( data.hasNext() )
            {
                Map<String, Object> row = nextRow( data );
                try
                {
                    out.writeStartArray();
                    for ( String key : columns )
                    {
                        Object val = row.get( key );
                        out.writeObject( val );
                    }
                }
                finally
                {
                    out.writeEndArray();
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

    @Override
    public void epilogue( Iterator<Neo4jError> errors )
    {
        epilogue( errors, /* writeEndArray */ true );
    }

    private void epilogue( Iterator<Neo4jError> errors, boolean writeEndArray )
    {
        try
        {
            if ( writeEndArray )
                out.writeEndArray(); // </results>
            try
            {
                out.writeArrayFieldStart( "errors" );
                while ( errors.hasNext() )
                {
                    Neo4jError error = errors.next();
                    try
                    {
                        out.writeStartObject();
                        out.writeObjectField( "code", error.getErrorCode().getCode() );
                        out.writeObjectField( "message", error.getMessage() );
                    }
                    finally
                    {
                        out.writeEndObject();
                    }
                }
                out.writeEndArray();  // </errors>
            }
            finally
            {
                out.writeEndObject(); // </result>
                out.flush();
            }
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }

    public void errorsOnly( Iterator<Neo4jError> errors )
    {
        prologue( /* writeResults */ false );
        epilogue( errors, /* writeEndArray */ false );
    }

    private Map<String, Object> nextRow( Iterator<Map<String, Object>> data ) throws UnknownStatementError
    {
        try
        {
            return data.next();
        }
        catch ( RuntimeException e )
        {

            throw new UnknownStatementError( "Executing statement failed.", e );
        }
    }

    private void handleIOException( IOException exc )
    {
        exc.printStackTrace();
    }
}
