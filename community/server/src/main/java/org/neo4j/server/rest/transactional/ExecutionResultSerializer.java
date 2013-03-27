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
import java.lang.Object;
import java.lang.RuntimeException;
import java.lang.String;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.transactional.error.UnknownStatementError;

public class ExecutionResultSerializer implements TransactionalActions.ResultHandler
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory().setCodec( new Neo4jJsonCodec() );

    private final JsonGenerator out;

    public ExecutionResultSerializer( OutputStream output )
    {
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
    public void begin( long txId )
    {
        try
        {
            out.writeStartObject();
            out.writeNumberField( "txId", txId );
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
            Iterable<String> columns = result.columns();
            Iterator<Map<String, Object>> data = result.iterator();

            try
            {
                out.writeStartObject();

                try
                {
                    out.writeArrayFieldStart( "columns" );
                    for ( String key : columns )
                    {
                        out.writeString( key );
                    }
                } finally
                {
                    out.writeEndArray(); // </columns>
                }

                try
                {
                    out.writeArrayFieldStart( "data" );
                    while(data.hasNext())
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
                        } finally
                        {
                            out.writeEndArray();
                        }
                    }
                } finally
                {
                    out.writeEndArray(); // </data>
                }

            } finally
            {
                out.writeEndObject(); // </result>
            }
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }

    @Override
    public void finish( Iterator<Neo4jError> errors )
    {
        try
        {
            out.writeEndArray(); // </results>
            try
            {
                out.writeArrayFieldStart( "errors" );
                while(errors.hasNext())
                {
                    Neo4jError error = errors.next();
                    try
                    {
                        out.writeStartObject();
                        out.writeObjectField( "code", error.getErrorCode().getCode() );
                        out.writeObjectField( "message", error.getMessage() );
                    } finally
                    {
                        out.writeEndObject();
                    }
                }
            } finally
            {
                out.writeEndArray();  // </errors>
                out.writeEndObject(); // </result>
                out.flush();
            }
        }
        catch ( IOException e )
        {
            handleIOException( e );
        }
    }

    private Map<String, Object> nextRow( Iterator<Map<String, Object>> data ) throws UnknownStatementError
    {
        try
        {
            return data.next();
        } catch(RuntimeException e)
        {

            throw new UnknownStatementError( "Executing statement failed.", e );
        }
    }

    private void handleIOException( IOException exc )
    {
        exc.printStackTrace();
    }
}
