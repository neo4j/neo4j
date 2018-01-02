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
package org.neo4j.server.rest.batch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonGenerator;

/*
 * Because the batch operation API operates on the HTTP abstraction
 * level, we do not use our normal serialization system for serializing
 * its' results.
 * 
 * Doing so would require us to de-serialize each JSON response we get from
 * each operation, and we would have to extend our current type safe serialization
 * system to incorporate arbitrary responses.
 */
public class StreamingBatchOperationResults
{
    public static final int HEAD_BUFFER = 10;
    public static final int IS_ERROR = -1;
    private final String encoding = "UTF-8";
    private final Map<Integer, String> locations = new HashMap<Integer, String>();
    private final JsonGenerator g;
    private final ServletOutputStream output;
    private ByteArrayOutputStream errorStream;
    private int bytesWritten = 0;
    private char[] head = new char[HEAD_BUFFER];

    public StreamingBatchOperationResults( JsonGenerator g, ServletOutputStream output ) throws IOException {
        this.g = g;
        this.output = output;
        g.writeStartArray();
    }

    public void startOperation(String from, Integer id) throws IOException {
        bytesWritten = 0;
        g.writeStartObject();
        if (id!=null) g.writeNumberField("id", id);
        g.writeStringField("from", from);
        g.writeRaw(",\"body\":");
        g.flush();
    }
    public void addOperationResult(int status, Integer id,String location) throws IOException {
        finishBody();
        if ( location != null ) {
            locations.put(id, location);
            g.writeStringField("location",location);
        }
        g.writeNumberField("status",status);
        g.writeEndObject();
    }

    private void finishBody() throws IOException
    {
        if (bytesWritten == 0 ) {
            g.writeRaw("null");
        } else if ( bytesWritten<HEAD_BUFFER) {
            g.writeRaw( head, 0, bytesWritten);
        }
    }

    public ServletOutputStream getServletOutputStream() {
        return new ServletOutputStream() {
            @Override
            public void write(int i) throws IOException {
                if ( redirectError( i ) ) return;
                writeChar( i );
                bytesWritten++;
                checkHead();
            }

            @Override
            public boolean isReady()
            {
                return true;
            }

            @Override
            public void setWriteListener( WriteListener writeListener )
            {
                try
                {
                    writeListener.onWritePossible();
                }
                catch ( IOException e )
                {
                    // Ignore
                }
            }
        };
    }

    private boolean redirectError( int i )
    {
        if ( bytesWritten != IS_ERROR ) return false;
        errorStream.write( i );
        return true;
    }

    private void writeChar( int i ) throws IOException
    {
        if (bytesWritten < HEAD_BUFFER) {
            head[bytesWritten]= (char) i;
        } else {
            output.write( i );
        }
    }

    private void checkHead() throws IOException
    {
        if (bytesWritten == HEAD_BUFFER) {
            if (isJson(head)) {
                for ( char c : head )
                {
                    output.write( c );
                }
            } else {
                errorStream = new ByteArrayOutputStream( 1024 );
                for ( char c : head )
                {
                    errorStream.write( c );
                }
                bytesWritten = IS_ERROR;
            }
        }
    }

    private boolean isJson( char[] head )
    {
        return String.valueOf( head ).matches( "\\s*([\\[\"\\{]|true|false).*" );
    }
    public Map<Integer, String> getLocations()
    {
        return locations;
    }

    public void close() throws IOException {
        g.writeEndArray();
        g.close();
    }

    public void writeError( int status, String message ) throws IOException {
        if (bytesWritten == 0 || bytesWritten == IS_ERROR) g.writeRaw( "null" );
        g.writeNumberField( "status",  status );
        if (message!=null && !message.trim().equals( Response.Status.fromStatusCode( status ).getReasonPhrase()))  g.writeStringField( "message", message);
        else {
            if (errorStream!=null) {
                g.writeStringField( "message", errorStream.toString( encoding ));
            }
        }
        g.close();
    }
}
