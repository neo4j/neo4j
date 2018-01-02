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
package org.neo4j.server.rest.web;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Response;

public class InternalJettyServletResponse extends Response
{

    private class Output extends ServletOutputStream
    {

        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void write( int c ) throws IOException
        {
            baos.write( c );
        }

        @Override
        public String toString()
        {
            try
            {
                baos.flush();
                return baos.toString("UTF-8");
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e);
            }
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
    }

    private final Map<String, Object> headers = new HashMap<>();
    private final Output output = new Output();
    private int status = -1;
    private String message = "";

    public InternalJettyServletResponse()
    {
        super( null, null );
    }

    @Override
    public void addCookie( Cookie cookie )
    {
        // TODO Auto-generated method stub
    }

    @Override
    public String encodeURL( String url )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void sendError( int sc ) throws IOException
    {
        sendError( sc, null );
    }

    @Override
    public void sendError( int code, String message ) throws IOException
    {
        setStatus( code, message );
    }

    @Override
    public void sendRedirect( String location ) throws IOException
    {
        setStatus( 304 );
        addHeader( "location", location );
    }

    @Override
    public boolean containsHeader( String name )
    {
        return headers.containsKey( name );
    }

    @Override
    public void setDateHeader( String name, long date )
    {
        headers.put( name, date );
    }

    @Override
    public void addDateHeader( String name, long date )
    {
        if ( headers.containsKey( name ) )
        {
            headers.put( name, date );
        }
    }

    @Override
    public void addHeader( String name, String value )
    {
        setHeader( name, value );
    }

    @Override
    public void setHeader( String name, String value )
    {
        headers.put( name, value );
    }

    @Override
    public void setIntHeader( String name, int value )
    {
        headers.put( name, value );
    }

    @Override
    public void addIntHeader( String name, int value )
    {
        setIntHeader( name, value );
    }

    @Override
    public String getHeader( String name )
    {
        if ( headers.containsKey( name ) )
        {
            Object value = headers.get( name );
            if ( value instanceof String )
            {
                return (String) value;
            }
            else if ( value instanceof Collection )
            {
                return ( (Collection<?>) value ).iterator()
                        .next()
                        .toString();
            }
            else
            {
                return value.toString();
            }
        }

        return null;
    }

    public Map<String, Object> getHeaders()
    {
        return headers;
    }

    @Override
    public Collection<String> getHeaders( String name )
    {
        if ( headers.containsKey( name ) )
        {
            Object value = headers.get( name );
            if ( value instanceof Collection )
            {
                return (Collection<String>) value;
            }
            else
            {
                return Collections.singleton( (String) value );
            }
        }
        return null;
    }

    @Override
    public void setStatus( int sc )
    {
        status = sc;
    }

    @Override
    public void setStatus( int sc, String sm )
    {
        status = sc;
        message = sm;
    }

    @Override
    public int getStatus()
    {
        return status;
    }

    @Override
    public String getReason()
    {
        return message;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException
    {
        return output;
    }

    @Override
    public boolean isWriting()
    {
        return false;
    }

    @Override
    public PrintWriter getWriter() throws IOException
    {
        return new PrintWriter( new OutputStreamWriter( output, "UTF-8") );
    }

    @Override
    public void setCharacterEncoding( String encoding )
    {

    }

    @Override
    public void setContentLength( int len )
    {
    }

    @Override
    public void setLongContentLength( long len )
    {
    }

    @Override
    public void setContentType( String contentType )
    {
    }

    @Override
    public void setBufferSize( int size )
    {
    }

    @Override
    public int getBufferSize()
    {
        return -1;
    }

    @Override
    public void flushBuffer() throws IOException
    {
    }

    @Override
    public String toString()
    {
        return null;
    }

    @Override
    public HttpFields getHttpFields()
    {
        return null;
    }

    @Override
    public long getContentCount()
    {
        return 1l;
    }

    public void complete() throws IOException
    {
    }

    @Override
    public void setLocale( Locale locale )
    {
    }

    @Override
    public boolean isCommitted()
    {
        return false;
    }

}
