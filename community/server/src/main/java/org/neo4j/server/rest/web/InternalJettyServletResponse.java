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
package org.neo4j.server.rest.web;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;

import org.mortbay.jetty.HttpFields;
import org.mortbay.jetty.Response;

public class InternalJettyServletResponse extends Response
{

    private class Output extends ServletOutputStream
    {

        private ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void write( int c ) throws IOException
        {
            baos.write( c );
        }

        public String toString()
        {
            try
            {
                baos.flush();
                String result = baos.toString("UTF-8");
                return result;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e);
            }
        }
    }

    private final Map<String, Object> headers = new HashMap<String, Object>();
    private final Output output = new Output();
    private int status = -1;
    private String message = "";

    public InternalJettyServletResponse()
    {
        super( null );
    }

    public void addCookie( Cookie cookie )
    {
        // TODO Auto-generated method stub
    }

    public String encodeURL( String url )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void sendError( int sc ) throws IOException
    {
        sendError( sc, null );
    }

    public void sendError( int code, String message ) throws IOException
    {
        setStatus( code, message );
    }

    public void sendRedirect( String location ) throws IOException
    {
        setStatus( 304 );
        addHeader( "location", location );
    }

    public boolean containsHeader( String name )
    {
        return headers.containsKey( name );
    }

    public void setDateHeader( String name, long date )
    {
        headers.put( name, date );
    }

    public void addDateHeader( String name, long date )
    {
        if ( headers.containsKey( name ) )
        {
            headers.put( name, date );
        }
    }

    public void addHeader( String name, String value )
    {
        setHeader( name, value );
    }

    public void setHeader( String name, String value )
    {
        headers.put( name, value );
    }

    public void setIntHeader( String name, int value )
    {
        headers.put( name, value );
    }

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
    public Enumeration<?> getHeaders( String name )
    {
        if ( headers.containsKey( name ) )
        {
            Object value = headers.get( name );
            if ( value instanceof Collection )
            {
                return Collections.enumeration( (Collection<?>) value );
            }
            else
            {
                return Collections.enumeration( Collections.singleton( value ) );
            }
        }
        return null;
    }

    public void setStatus( int sc, String sm )
    {
        status = sc;
        message = sm;
    }

    public int getStatus()
    {
        return status;
    }

    public String getReason()
    {
        return message;
    }

    public ServletOutputStream getOutputStream() throws IOException
    {
        return output;
    }

    public boolean isWriting()
    {
        return false;
    }

    public PrintWriter getWriter() throws IOException
    {
        return new PrintWriter( output );
    }

    public void setCharacterEncoding( String encoding )
    {

    }

    public void setContentLength( int len )
    {
    }

    public void setLongContentLength( long len )
    {
    }

    public void setContentType( String contentType )
    {
    }

    public void setBufferSize( int size )
    {
    }

    public int getBufferSize()
    {
        return -1;
    }

    public void flushBuffer() throws IOException
    {
    }

    public String toString()
    {
        return null;
    }

    public HttpFields getHttpFields()
    {
        return null;
    }

    public long getContentCount()
    {
        return 1l;
    }

    public void complete() throws IOException
    {
    }

    public void setLocale( Locale locale )
    {
    }

    public boolean isCommitted()
    {
        return false;
    }

}
