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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.ws.rs.core.MediaType;

import org.mortbay.jetty.HttpURI;
import org.mortbay.jetty.Request;

public class InternalJettyServletRequest extends Request
{

    private class Input extends ServletInputStream
    {

        private final byte[] bytes;
        private int position = 0;

        public Input( String data )
        {
            try
            {
                bytes = data.getBytes("UTF-8");
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( e );
            }
        }

        public int read() throws IOException
        {
            if ( bytes.length > position ) return (int) bytes[position++];

            return -1;
        }

        public int length()
        {
            return bytes.length;
        }

        public long contentRead()
        {
            return (long) position;
        }
    }

    private final Map<String, Object> headers;
    private final Cookie[] cookies;
    private final Input input;
    private final BufferedReader inputReader;
    private String contentType;
    private final String method;

    public InternalJettyServletRequest( String method, String uri, String body ) throws UnsupportedEncodingException
    {
        this( method, new HttpURI( uri ), body, new Cookie[] {}, MediaType.APPLICATION_JSON, "UTF-8" );
    }

    public InternalJettyServletRequest( String method, HttpURI uri, String body, Cookie[] cookies, String contentType, String encoding )
            throws UnsupportedEncodingException
    {
        this.input = new Input( body );
        this.inputReader = new BufferedReader( new StringReader( body ) );

        this.contentType = contentType;
        this.cookies = cookies;
        this.method = method;

        this.headers = new HashMap<String, Object>();

        setUri( uri );
        setCharacterEncoding( encoding );
        setRequestURI( null );
        setQueryString( null );
        setScheme(uri.getScheme());
    }

    @Override
    public int getContentLength()
    {
        return input.length();
    }

    @Override
    public String getContentType()
    {
        return contentType;
    }

    public void setContentType( String contentType )
    {
        this.contentType = contentType;
    }

    public long getContentRead()
    {
        return input.contentRead();
    }

    @Override
    public ServletInputStream getInputStream() throws IOException
    {
        return input;
    }

    @Override
    public String getProtocol()
    {
        return "HTTP/1.1";
    }

    @Override
    public BufferedReader getReader() throws IOException
    {
        return inputReader;
    }

    @Override
    public String getRemoteAddr()
    {
        return null;
    }

    @Override
    public String getRemoteHost()
    {
        return null;
    }

    @Override
    public boolean isSecure()
    {
        return false;
    }

    @Override
    public int getRemotePort()
    {
        return 0;
    }

    @Override
    public String getLocalName()
    {
        return null;
    }

    @Override
    public String getLocalAddr()
    {
        return null;
    }

    @Override
    public int getLocalPort()
    {
        return 0;
    }

    @Override
    public String getAuthType()
    {
        return null;
    }

    @Override
    public Cookie[] getCookies()
    {
        return cookies;
    }

    public void addHeader(String header, String value)
    {
        headers.put(header, value);
    }

    @Override
    public long getDateHeader( String name )
    {
        if ( headers.containsKey( name ) )
        {
            return (Long) headers.get( name );
        }
        return -1;
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

    @Override
    public Enumeration<?> getHeaderNames()
    {
        return Collections.enumeration( headers.keySet() );
    }

    @Override
    public int getIntHeader( String name )
    {
        if ( headers.containsKey( name ) )
        {
            return (Integer) headers.get( name );
        }
        return -1;
    }

    @Override
    public String getMethod()
    {
        return method;
    }

    @Override
    public String toString()
    {
        return String.format( "%s %s %s\n%s", getMethod(), getUri(), getProtocol(), getConnection() != null ? getConnection().getRequestFields() : "no HttpConnection" );
    }
}
