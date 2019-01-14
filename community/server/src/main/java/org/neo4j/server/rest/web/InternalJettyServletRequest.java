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
package org.neo4j.server.rest.web;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.MetaData;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.DispatcherType;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;

import org.neo4j.string.UTF8;

public class InternalJettyServletRequest extends Request
{
    private class Input extends ServletInputStream
    {

        private final byte[] bytes;
        private int position;
        private ReadListener readListener;

        Input( String data )
        {
            bytes = UTF8.encode( data );
        }

        @Override
        public int read() throws IOException
        {
            if ( bytes.length > position )
            {
                return (int) bytes[position++];
            }

            if ( readListener != null )
            {
                readListener.onAllDataRead();
            }

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

        @Override
        public boolean isFinished()
        {
            return bytes.length == position;
        }

        @Override
        public boolean isReady()
        {
            return true;
        }

        @Override
        public void setReadListener( ReadListener readListener )
        {
            this.readListener = readListener;
            try
            {
                readListener.onDataAvailable();
            }
            catch ( IOException e )
            {
                // Ignore
            }
        }
    }

    private final Map<String, Object> headers;
    private final Cookie[] cookies;
    private final Input input;
    private final BufferedReader inputReader;
    private String contentType;
    private final String method;
    private final InternalJettyServletResponse response;

    /** Contains metadata for the request, for example remote address and port. */
    private final RequestData requestData;

    public InternalJettyServletRequest( String method, String uri, String body, InternalJettyServletResponse res,
            RequestData requestData ) throws UnsupportedEncodingException
    {
        this( method, new HttpURI( uri ), body, new Cookie[] {}, MediaType.APPLICATION_JSON,
                StandardCharsets.UTF_8.name(), res, requestData );
    }

    public InternalJettyServletRequest( String method, HttpURI uri, String body, Cookie[] cookies, String contentType,
            String encoding, InternalJettyServletResponse res, RequestData requestData ) throws UnsupportedEncodingException
    {
        super( null, null );

        this.input = new Input( body );
        this.inputReader = new BufferedReader( new StringReader( body ) );

        this.contentType = contentType;
        this.cookies = cookies;
        this.method = method;
        this.response = res;
        this.requestData = requestData;

        this.headers = new HashMap<>();

        setCharacterEncoding( encoding );
        setDispatcherType( DispatcherType.REQUEST );

        MetaData.Request request = new MetaData.Request( new HttpFields() );
        request.setMethod( method );
        request.setURI( uri );
        setMetaData( request );
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

    @Override
    public void setContentType( String contentType )
    {
        this.contentType = contentType;
    }

    @Override
    public long getContentRead()
    {
        return input.contentRead();
    }

    @Override
    public ServletInputStream getInputStream()
    {
        return input;
    }

    @Override
    public String getProtocol()
    {
        return "HTTP/1.1";
    }

    @Override
    public BufferedReader getReader()
    {
        return inputReader;
    }

    @Override
    public String getRemoteAddr()
    {
        return requestData.remoteAddress;
    }

    @Override
    public String getRemoteHost()
    {
        throw new UnsupportedOperationException( "Remote host-name lookup might prove expensive, " +
                "this should be explicitly considered." );
    }

    @Override
    public boolean isSecure()
    {
        return requestData.isSecure;
    }

    @Override
    public int getRemotePort()
    {
        return requestData.remotePort;
    }

    @Override
    public String getLocalName()
    {
        return requestData.localName;
    }

    @Override
    public String getLocalAddr()
    {
        return requestData.localAddress;
    }

    @Override
    public int getLocalPort()
    {
        return requestData.localPort;
    }

    @Override
    public String getAuthType()
    {
        return requestData.authType;
    }

    @Override
    public Cookie[] getCookies()
    {
        return cookies;
    }

    public void addHeader( String header, String value )
    {
        headers.put( header, value );
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
    public Enumeration<String> getHeaders( String name )
    {
        if ( headers.containsKey( name ) )
        {
            Object value = headers.get( name );
            if ( value instanceof Collection )
            {
                return Collections.enumeration( (Collection<String>) value );
            }
            else
            {
                return Collections.enumeration( Collections.singleton( (String) value ) );
            }
        }
        return null;
    }

    @Override
    public Enumeration<String> getHeaderNames()
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
    public Response getResponse()
    {
        return response;
    }

    @Override
    public String toString()
    {
        return String.format( "%s %s %s\n%s", getMethod(), this.getHttpURI(), getProtocol(), getHttpFields() );
    }

    public static class RequestData
    {
        public final String remoteAddress;
        public final boolean isSecure;
        public final int remotePort;
        public final String localName;
        public final String localAddress;
        public final int localPort;
        public final String authType;

        public RequestData(
                String remoteAddress,
                boolean isSecure,
                int remotePort,
                String localName,
                String localAddress,
                int localPort,
                String authType )
        {
            this.remoteAddress = remoteAddress;
            this.isSecure = isSecure;
            this.remotePort = remotePort;
            this.localName = localName;
            this.localAddress = localAddress;
            this.localPort = localPort;
            this.authType = authType;
        }

        public static RequestData from( HttpServletRequest req )
        {
            return new RequestData(
                    req.getRemoteAddr(),
                    req.isSecure(),
                    req.getRemotePort(),
                    req.getLocalName(),
                    req.getLocalAddr(),
                    req.getLocalPort(),
                    req.getAuthType() == null ? "" : req.getAuthType()
                );
        }
    }
}
