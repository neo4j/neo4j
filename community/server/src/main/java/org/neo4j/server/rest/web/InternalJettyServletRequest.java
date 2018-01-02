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

import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.HttpChannelState;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.RequestLogHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

public class InternalJettyServletRequest extends Request
{
    private class Input extends ServletInputStream
    {

        private final byte[] bytes;
        private int position = 0;
        private ReadListener readListener;

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

            if (readListener != null)
                readListener.onAllDataRead();

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

    private static final HttpChannelState DUMMY_HTTP_CHANNEL_STATE = new HttpChannelState( null )
    {
        /**
         * This method is called when request logging is turned on.
         * It is done to examine if the request is a continuation request in
         * {@link RequestLogHandler#handle(String, Request, HttpServletRequest, HttpServletResponse)} method.
         * If this request is a continuation and it is the one that started the call than it is simply logged,
         * else a listener is installed on a continuation completion and this listener does the actual logging.
         *
         * We do not use the continuations/async requests so always return false in this method.
         */
        @Override
        public boolean isAsync()
        {
            return false;
        }
    };

    private final Map<String, Object> headers;
    private final Cookie[] cookies;
    private final Input input;
    private final BufferedReader inputReader;
    private String contentType;
    private final String method;
    private final InternalJettyServletResponse response;

    /** Optional, another HttpServletRequest to use to pull metadata, like remote address and port, out of. */
    private HttpServletRequest outerRequest;

    public InternalJettyServletRequest( String method, String uri, String body, InternalJettyServletResponse res ) throws UnsupportedEncodingException
    {
        this( method, new HttpURI( uri ), body, new Cookie[] {}, MediaType.APPLICATION_JSON, "UTF-8", res );
    }

    public InternalJettyServletRequest( String method, String uri, String body, InternalJettyServletResponse res, HttpServletRequest outerReq ) throws UnsupportedEncodingException
    {
        this( method, new HttpURI( uri ), body, new Cookie[] {}, MediaType.APPLICATION_JSON, "UTF-8", res);
        this.outerRequest = outerReq;
    }

    public InternalJettyServletRequest( String method, HttpURI uri, String body, Cookie[] cookies, String contentType, String encoding, InternalJettyServletResponse res )
            throws UnsupportedEncodingException
    {
        super( null, null );

        this.input = new Input( body );
        this.inputReader = new BufferedReader( new StringReader( body ) );

        this.contentType = contentType;
        this.cookies = cookies;
        this.method = method;
        this.response = res;

        this.headers = new HashMap<>();

        setUri( uri );
        setCharacterEncoding( encoding );
        setRequestURI( null );
        setQueryString( null );

        setScheme(uri.getScheme());
        setDispatcherType( DispatcherType.REQUEST );
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
        return outerRequest == null ? null : outerRequest.getRemoteAddr();
    }

    @Override
    public String getRemoteHost()
    {
        return outerRequest == null ? null : outerRequest.getRemoteHost();
    }

    @Override
    public boolean isSecure()
    {
        return outerRequest != null && outerRequest.isSecure();
    }

    @Override
    public int getRemotePort()
    {
        return outerRequest == null ? -1 : outerRequest.getRemotePort();
    }

    @Override
    public String getLocalName()
    {
        return outerRequest == null ? null : outerRequest.getLocalName();
    }

    @Override
    public String getLocalAddr()
    {
        return outerRequest == null ? null : outerRequest.getLocalAddr();
    }

    @Override
    public int getLocalPort()
    {
        return outerRequest == null ? -1 : outerRequest.getLocalPort();
    }

    @Override
    public String getAuthType()
    {
        return outerRequest == null ? null : outerRequest.getAuthType();
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
        return String.format( "%s %s %s\n%s", getMethod(), getUri(), getProtocol(), getHttpFields() );
    }

    @Override
    public HttpChannelState getHttpChannelState()
    {
        return DUMMY_HTTP_CHANNEL_STATE;
    }
}
