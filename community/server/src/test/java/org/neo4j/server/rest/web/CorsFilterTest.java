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

import org.junit.Test;

import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.enumeration;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_METHODS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_REQUEST_HEADERS;
import static org.neo4j.server.rest.web.CorsFilter.ACCESS_CONTROL_REQUEST_METHOD;

public class CorsFilterTest
{
    private final HttpServletRequest emptyRequest = requestMock( emptyList(), emptyList() );
    private final HttpServletResponse response = responseMock();
    private final FilterChain chain = filterChainMock();

    private final CorsFilter filter = new CorsFilter( NullLogProvider.getInstance(), "*" );

    @Test
    public void shouldCallChainDoFilter() throws Exception
    {
        filter.doFilter( emptyRequest, response, chain );

        verify( chain ).doFilter( emptyRequest, response );
    }

    @Test
    public void shouldSetAccessControlAllowOrigin() throws Exception
    {
        filter.doFilter( emptyRequest, response, filterChainMock() );

        verify( response ).setHeader( ACCESS_CONTROL_ALLOW_ORIGIN, "*" );
    }

    @Test
    public void shouldAttachNoHttpMethodsToAccessControlAllowMethodsWhenHeaderIsEmpty() throws Exception
    {
        filter.doFilter( emptyRequest, response, chain );

        verify( response, never() ).addHeader( eq( ACCESS_CONTROL_ALLOW_METHODS ), anyString() );
    }

    @Test
    public void shouldAttachNoHttpMethodsToAccessControlAllowMethodsWhenHeaderIsNull() throws Exception
    {
        HttpServletRequest request = requestMock();
        when( request.getHeaders( ACCESS_CONTROL_REQUEST_METHOD ) ).thenReturn( null );

        filter.doFilter( request, response, chain );

        verify( response, never() ).addHeader( eq( ACCESS_CONTROL_ALLOW_METHODS ), anyString() );
    }

    @Test
    public void shouldAttachValidHttpMethodsToAccessControlAllowMethods() throws Exception
    {
        List<String> accessControlRequestMethods = asList( "GET", "WRONG", "POST", "TAKE", "CONNECT" );
        HttpServletRequest request = requestMock( accessControlRequestMethods, emptyList() );

        filter.doFilter( request, response, chain );

        verify( response ).addHeader( ACCESS_CONTROL_ALLOW_METHODS, "GET" );
        verify( response ).addHeader( ACCESS_CONTROL_ALLOW_METHODS, "POST" );
        verify( response ).addHeader( ACCESS_CONTROL_ALLOW_METHODS, "CONNECT" );

        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_METHODS, "TAKE" );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_METHODS, "WRONG" );
    }

    @Test
    public void shouldAttachNoRequestHeadersToAccessControlAllowHeadersWhenHeaderIsEmpty() throws Exception
    {
        filter.doFilter( emptyRequest, response, chain );

        verify( response, never() ).addHeader( eq( ACCESS_CONTROL_ALLOW_HEADERS ), anyString() );
    }

    @Test
    public void shouldAttachNoRequestHeadersToAccessControlAllowHeadersWhenHeaderIsNull() throws Exception
    {
        HttpServletRequest request = requestMock();
        when( request.getHeaders( ACCESS_CONTROL_REQUEST_HEADERS ) ).thenReturn( null );

        filter.doFilter( request, response, chain );

        verify( response, never() ).addHeader( eq( ACCESS_CONTROL_ALLOW_HEADERS ), anyString() );
    }

    @Test
    public void shouldAttachValidRequestHeadersToAccessControlAllowHeaders() throws Exception
    {
        List<String> accessControlRequestHeaders = asList( "Accept", "X-Wrong\nHeader", "Content-Type", "Accept\r", "Illegal\r\nHeader", "", null, "   " );
        HttpServletRequest request = requestMock( emptyList(), accessControlRequestHeaders );

        filter.doFilter( request, response, chain );

        verify( response ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "Accept" );
        verify( response ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type" );

        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, null );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "" );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "   " );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "X-Wrong\nHeader" );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "Accept\r" );
        verify( response, never() ).addHeader( ACCESS_CONTROL_ALLOW_HEADERS, "Illegal\r\nHeader" );
    }

    private static HttpServletRequest requestMock()
    {
        return requestMock( emptyList(), emptyList() );
    }

    private static HttpServletRequest requestMock( List<String> accessControlRequestMethods, List<String> accessControlRequestHeaders )
    {
        HttpServletRequest mock = mock( HttpServletRequest.class );
        when( mock.getHeaders( ACCESS_CONTROL_REQUEST_METHOD ) ).thenReturn( enumeration( accessControlRequestMethods ) );
        when( mock.getHeaders( ACCESS_CONTROL_REQUEST_HEADERS ) ).thenReturn( enumeration( accessControlRequestHeaders ) );
        return mock;
    }

    private static HttpServletResponse responseMock()
    {
        return mock( HttpServletResponse.class );
    }

    private static FilterChain filterChainMock()
    {
        return mock( FilterChain.class );
    }
}
