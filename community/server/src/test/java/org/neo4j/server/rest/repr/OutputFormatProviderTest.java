/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import org.junit.Test;
import org.neo4j.server.AbstractNeoServer;

public class OutputFormatProviderTest
{
    @Test
    public void shouldUseXForwardedHostHeaderWhenPresent() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( "foobar.com:9999" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://foobar.com:9999" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseXForwardedProtoHeaderWhenPresent() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "https://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Proto" ) ).thenReturn( "http" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://localhost:37465" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldPickFirstXForwardedHostHeaderValueFromCommaAndSpaceSeparatedList() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( "foobar.com:9999, " +
                "bazbar.org:8888, barbar.me:7777" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://foobar.com:9999" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldPickFirstXForwardedHostHeaderValueFromCommaSeparatedList() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( "foobar.com:9999," +
                "bazbar.org:8888,barbar.me:7777" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://foobar.com:9999" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedHostHeader() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( ":5543:foobar.com:9999" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://localhost:37465" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseBaseUriIfFirstAddressInXForwardedHostHeaderIsBad() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( ":5543:foobar.com:9999, " +
                "bazbar.com:8888" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://localhost:37465" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedProtoHeader() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Proto" ) ).thenReturn( "%%%DEFINITELY-NOT-A-PROTO!" );

        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://localhost:37465" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseXForwardedHostAndXForwardedProtoHeadersWhenPresent() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost:37465" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( "foobar.com:9999" );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Proto" ) ).thenReturn( "https" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "https://foobar.com:9999" ) ),
                any( ExtensionInjector.class ) );
    }

    @Test
    public void shouldUseDefaultPortIfNoPortNumberSpecifiedOnXForwardedHostHeader() throws Exception
    {
        // given
        OutputFormatProvider provider = new OutputFormatProvider( new RepresentationFormatRepository(
                mock( AbstractNeoServer.class ) ) );

        HttpRequestContext httpRequestContext = mock( HttpRequestContext.class );
        HttpContext httpContext = mock( HttpContext.class );
        when( httpContext.getRequest() ).thenReturn( httpRequestContext );
        when( httpRequestContext.getBaseUri() ).thenReturn( new URI( "http://localhost" ) );
        when( httpRequestContext.getHeaderValue( "X-Forwarded-Host" ) ).thenReturn( "foobar.com" );


        Representation representation = mock( Representation.class );
        OutputFormat outputFormat = provider.getValue( httpContext );

        // when
        outputFormat.assemble( representation );

        // then
        verify( representation ).serialize( any( RepresentationFormat.class ),
                eq( new URI( "http://foobar.com" ) ),
                any( ExtensionInjector.class ) );
    }
}
