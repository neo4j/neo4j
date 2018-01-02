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
package org.neo4j.server;

import java.io.InputStream;
import java.net.URI;

import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.WebApplication;
import org.junit.Test;
import org.neo4j.server.web.XForwardFilter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class XForwardFilterTest
{
    private static final String X_FORWARD_HOST_HEADER_KEY = "X-Forwarded-Host";
    private static final String X_FORWARD_PROTO_HEADER_KEY = "X-Forwarded-Proto";

    @Test
    public void shouldSetTheBaseUriToTheSameValueAsTheXForwardHostHeader() throws Exception
    {
        // given
        final String xForwardHostAndPort = "jimwebber.org:1234";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_HOST_HEADER_KEY, xForwardHostAndPort );

        ContainerRequest request = new ContainerRequest( mock( WebApplication.class ), "GET",
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ),
                headers, mock( InputStream.class ) );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().toString(), containsString( xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameValueAsTheXForwardHostHeader() throws Exception
    {
        // given
        final String xForwardHostAndPort = "jimwebber.org:1234";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_HOST_HEADER_KEY, xForwardHostAndPort );

        ContainerRequest request = new ContainerRequest( mock( WebApplication.class ), "GET",
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ),
                headers, mock( InputStream.class ) );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertTrue( result.getRequestUri().toString().startsWith( "http://" + xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheBaseUriToTheSameProtocolAsTheXForwardProtoHeader() throws Exception
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest( mock( WebApplication.class ), "GET",
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ),
                headers, mock( InputStream.class ) );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().getScheme(), containsString( theProtocol ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameProtocolAsTheXForwardProtoHeader() throws Exception
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest( mock( WebApplication.class ), "GET",
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ),
                headers, mock( InputStream.class ) );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().getScheme(), containsString( theProtocol ) );
    }
}
