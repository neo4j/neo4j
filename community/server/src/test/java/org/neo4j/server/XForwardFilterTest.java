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
package org.neo4j.server;

import com.sun.jersey.core.header.InBoundHeaders;
import org.glassfish.jersey.internal.PropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Test;

import java.net.URI;
import javax.ws.rs.core.SecurityContext;

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
    public void shouldSetTheBaseUriToTheSameValueAsTheXForwardHostHeader()
    {
        // given
        final String xForwardHostAndPort = "jimwebber.org:1234";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_HOST_HEADER_KEY, xForwardHostAndPort );

        ContainerRequest request = new ContainerRequest(
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ), "GET",
                mock( SecurityContext.class ), mock( PropertiesDelegate.class ) );

        request.headers( headers );

        // when
        filter.filter( request );

        // then
        assertThat( request.getBaseUri().toString(), containsString( xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameValueAsTheXForwardHostHeader()
    {
        // given
        final String xForwardHostAndPort = "jimwebber.org:1234";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_HOST_HEADER_KEY, xForwardHostAndPort );

        ContainerRequest request = new ContainerRequest(
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ), "GET",
                mock( SecurityContext.class ), mock( PropertiesDelegate.class ) );

        request.headers( headers );

        // when
        filter.filter( request );

        // then
        assertTrue( request.getRequestUri().toString().startsWith( "http://" + xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheBaseUriToTheSameProtocolAsTheXForwardProtoHeader()
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest(
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ), "GET",
                mock( SecurityContext.class ), mock( PropertiesDelegate.class ) );

        request.headers( headers );

        // when
        filter.filter( request );

        // then
        assertThat( request.getBaseUri().getScheme(), containsString( theProtocol ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameProtocolAsTheXForwardProtoHeader()
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest(
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ), "GET",
                mock( SecurityContext.class ), mock( PropertiesDelegate.class ) );

        request.headers( headers );

        // when
        filter.filter( request );

        // then
        assertThat( request.getBaseUri().getScheme(), containsString( theProtocol ) );
    }
}
