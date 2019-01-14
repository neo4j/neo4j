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

import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.core.ResourceContext;
import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.core.spi.component.ioc.IoCComponentProviderFactory;
import com.sun.jersey.core.util.FeaturesAndProperties;
import com.sun.jersey.server.impl.inject.ServerInjectableProviderFactory;
import com.sun.jersey.spi.MessageBodyWorkers;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseWriter;
import com.sun.jersey.spi.container.ExceptionMapperContext;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.monitoring.DispatchingListener;
import com.sun.jersey.spi.monitoring.RequestListener;
import com.sun.jersey.spi.monitoring.ResponseListener;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import javax.ws.rs.ext.Providers;

import org.neo4j.server.web.XForwardFilter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

        ContainerRequest request = new ContainerRequest( WEB_APPLICATION, "GET",
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ),
                headers, INPUT_STREAM );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().toString(), containsString( xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameValueAsTheXForwardHostHeader()
    {
        // given
        final String xForwardHostAndPort = "jimwebber.org:1234";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_HOST_HEADER_KEY, xForwardHostAndPort );

        ContainerRequest request = new ContainerRequest( WEB_APPLICATION, "GET",
                URI.create( "http://iansrobinson.com" ), URI.create( "http://iansrobinson.com/foo/bar" ),
                headers, INPUT_STREAM );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertTrue( result.getRequestUri().toString().startsWith( "http://" + xForwardHostAndPort ) );
    }

    @Test
    public void shouldSetTheBaseUriToTheSameProtocolAsTheXForwardProtoHeader()
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest( WEB_APPLICATION, "GET",
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ),
                headers, INPUT_STREAM );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().getScheme(), containsString( theProtocol ) );
    }

    @Test
    public void shouldSetTheRequestUriToTheSameProtocolAsTheXForwardProtoHeader()
    {
        // given
        final String theProtocol = "https";

        XForwardFilter filter = new XForwardFilter();

        InBoundHeaders headers = new InBoundHeaders();
        headers.add( X_FORWARD_PROTO_HEADER_KEY, theProtocol );

        ContainerRequest request = new ContainerRequest( WEB_APPLICATION, "GET",
                URI.create( "http://jimwebber.org:1234" ), URI.create( "http://jimwebber.org:1234/foo/bar" ),
                headers, INPUT_STREAM );

        // when
        ContainerRequest result = filter.filter( request );

        // then
        assertThat( result.getBaseUri().getScheme(), containsString( theProtocol ) );
    }

    //Mocking WebApplication leads to flakyness on ibm-jdk, hence
    //we use a manual mock instead
    private static final WebApplication WEB_APPLICATION = new WebApplication()
    {
        @Override
        public boolean isInitiated()
        {
            return false;
        }

        @Override
        public void initiate( ResourceConfig resourceConfig ) throws IllegalArgumentException, ContainerException
        {

        }

        @Override
        public void initiate( ResourceConfig resourceConfig, IoCComponentProviderFactory ioCComponentProviderFactory )
                throws IllegalArgumentException, ContainerException
        {

        }

        @SuppressWarnings( "CloneDoesntCallSuperClone" )
        @Override
        public WebApplication clone()
        {
            return null;
        }

        @Override
        public FeaturesAndProperties getFeaturesAndProperties()
        {
            return null;
        }

        @Override
        public Providers getProviders()
        {
            return null;
        }

        @Override
        public ResourceContext getResourceContext()
        {
            return null;
        }

        @Override
        public MessageBodyWorkers getMessageBodyWorkers()
        {
            return null;
        }

        @Override
        public ExceptionMapperContext getExceptionMapperContext()
        {
            return null;
        }

        @Override
        public HttpContext getThreadLocalHttpContext()
        {
            return null;
        }

        @Override
        public ServerInjectableProviderFactory getServerInjectableProviderFactory()
        {
            return null;
        }

        @Override
        public RequestListener getRequestListener()
        {
            return null;
        }

        @Override
        public DispatchingListener getDispatchingListener()
        {
            return null;
        }

        @Override
        public ResponseListener getResponseListener()
        {
            return null;
        }

        @Override
        public void handleRequest( ContainerRequest containerRequest, ContainerResponseWriter containerResponseWriter )
        {

        }

        @Override
        public void handleRequest( ContainerRequest containerRequest, ContainerResponse containerResponse )
        {

        }

        @Override
        public void destroy()
        {

        }

        @Override
        public boolean isTracingEnabled()
        {
            return false;
        }

        @Override
        public void trace( String s )
        {

        }
    };

    //Using mockito to mock arguments to ContainerRequest leads to flakyness
    //on ibm jdk, hence the manual mocks
    private static final InputStream INPUT_STREAM = new InputStream()
    {
        @Override
        public int read()
        {
            return 0;
        }
    };
}
