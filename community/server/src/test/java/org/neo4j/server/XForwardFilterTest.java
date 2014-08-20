package org.neo4j.server;

import java.io.InputStream;
import java.net.URI;

import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.WebApplication;
import org.junit.Test;

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

        XForwardHostFilter filter = new XForwardHostFilter();

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

        XForwardHostFilter filter = new XForwardHostFilter();

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

        XForwardHostFilter filter = new XForwardHostFilter();

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

        XForwardHostFilter filter = new XForwardHostFilter();

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