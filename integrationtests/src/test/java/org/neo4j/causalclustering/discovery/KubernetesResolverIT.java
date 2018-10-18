/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.discovery.MultiRetryStrategyTest.testRetryStrategy;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

public class KubernetesResolverIT
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    private final int port = PortAuthority.allocatePort();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final String testPortName = "test-port-name";
    private final String testServiceName = "test-service-name";
    private final int testPortNumber = 4313;
    private final String testNamespace = "test-namespace";
    private final String testLabelSelector = "test-label-selector";
    private final String testAuthToken = "Oh go on then";
    private final Config config = Config
            .builder()
            .withSetting( CausalClusteringSettings.kubernetes_address, "localhost:" + port )
            .withSetting( CausalClusteringSettings.kubernetes_label_selector, testLabelSelector )
            .withSetting( CausalClusteringSettings.kubernetes_service_port_name, testPortName )
            .build();

    private AdvertisedSocketAddress expectedAddress =
            new AdvertisedSocketAddress( String.format( "%s.%s.svc.cluster.local", testServiceName, testNamespace ), testPortNumber );

    private final HttpClient httpClient = new HttpClient( new SslContextFactory( true ) );

    private final HostnameResolver resolver = new KubernetesResolver.KubernetesClient(
            new SimpleLogService( userLogProvider, logProvider ),
            httpClient,
            testAuthToken,
            testNamespace,
            config,
            testRetryStrategy( 1 ) );

    @Test
    public void shouldResolveAddressesFromApiReturningShortJson() throws Throwable
    {
        withServer( shortJson(), () -> {
            Collection<AdvertisedSocketAddress> addresses = resolver.resolve( null );

            assertThat( addresses, contains( expectedAddress ) );
        } );
    }

    @Test
    public void shouldResolveAddressesFromApiReturningLongJson() throws Throwable
    {
        withServer( longJson(), () -> {
            Collection<AdvertisedSocketAddress> addresses = resolver.resolve( null );

            assertThat( addresses, contains( expectedAddress ) );
        } );
    }

    @Test
    public void shouldLogResolvedAddressesToUserLog() throws Throwable
    {
        withServer( longJson(), () -> {
           resolver.resolve( null );
           userLogProvider.assertContainsMessageContaining( "Resolved %s from Kubernetes API at %s namespace %s labelSelector %s" );
        } );
    }

    @Test
    public void shouldLogEmptyAddressesToDebugLog() throws Throwable
    {
        String response = "{ \"kind\":\"ServiceList\", \"items\":[] }";
        withServer( response, () -> {
            resolver.resolve( null );
            logProvider.assertContainsMessageContaining( "Resolved empty hosts from Kubernetes API at %s namespace %s labelSelector %s" );
        } );
    }

    @Test
    public void shouldLogParseErrorToDebugLog() throws Throwable
    {
        String response = "{}";
        withServer( response, () -> {
            resolver.resolve( null );
            logProvider.assertContainsMessageContaining( "Failed to parse result from Kubernetes API" );
        } );
    }

    @Test
    public void shouldReportFailureDueToAuth() throws Throwable
    {
        expected.expect( IllegalStateException.class );
        expected.expectMessage( "Forbidden" );

        withServer( failJson(), () -> {
            resolver.resolve( null );
        } );
    }

    public void withServer( String json, Runnable test ) throws Exception
    {
        Server server = setUp( json );

        try
        {
            test.run();
        }
        finally
        {
            tearDown( server );
        }
    }

    private String failJson() throws IOException, URISyntaxException
    {
        return readJsonFile( "authFail.json" );
    }

    private String shortJson() throws IOException, URISyntaxException
    {
        return readJsonFile( "short.json" );
    }

    private String longJson() throws IOException, URISyntaxException
    {
        return readJsonFile( "long.json" );
    }

    private String readJsonFile( final String fileName ) throws IOException, URISyntaxException
    {
        Path path = Paths.get( getClass().getResource( "/org.neo4j.causalclustering.discovery/" + fileName ).toURI() );
        String fullFile = Files.lines( path ).collect( Collectors.joining( "\n" ) );
        return String.format( fullFile, testServiceName, testPortName, testPortNumber );
    }

    private Server setUp( String response ) throws Exception
    {
        Server server = new Server();
        server.setHandler( new FakeKubernetesHandler( testNamespace, testLabelSelector, testAuthToken, response ) );

        HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer( new SecureRequestCustomizer() );

        String keyStorePass = "key store pass";
        String privateKeyPass = "private key pass";
        SslResource server1 = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "k8s" ) );
        SslPolicy sslPolicy = org.neo4j.ssl.SslContextFactory.makeSslPolicy( server1 );
        KeyStore keyStore = sslPolicy.getKeyStore( keyStorePass.toCharArray(), privateKeyPass.toCharArray() );

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStore( keyStore );
        sslContextFactory.setKeyStorePassword( keyStorePass );
        sslContextFactory.setKeyManagerPassword( privateKeyPass );

        ServerConnector sslConnector = new ServerConnector(
                server,
                new SslConnectionFactory( sslContextFactory, "http/1.1" ),
                new HttpConnectionFactory( https )
        );

        sslConnector.setPort( port );

        server.setConnectors( new Connector[]{sslConnector} );

        server.start();

        httpClient.start();

        return server;
    }

    private void tearDown( Server server ) throws Exception
    {
        httpClient.stop();
        server.stop();
    }

    private static class FakeKubernetesHandler extends AbstractHandler
    {
        private final String expectedNamespace;
        private final String expectedLabelSelector;
        private final String expectedAuthToken;
        private final String body;

        private FakeKubernetesHandler( String expectedNamespace, String labelSelector, String authToken, String body )
        {
            this.expectedNamespace = expectedNamespace;
            this.expectedLabelSelector = labelSelector;
            this.expectedAuthToken = authToken;
            this.body = body;
        }

        @Override
        public void handle( String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response ) throws IOException
        {
            PrintWriter out = response.getWriter();
            response.setContentType( MimeTypes.Type.APPLICATION_JSON.asString() );

            String path = request.getPathInfo();
            String expectedPath = String.format( KubernetesResolver.KubernetesClient.path, expectedNamespace );

            String labelSelector = request.getParameter( "labelSelector" );
            String auth = request.getHeader( HttpHeader.AUTHORIZATION.name() );
            String expectedAuth = "Bearer " + expectedAuthToken;

            if ( !expectedPath.equals( path ) )
            {
                response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
                out.println( fail( "Unexpected path: " + path ) );
            }
            else if ( !expectedLabelSelector.equals( labelSelector ) )
            {
                response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
                out.println( fail( "Unexpected labelSelector: " + labelSelector ) );
            }
            else if ( !expectedAuth.equals( auth ) )
            {
                response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
                out.println( fail( "Unexpected auth header value: " + auth ) );
            }
            else if ( !"GET".equals( request.getMethod() ) )
            {
                response.setStatus( HttpServletResponse.SC_BAD_REQUEST );
                out.println( fail( "Unexpected method: " + request.getMethod() ) );
            }
            else
            {
                response.setStatus( HttpServletResponse.SC_OK );
                if ( body != null )
                {
                    out.println( body );
                }
            }

            baseRequest.setHandled( true );
        }

        private String fail( String message )
        {
            return String.format( "{ \"kind\": \"Status\", \"message\": \"%s\"}", message );
        }
    }
}
