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
package org.neo4j.harness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.harness.Neo4jBuilders.newInProcessBuilder;
import static org.neo4j.internal.helpers.collection.Iterables.asIterable;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.server.WebContainerTestUtils.verifyConnector;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class InProcessServerBuilderIT
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = directory.directory( "specific" );

        // When
        try ( Neo4j neo4j = getTestBuilder( workDir ).build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status() ).isEqualTo( 200 );
            assertThat( workDir.list().length ).isEqualTo( 1 );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( workDir.list().length ).as( Arrays.toString( workDir.list() ) ).isEqualTo( 0 );
    }

    @Test
    void shouldAllowCustomServerAndDbConfig() throws Exception
    {
        // Given
        trustAllSSLCerts();

        // Get default trusted cypher suites
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        List<String> defaultCiphers = Arrays.asList( ssf.getDefaultCipherSuites() );

        // When
        SslPolicyConfig pem = SslPolicyConfig.forScope( HTTPS );

        var certificates = directory.directory( "certificates" );
        SelfSignedCertificateFactory.create( certificates, "private.key", "public.crt" );
        new File( certificates, "trusted" ).mkdir();
        new File( certificates, "revoked" ).mkdir();

        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() )
                .withConfig( HttpConnector.enabled, true )
                .withConfig( HttpConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .withConfig( HttpsConnector.enabled, true )
                .withConfig( HttpsConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .withConfig( GraphDatabaseSettings.dense_node_threshold, 20 )
                // override legacy policy
                .withConfig( pem.enabled, Boolean.TRUE )
                .withConfig( pem.base_directory, certificates.toPath() )
                .withConfig( pem.ciphers, defaultCiphers )
                .withConfig( pem.tls_versions, List.of( "TLSv1.2", "TLSv1.1", "TLSv1" ) )
                .withConfig( pem.client_auth, ClientAuth.NONE )
                .withConfig( pem.trust_all, true )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status() ).isEqualTo( 200 );
            assertThat( HTTP.GET( neo4j.httpsURI().toString() ).status() ).isEqualTo( 200 );
            Config config = ((GraphDatabaseAPI) neo4j.defaultDatabaseService()).getDependencyResolver().resolveDependency( Config.class );
            assertEquals( 20, config.get( GraphDatabaseSettings.dense_node_threshold ) );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByClass()
    {
        // When
        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() )
                .withUnmanagedExtension( "/path/to/my/extension", MyUnmanagedExtension.class )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() + "path/to/my/extension/myExtension" ).status() ).isEqualTo( 234 );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByPackage()
    {
        // When
        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() )
                .withUnmanagedExtension( "/path/to/my/extension", "org.neo4j.harness.extensionpackage" )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() + "path/to/my/extension/myExtension" ).status() ).isEqualTo( 234 );
        }
    }

    @Test
    void startWithCustomExtension()
    {
        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() )
                .withExtensionFactories( asIterable( new TestExtensionFactory() ) ).build() )
        {
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status() ).isEqualTo( 200 );
            assertEquals( 1, TestExtension.getStartCounter() );
        }
    }

    @Test
    void startWithDisabledServer()
    {
        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() ).withDisabledServer().build() )
        {
            assertThrows( IllegalStateException.class, neo4j::httpURI );
            assertThrows( IllegalStateException.class, neo4j::httpsURI );

            assertDoesNotThrow( () ->
            {
                GraphDatabaseService service = neo4j.defaultDatabaseService();
                try ( Transaction transaction = service.beginTx() )
                {
                    transaction.createNode();
                    transaction.commit();
                }
            } );
        }
    }

    @Test
    void shouldFindFreePort()
    {
        // Given one server is running
        try ( Neo4j firstServer = getTestBuilder( directory.homeDir() ).build() )
        {
            // When I build a second server
            try ( Neo4j secondServer = getTestBuilder( directory.homeDir() ).build() )
            {
                // Then
                assertThat( secondServer.httpURI().getPort() ).isNotEqualTo( firstServer.httpURI().getPort() );
            }
        }
    }

    @Test
    void shouldRunBuilderOnExistingStoreDir()
    {
        // When
        // create graph db with one node upfront
        File existingHomeDir = directory.homeDir( "existingStore" );

        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( existingHomeDir ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( "create ()" );
            transaction.commit();
        }
        finally
        {
            managementService.shutdown();
        }

        try ( Neo4j neo4j = getTestBuilder( existingHomeDir ).copyFrom( existingHomeDir ).build() )
        {
            // Then
            GraphDatabaseService graphDatabaseService = neo4j.defaultDatabaseService();
            try ( Transaction tx = graphDatabaseService.beginTx() )
            {
                ResourceIterable<Node> allNodes = Iterables.asResourceIterable( tx.getAllNodes() );

                assertTrue( Iterables.count( allNodes ) > 0 );

                // When: create another node
                tx.createNode();
                tx.commit();
            }
        }

        // Then: we still only have one node since the server is supposed to work on a copy
        managementService = new TestDatabaseManagementServiceBuilder( existingHomeDir ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, Iterables.count( tx.getAllNodes() ) );
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldOpenBoltPort()
    {
        // given
        try ( Neo4j neo4j = getTestBuilder( directory.homeDir() ).build() )
        {
            URI uri = neo4j.boltURI();

            // when
            assertDoesNotThrow( () -> new SocketConnection().connect( new HostnamePort( uri.getHost(), uri.getPort() ) ) );
        }
    }

    @Test
    void shouldFailWhenProvidingANonDirectoryAsSource() throws IOException
    {
        File notADirectory = File.createTempFile( "prefix", "suffix" );
        assertFalse( notADirectory.isDirectory() );

        RuntimeException exception =
                assertThrows( RuntimeException.class, () -> getTestBuilder( directory.homeDir() ).copyFrom( notADirectory ).build() );
        Throwable cause = exception.getCause();
        assertTrue( cause instanceof IOException );
        assertTrue( cause.getMessage().contains( "exists but is not a directory" ) );
    }

    @Test
    void shouldStartServerWithHttpHttpsAndBoltDisabled()
    {
        testStartupWithConnectors( false, false, false );
    }

    @Test
    void shouldStartServerWithHttpEnabledAndHttpsBoltDisabled()
    {
        testStartupWithConnectors( true, false, false );
    }

    @Test
    void shouldStartServerWithHttpsEnabledAndHttpBoltDisabled()
    {
        testStartupWithConnectors( false, true, false );
    }

    @Test
    void shouldStartServerWithBoltEnabledAndHttpHttpsDisabled()
    {
        testStartupWithConnectors( false, false, true );
    }

    @Test
    void shouldStartServerWithHttpHttpsEnabledAndBoltDisabled()
    {
        testStartupWithConnectors( true, true, false );
    }

    @Test
    void shouldStartServerWithHttpBoltEnabledAndHttpsDisabled()
    {
        testStartupWithConnectors( true, false, true );
    }

    @Test
    void shouldStartServerWithHttpsBoltEnabledAndHttpDisabled()
    {
        testStartupWithConnectors( false, true, true );
    }

    private void testStartupWithConnectors( boolean httpEnabled, boolean httpsEnabled, boolean boltEnabled )
    {
        var certificates = directory.directory( "certificates" );
        Neo4jBuilder serverBuilder = newInProcessBuilder( directory.homeDir() )
                .withConfig( HttpConnector.enabled, httpEnabled )
                .withConfig( HttpConnector.listen_address, new SocketAddress( 0 ) )
                .withConfig( HttpsConnector.enabled, httpsEnabled )
                .withConfig( HttpsConnector.listen_address, new SocketAddress( 0 ) )
                .withConfig( BoltConnector.enabled, boltEnabled )
                .withConfig( BoltConnector.listen_address, new SocketAddress( 0 ) );

        if ( httpsEnabled )
        {
            SelfSignedCertificateFactory.create( certificates );
            serverBuilder.withConfig( SslPolicyConfig.forScope( HTTPS ).enabled, Boolean.TRUE );
            serverBuilder.withConfig( SslPolicyConfig.forScope( HTTPS ).base_directory, certificates.toPath() );
        }

        try ( Neo4j neo4j = serverBuilder.build() )
        {
            GraphDatabaseService db = neo4j.defaultDatabaseService();

            assertDbAccessible( db );
            verifyConnector( db, "http", httpEnabled );
            verifyConnector( db, "https", httpsEnabled );
            verifyConnector( db, "bolt", boltEnabled );
        }
    }

    private static void assertDbAccessible( GraphDatabaseService db )
    {
        Label label = () -> "Person";
        String propertyKey = "name";
        String propertyValue = "Thor Odinson";

        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( propertyKey, propertyValue );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( tx.findNodes( label ) );
            assertEquals( propertyValue, node.getProperty( propertyKey ) );
            tx.commit();
        }
    }

    private void trustAllSSLCerts() throws NoSuchAlgorithmException, KeyManagementException
    {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager()
        {
            @Override
            public void checkClientTrusted( X509Certificate[] arg0, String arg1 )
            {
            }

            @Override
            public void checkServerTrusted( X509Certificate[] arg0, String arg1 )
            {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
        }
        };

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance( "TLS" );
        sc.init( null, trustAllCerts, new SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
    }

    private static Neo4jBuilder getTestBuilder( File workDir )
    {
        return newInProcessBuilder( workDir );
    }

    private static class TestExtensionFactory extends ExtensionFactory<TestExtensionFactory.Dependencies>
    {
        interface Dependencies
        {
        }

        TestExtensionFactory()
        {
            super( "testExtension" );
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
        {
            return new TestExtension();
        }
    }

    private static class TestExtension extends LifecycleAdapter
    {

        static final AtomicLong startCounter = new AtomicLong();

        @Override
        public void start()
        {
            startCounter.incrementAndGet();
        }

        static long getStartCounter()
        {
            return startCounter.get();
        }
    }

}
