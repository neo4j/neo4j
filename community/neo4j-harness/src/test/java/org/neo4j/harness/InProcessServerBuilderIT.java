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

import org.codehaus.jackson.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.harness.internal.Neo4jControls;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.ssl.ClientAuth;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.harness.internal.TestNeo4jBuilders.newInProcessBuilder;
import static org.neo4j.helpers.collection.Iterables.asIterable;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.server.ServerTestUtils.connectorAddress;
import static org.neo4j.server.ServerTestUtils.verifyConnector;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
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
        try ( Neo4jControls controls = getTestBuilder( workDir ).build() )
        {
            // Then
            assertThat( HTTP.GET( controls.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo( 1 ) );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    @Test
    void shouldAllowCustomServerAndDbConfig() throws Exception
    {
        // Given
        trustAllSSLCerts();

        // Get default trusted cypher suites
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        String[] defaultCiphers = ssf.getDefaultCipherSuites();

        // When
        HttpConnector httpConnector = new HttpConnector( "0", Encryption.NONE );
        HttpConnector httpsConnector = new HttpConnector( "1", Encryption.TLS );
        try ( Neo4jControls controls = getTestBuilder( directory.directory() )
                .withConfig( httpConnector.type, "HTTP" )
                .withConfig( httpConnector.enabled, "true" )
                .withConfig( httpConnector.encryption, "NONE" )
                .withConfig( httpConnector.listen_address, "localhost:0" )
                .withConfig( httpsConnector.type, "HTTP" )
                .withConfig( httpsConnector.enabled, "true" )
                .withConfig( httpsConnector.encryption, "TLS" )
                .withConfig( httpsConnector.listen_address, "localhost:0" )
                .withConfig( GraphDatabaseSettings.dense_node_threshold, "20" )
                // override legacy policy
                .withConfig( "https.ssl_policy", "test" )
                .withConfig( "dbms.ssl.policy.test.base_directory", directory.directory( "certificates" ).getAbsolutePath() )
                .withConfig( "dbms.ssl.policy.test.allow_key_generation", "true" )
                .withConfig( "dbms.ssl.policy.test.ciphers", String.join( ",", defaultCiphers ) )
                .withConfig( "dbms.ssl.policy.test.tls_versions", "TLSv1.2, TLSv1.1, TLSv1" )
                .withConfig( "dbms.ssl.policy.test.client_auth", ClientAuth.NONE.name() )
                .withConfig( "dbms.ssl.policy.test.trust_all", "true" )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( controls.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( HTTP.GET( controls.httpsURI().get().toString() ).status(), equalTo( 200 ) );
            assertDBConfig( controls, "20", GraphDatabaseSettings.dense_node_threshold.name() );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByClass()
    {
        // When
        try ( Neo4jControls controls = getTestBuilder( directory.directory() )
                .withUnmanagedExtension( "/path/to/my/extension", MyUnmanagedExtension.class )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( controls.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByPackage()
    {
        // When
        try ( Neo4jControls controls = getTestBuilder( directory.directory() )
                .withUnmanagedExtension( "/path/to/my/extension", "org.neo4j.harness.extensionpackage" )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( controls.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    void startWithCustomExtension()
    {
        try ( Neo4jControls controls = getTestBuilder( directory.directory() )
                .withExtensionFactories( asIterable( new TestExtensionFactory() ) ).build() )
        {
            assertThat( HTTP.GET( controls.httpURI().toString() ).status(), equalTo( 200 ) );
            assertEquals( 1, TestExtension.getStartCounter() );
        }
    }

    @Test
    void startWithDisabledServer()
    {
        try ( Neo4jControls controls = getTestBuilder( directory.directory() ).withDisabledServer().build() )
        {
            assertThrows( IllegalStateException.class, controls::httpURI );
            assertFalse( controls.httpsURI().isPresent() );

            assertDoesNotThrow( () ->
            {
                GraphDatabaseService service = controls.graph();
                try ( Transaction transaction = service.beginTx() )
                {
                    service.createNode();
                    transaction.success();
                }
            } );
        }
    }

    @Test
    void shouldFindFreePort()
    {
        // Given one server is running
        try ( Neo4jControls firstServer = getTestBuilder( directory.directory() ).build() )
        {
            // When I build a second server
            try ( Neo4jControls secondServer = getTestBuilder( directory.directory() ).build() )
            {
                // Then
                assertThat( secondServer.httpURI().getPort(), not( firstServer.httpURI().getPort() ) );
            }
        }
    }

    @Test
    void shouldRunBuilderOnExistingStoreDir()
    {
        // When
        // create graph db with one node upfront
        File existingStoreDir = directory.directory( "existingStore" );
        File storeDir = Config.defaults( GraphDatabaseSettings.data_directory, existingStoreDir.toPath().toString() )
                .get( GraphDatabaseSettings.database_path );
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            db.execute( "create ()" );
        }
        finally
        {
            db.shutdown();
        }

        try ( Neo4jControls controls = getTestBuilder( directory.databaseDir() ).copyFrom( existingStoreDir )
                .build() )
        {
            // Then
            try ( Transaction tx = controls.graph().beginTx() )
            {
                ResourceIterable<Node> allNodes = Iterables.asResourceIterable( controls.graph().getAllNodes() );

                assertTrue( Iterables.count( allNodes ) > 0 );

                // When: create another node
                controls.graph().createNode();
                tx.success();
            }
        }

        // Then: we still only have one node since the server is supposed to work on a copy
        db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, Iterables.count( db.getAllNodes() ) );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    void shouldOpenBoltPort()
    {
        // given
        try ( Neo4jControls controls = getTestBuilder( directory.directory() ).build() )
        {
            URI uri = controls.boltURI();

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
                assertThrows( RuntimeException.class, () -> getTestBuilder( directory.directory() ).copyFrom( notADirectory ).build() );
        Throwable cause = exception.getCause();
        assertTrue( cause instanceof IOException );
        assertTrue( cause.getMessage().contains( "exists but is not a directory" ) );
    }

    @Test
    void shouldReturnBoltUriWhenMultipleBoltConnectorsConfigured()
    {
        Neo4jBuilder serverBuilder = newInProcessBuilder( directory.directory() )
                .withConfig( "dbms.connector.another_bolt.type", "BOLT" )
                .withConfig( "dbms.connector.another_bolt.enabled", "true" )
                .withConfig( "dbms.connector.another_bolt.listen_address", ":0" )
                .withConfig( "dbms.connector.bolt.enabled", "true" )
                .withConfig( "dbms.connector.bolt.listen_address", ":0" );

        try ( Neo4jControls controls = serverBuilder.build() )
        {
            HostnamePort boltHostPort = connectorAddress( controls.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( controls.graph(), "another_bolt" );

            assertNotNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );
            assertNotEquals( boltHostPort, anotherBoltHostPort );

            URI boltUri = controls.boltURI();
            assertEquals( "bolt", boltUri.getScheme() );
            assertEquals( boltHostPort.getHost(), boltUri.getHost() );
            assertEquals( boltHostPort.getPort(), boltUri.getPort() );
        }
    }

    @Test
    void shouldReturnBoltUriWhenDefaultBoltConnectorOffAndOtherConnectorConfigured()
    {
        Neo4jBuilder serverBuilder = newInProcessBuilder( directory.directory() )
                .withConfig( "dbms.connector.bolt.enabled", "false" )
                .withConfig( "dbms.connector.another_bolt.type", "BOLT" )
                .withConfig( "dbms.connector.another_bolt.enabled", "true" )
                .withConfig( "dbms.connector.another_bolt.listen_address", ":0" );

        try ( Neo4jControls controls = serverBuilder.build() )
        {
            HostnamePort boltHostPort = connectorAddress( controls.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( controls.graph(), "another_bolt" );

            assertNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );

            URI boltUri = controls.boltURI();
            assertEquals( "bolt", boltUri.getScheme() );
            assertEquals( anotherBoltHostPort.getHost(), boltUri.getHost() );
            assertEquals( anotherBoltHostPort.getPort(), boltUri.getPort() );
        }
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
        Neo4jBuilder serverBuilder = newInProcessBuilder( directory.directory() )
                .withConfig( "dbms.connector.http.enabled", Boolean.toString( httpEnabled ) )
                .withConfig( "dbms.connector.http.listen_address", ":0" )
                .withConfig( "dbms.connector.https.enabled", Boolean.toString( httpsEnabled ) )
                .withConfig( "dbms.connector.https.listen_address", ":0" )
                .withConfig( "dbms.connector.bolt.enabled", Boolean.toString( boltEnabled ) )
                .withConfig( "dbms.connector.bolt.listen_address", ":0" );

        try ( Neo4jControls controls = serverBuilder.build() )
        {
            GraphDatabaseService db = controls.graph();

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
            db.createNode( label ).setProperty( propertyKey, propertyValue );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.findNodes( label ) );
            assertEquals( propertyValue, node.getProperty( propertyKey ) );
            tx.success();
        }
    }

    private void assertDBConfig( Neo4jControls server, String expected, String key ) throws JsonParseException
    {
        JsonNode beans = HTTP.GET(
                server.httpURI().toString() + "db/manage/server/jmx/domain/org.neo4j/" ).get( "beans" );
        JsonNode configurationBean = findNamedBean( beans, "Configuration" ).get( "attributes" );
        boolean foundKey = false;
        for ( JsonNode attribute : configurationBean )
        {
            if ( attribute.get( "name" ).asText().equals( key ) )
            {
                assertThat( attribute.get( "value" ).asText(), equalTo( expected ) );
                foundKey = true;
                break;
            }
        }
        if ( !foundKey )
        {
            fail( "No config key '" + key + "'." );
        }
    }

    private JsonNode findNamedBean( JsonNode beans, String beanName )
    {
        for ( JsonNode bean : beans )
        {
            JsonNode name = bean.get( "name" );
            if ( name != null && name.asText().endsWith( ",name=" + beanName ) )
            {
                return bean;
            }
        }
        throw new NoSuchElementException();
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
