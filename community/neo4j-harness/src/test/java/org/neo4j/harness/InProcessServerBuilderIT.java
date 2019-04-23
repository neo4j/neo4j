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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpConnector.Encryption;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.harness.junit.Neo4j;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_TX_LOGS_ROOT_DIR_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.configuration.ssl.BaseSslPolicyConfig.Format.PEM;
import static org.neo4j.harness.internal.TestNeo4jBuilders.newInProcessBuilder;
import static org.neo4j.helpers.collection.Iterables.asIterable;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.server.ServerTestUtils.connectorAddress;
import static org.neo4j.server.ServerTestUtils.verifyConnector;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

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
        try ( InProcessNeo4j neo4j = getTestBuilder( workDir ).build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status(), equalTo( 200 ) );
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
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() )
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
                .withConfig( "dbms.ssl.policy.test.format", PEM.name() )
                .withConfig( "dbms.ssl.policy.test.allow_key_generation", "true" )
                .withConfig( "dbms.ssl.policy.test.ciphers", String.join( ",", defaultCiphers ) )
                .withConfig( "dbms.ssl.policy.test.tls_versions", "TLSv1.2, TLSv1.1, TLSv1" )
                .withConfig( "dbms.ssl.policy.test.client_auth", ClientAuth.NONE.name() )
                .withConfig( "dbms.ssl.policy.test.trust_all", "true" )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( HTTP.GET( neo4j.httpsURI().toString() ).status(), equalTo( 200 ) );
            assertDBConfig( neo4j, "20", GraphDatabaseSettings.dense_node_threshold.name() );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByClass()
    {
        // When
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() )
                .withUnmanagedExtension( "/path/to/my/extension", MyUnmanagedExtension.class )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    void shouldMountUnmanagedExtensionsByPackage()
    {
        // When
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() )
                .withUnmanagedExtension( "/path/to/my/extension", "org.neo4j.harness.extensionpackage" )
                .build() )
        {
            // Then
            assertThat( HTTP.GET( neo4j.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    void startWithCustomExtension()
    {
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() )
                .withExtensionFactories( asIterable( new TestExtensionFactory() ) ).build() )
        {
            assertThat( HTTP.GET( neo4j.httpURI().toString() ).status(), equalTo( 200 ) );
            assertEquals( 1, TestExtension.getStartCounter() );
        }
    }

    @Test
    void startWithDisabledServer()
    {
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() ).withDisabledServer().build() )
        {
            assertThrows( IllegalStateException.class, neo4j::httpURI );
            assertThrows( IllegalStateException.class, neo4j::httpsURI );

            assertDoesNotThrow( () ->
            {
                GraphDatabaseService service = neo4j.graph();
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
        try ( InProcessNeo4j firstServer = getTestBuilder( directory.directory() ).build() )
        {
            // When I build a second server
            try ( InProcessNeo4j secondServer = getTestBuilder( directory.directory() ).build() )
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
        Config config = Config.defaults( data_directory, existingStoreDir.toPath().toString() );
        File rootDirectory = config.get( databases_root_path );
        DatabaseManagementServiceBuilder databaseBuilder = new TestDatabaseManagementServiceBuilder( rootDirectory )
                .setConfig( transaction_logs_root_path, new File( existingStoreDir, DEFAULT_TX_LOGS_ROOT_DIR_NAME ).getAbsolutePath() );
        DatabaseManagementService managementService = databaseBuilder.build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            db.execute( "create ()" );
        }
        finally
        {
            managementService.shutdown();
        }

        try ( InProcessNeo4j neo4j = getTestBuilder( directory.storeDir() ).copyFrom( existingStoreDir ).build() )
        {
            // Then
            try ( Transaction tx = neo4j.graph().beginTx() )
            {
                ResourceIterable<Node> allNodes = Iterables.asResourceIterable( neo4j.graph().getAllNodes() );

                assertTrue( Iterables.count( allNodes ) > 0 );

                // When: create another node
                neo4j.graph().createNode();
                tx.success();
            }
        }

        // Then: we still only have one node since the server is supposed to work on a copy
        managementService = databaseBuilder.build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
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
            managementService.shutdown();
        }
    }

    @Test
    void shouldOpenBoltPort()
    {
        // given
        try ( InProcessNeo4j neo4j = getTestBuilder( directory.directory() ).build() )
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

        try ( InProcessNeo4j neo4j = serverBuilder.build() )
        {
            HostnamePort boltHostPort = connectorAddress( neo4j.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( neo4j.graph(), "another_bolt" );

            assertNotNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );
            assertNotEquals( boltHostPort, anotherBoltHostPort );

            URI boltUri = neo4j.boltURI();
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

        try ( InProcessNeo4j neo4j = serverBuilder.build() )
        {
            HostnamePort boltHostPort = connectorAddress( neo4j.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( neo4j.graph(), "another_bolt" );

            assertNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );

            URI boltUri = neo4j.boltURI();
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

        try ( InProcessNeo4j neo4j = serverBuilder.build() )
        {
            GraphDatabaseService db = neo4j.graph();

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

    private void assertDBConfig( Neo4j server, String expected, String key )
    {
        String query = "CALL dbms.queryJmx('org.neo4j:*,name=Configuration') YIELD attributes WITH attributes['" + key +
                "']['value'] AS value RETURN CASE WHEN value = '" + expected + "' THEN 'SUCCESS' ELSE 'FAIL' END AS response";

        HTTP.Response response =
                HTTP.POST( server.httpURI().toString() + "db/data/transaction/commit", rawPayload( "{\"statements\":[ {\"statement\": \"" + query + "\"}]}" ) );

        assertThat( response.rawContent(), containsString( "SUCCESS" ) );
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
