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
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.NoSuchElementException;
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
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.ssl.ClientAuth;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.harness.TestServerBuilders.newInProcessBuilder;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.server.ServerTestUtils.connectorAddress;
import static org.neo4j.server.ServerTestUtils.verifyConnector;

public class InProcessServerBuilderIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldLaunchAServerInSpecifiedDirectory()
    {
        // Given
        File workDir = testDir.directory( "specific" );

        // When
        try ( ServerControls server = getTestServerBuilder( workDir ).newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo( 1 ) );
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    private TestServerBuilder getTestServerBuilder( File workDir )
    {
        return newInProcessBuilder( workDir );
    }

    @Test
    public void shouldAllowCustomServerAndDbConfig() throws Exception
    {
        // Given
        trustAllSSLCerts();

        // Get default trusted cypher suites
        SSLServerSocketFactory ssf = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        String[] defaultCiphers = ssf.getDefaultCipherSuites();

        // When
        HttpConnector httpConnector = new HttpConnector( "0", Encryption.NONE );
        HttpConnector httpsConnector = new HttpConnector( "1", Encryption.TLS );
        try ( ServerControls server = getTestServerBuilder( testDir.directory() )
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
                .withConfig( "dbms.ssl.policy.test.base_directory", testDir.directory( "certificates" ).getAbsolutePath() )
                .withConfig( "dbms.ssl.policy.test.allow_key_generation", "true" )
                .withConfig( "dbms.ssl.policy.test.ciphers", String.join( ",", defaultCiphers ) )
                .withConfig( "dbms.ssl.policy.test.tls_versions", "TLSv1.2, TLSv1.1, TLSv1" )
                .withConfig( "dbms.ssl.policy.test.client_auth", ClientAuth.NONE.name() )
                .withConfig( "dbms.ssl.policy.test.trust_all", "true" )
                .newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( HTTP.GET( server.httpsURI().get().toString() ).status(), equalTo( 200 ) );
            assertDBConfig( server, "20", GraphDatabaseSettings.dense_node_threshold.name() );
        }
    }

    @Test
    public void shouldMountUnmanagedExtensionsByClass()
    {
        // When
        try ( ServerControls server = getTestServerBuilder( testDir.directory() )
                .withExtension( "/path/to/my/extension", MyUnmanagedExtension.class )
                .newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    public void shouldMountUnmanagedExtensionsByPackage()
    {
        // When
        try ( ServerControls server = getTestServerBuilder( testDir.directory() )
                .withExtension( "/path/to/my/extension", "org.neo4j.harness.extensionpackage" )
                .newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    public void shouldFindFreePort()
    {
        // Given one server is running
        try ( ServerControls firstServer = getTestServerBuilder( testDir.directory() ).newServer() )
        {
            // When I start a second server
            try ( ServerControls secondServer = getTestServerBuilder( testDir.directory() ).newServer() )
            {
                // Then
                assertThat( secondServer.httpURI().getPort(), not( firstServer.httpURI().getPort() ) );
            }
        }
    }

    @Test
    public void shouldRunBuilderOnExistingStoreDir() throws Exception
    {
        // When
        // create graph db with one node upfront
        File existingStoreDir = testDir.directory( "existingStore" );
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

        try ( ServerControls server = getTestServerBuilder( testDir.databaseDir() ).copyFrom( existingStoreDir )
                .newServer() )
        {
            // Then
            try ( Transaction tx = server.graph().beginTx() )
            {
                ResourceIterable<Node> allNodes = Iterables.asResourceIterable( server.graph().getAllNodes() );

                assertTrue( Iterables.count( allNodes ) > 0 );

                // When: create another node
                server.graph().createNode();
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
    public void shouldOpenBoltPort() throws Throwable
    {
        // given
        try ( ServerControls controls = getTestServerBuilder( testDir.directory() ).newServer() )
        {
            URI uri = controls.boltURI();

            // when
            new SocketConnection().connect( new HostnamePort( uri.getHost(), uri.getPort() ) );

            // then no exception
        }
    }

    @Test
    public void shouldFailWhenProvidingANonDirectoryAsSource() throws IOException
    {

        File notADirectory = File.createTempFile( "prefix", "suffix" );
        assertFalse( notADirectory.isDirectory() );

        try ( ServerControls ignored = getTestServerBuilder( testDir.directory() )
                .copyFrom( notADirectory ).newServer() )
        {
            fail( "server should not start" );
        }
        catch ( RuntimeException rte )
        {
            Throwable cause = rte.getCause();
            assertTrue( cause instanceof IOException );
            assertTrue( cause.getMessage().contains( "exists but is not a directory" ) );
        }

    }

    @Test
    public void shouldReturnBoltUriWhenMultipleBoltConnectorsConfigured()
    {
        TestServerBuilder serverBuilder = newInProcessBuilder( testDir.directory() )
                .withConfig( "dbms.connector.another_bolt.type", "BOLT" )
                .withConfig( "dbms.connector.another_bolt.enabled", "true" )
                .withConfig( "dbms.connector.another_bolt.listen_address", ":0" )
                .withConfig( "dbms.connector.bolt.enabled", "true" )
                .withConfig( "dbms.connector.bolt.listen_address", ":0" );

        try ( ServerControls server = serverBuilder.newServer() )
        {
            HostnamePort boltHostPort = connectorAddress( server.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( server.graph(), "another_bolt" );

            assertNotNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );
            assertNotEquals( boltHostPort, anotherBoltHostPort );

            URI boltUri = server.boltURI();
            assertEquals( "bolt", boltUri.getScheme() );
            assertEquals( boltHostPort.getHost(), boltUri.getHost() );
            assertEquals( boltHostPort.getPort(), boltUri.getPort() );
        }
    }

    @Test
    public void shouldReturnBoltUriWhenDefaultBoltConnectorOffAndOtherConnectorConfigured()
    {
        TestServerBuilder serverBuilder = newInProcessBuilder( testDir.directory() )
                .withConfig( "dbms.connector.bolt.enabled", "false" )
                .withConfig( "dbms.connector.another_bolt.type", "BOLT" )
                .withConfig( "dbms.connector.another_bolt.enabled", "true" )
                .withConfig( "dbms.connector.another_bolt.listen_address", ":0" );

        try ( ServerControls server = serverBuilder.newServer() )
        {
            HostnamePort boltHostPort = connectorAddress( server.graph(), "bolt" );
            HostnamePort anotherBoltHostPort = connectorAddress( server.graph(), "another_bolt" );

            assertNull( boltHostPort );
            assertNotNull( anotherBoltHostPort );

            URI boltUri = server.boltURI();
            assertEquals( "bolt", boltUri.getScheme() );
            assertEquals( anotherBoltHostPort.getHost(), boltUri.getHost() );
            assertEquals( anotherBoltHostPort.getPort(), boltUri.getPort() );
        }
    }

    @Test
    public void shouldStartServerWithHttpHttpsAndBoltDisabled()
    {
        testStartupWithConnectors( false, false, false );
    }

    @Test
    public void shouldStartServerWithHttpEnabledAndHttpsBoltDisabled()
    {
        testStartupWithConnectors( true, false, false );
    }

    @Test
    public void shouldStartServerWithHttpsEnabledAndHttpBoltDisabled()
    {
        testStartupWithConnectors( false, true, false );
    }

    @Test
    public void shouldStartServerWithBoltEnabledAndHttpHttpsDisabled()
    {
        testStartupWithConnectors( false, false, true );
    }

    @Test
    public void shouldStartServerWithHttpHttpsEnabledAndBoltDisabled()
    {
        testStartupWithConnectors( true, true, false );
    }

    @Test
    public void shouldStartServerWithHttpBoltEnabledAndHttpsDisabled()
    {
        testStartupWithConnectors( true, false, true );
    }

    @Test
    public void shouldStartServerWithHttpsBoltEnabledAndHttpDisabled()
    {
        testStartupWithConnectors( false, true, true );
    }

    private void testStartupWithConnectors( boolean httpEnabled, boolean httpsEnabled, boolean boltEnabled )
    {
        TestServerBuilder serverBuilder = newInProcessBuilder( testDir.directory() )
                .withConfig( "dbms.connector.http.enabled", Boolean.toString( httpEnabled ) )
                .withConfig( "dbms.connector.http.listen_address", ":0" )
                .withConfig( "dbms.connector.https.enabled", Boolean.toString( httpsEnabled ) )
                .withConfig( "dbms.connector.https.listen_address", ":0" )
                .withConfig( "dbms.connector.bolt.enabled", Boolean.toString( boltEnabled ) )
                .withConfig( "dbms.connector.bolt.listen_address", ":0" );

        try ( ServerControls server = serverBuilder.newServer() )
        {
            GraphDatabaseService db = server.graph();

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

    private void assertDBConfig( ServerControls server, String expected, String key ) throws JsonParseException
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
}
