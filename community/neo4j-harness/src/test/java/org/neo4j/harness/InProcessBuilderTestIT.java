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

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.server.configuration.ServerSettings;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.harness.TestServerBuilders.newInProcessBuilder;

public class InProcessBuilderTestIT
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
        return newInProcessBuilder( workDir ).withConfig( ServerSettings.script_enabled, Settings.TRUE );
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
                .withConfig( httpConnector.listen_address, "localhost:" + PortAuthority.allocatePort())
                .withConfig( httpsConnector.type, "HTTP" )
                .withConfig( httpsConnector.enabled, "true" )
                .withConfig( httpsConnector.encryption, "TLS" )
                .withConfig( httpsConnector.listen_address, "localhost:" + PortAuthority.allocatePort() )
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
        Path dir = Files.createTempDirectory( getClass().getSimpleName() + "_shouldRunBuilderOnExistingStorageDir" );
        File storeDir = Config.defaults( GraphDatabaseSettings.data_directory, dir.toString() )
                .get( GraphDatabaseSettings.database_path );
        try
        {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            try
            {
                db.execute( "create ()" );
            }
            finally
            {
                db.shutdown();
            }

            try ( ServerControls server = getTestServerBuilder( testDir.directory() ).copyFrom( dir.toFile() )
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
        finally
        {
            FileUtils.forceDelete( dir.toFile() );
        }
    }

    @Test
    public void shouldOpenBoltPort() throws Throwable
    {
        // given
        try ( ServerControls controls = getTestServerBuilder( testDir.graphDbDir() ).newServer() )
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

        try ( ServerControls ignored = getTestServerBuilder( testDir.graphDbDir() )
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
