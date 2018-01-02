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
package org.neo4j.harness;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.NoSuchElementException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.server.HTTP;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.harness.TestServerBuilders.newInProcessBuilder;

public class InProcessBuilderTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( InProcessBuilderTest.class );

    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void shouldLaunchAServerInSpecifiedDirectory() throws Exception
    {
        // Given
        File workDir = new File(testDir.directory(), "specific");
        workDir.mkdir();

        // When
        try(ServerControls server = newInProcessBuilder( workDir ).newServer())
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() ).status(), equalTo( 200 ) );
            assertThat( workDir.list().length, equalTo(1));
        }

        // And after it's been closed, it should've cleaned up after itself.
        assertThat( Arrays.toString( workDir.list() ), workDir.list().length, equalTo( 0 ) );
    }

    @Test
    public void shouldAllowCustomServerAndDbConfig() throws Exception
    {
        // Given
        trustAllSSLCerts();

        // When
        try ( ServerControls server = newInProcessBuilder( testDir.directory() )
                .withConfig( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY, "true")
                .withConfig( ServerSettings.tls_certificate_file.name(), testDir.file( "cert" ).getAbsolutePath() )
                .withConfig( Configurator.WEBSERVER_KEYSTORE_PATH_PROPERTY_KEY, testDir.file( "keystore" ).getAbsolutePath() )
                .withConfig( ServerSettings.tls_key_file.name(), testDir.file( "key" ).getAbsolutePath() )
                .withConfig( GraphDatabaseSettings.dense_node_threshold, "20" )
                .newServer() )
        {
            // Then
            assertThat( HTTP.GET( server.httpsURI().toString() ).status(), equalTo( 200 ) );
            assertDBConfig( server, "20", GraphDatabaseSettings.dense_node_threshold.name() );
        }
    }

    @Test
    public void shouldMountUnmanagedExtensionsByClass() throws Exception
    {
        // When
        try(ServerControls server = newInProcessBuilder( testDir.directory() )
                .withExtension( "/path/to/my/extension", MyUnmanagedExtension.class )
                .newServer())
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    public void shouldMountUnmanagedExtensionsByPackage() throws Exception
    {
        // When
        try(ServerControls server = newInProcessBuilder( testDir.directory() )
                .withExtension( "/path/to/my/extension", "org.neo4j.harness.extensionpackage" )
                .newServer())
        {
            // Then
            assertThat( HTTP.GET( server.httpURI().toString() + "path/to/my/extension/myExtension" ).status(),
                    equalTo( 234 ) );
        }
    }

    @Test
    public void shouldFindFreePort() throws Exception
    {
        // Given one server is running
        try(ServerControls firstServer = newInProcessBuilder( testDir.directory() ).newServer())
        {
            // When I start a second server
            try(ServerControls secondServer = newInProcessBuilder( testDir.directory() ).newServer())
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
        Path dir = Files.createTempDirectory( getClass().getSimpleName() +
                "_shouldRunBuilderOnExistingStorageDir" );
        try
        {

            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.toString() );
            try
            {
                db.execute( "create ()" );
            }
            finally
            {
                db.shutdown();
            }

            try ( ServerControls server = newInProcessBuilder( testDir.directory() ).copyFrom( dir.toFile() )
                    .newServer() )
            {
                // Then
                try ( Transaction tx = server.graph().beginTx() )
                {
                    ResourceIterable<Node> allNodes = GlobalGraphOperations.at(
                            server.graph() ).getAllNodes();

                    assertTrue( IteratorUtil.count( allNodes ) > 0 );

                    // When: create another node
                    server.graph().createNode();
                    tx.success();
                }
            }

            // Then: we still only have one node since the server is supposed to work on a copy
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.toString() );
            try
            {
                try ( Transaction tx = db.beginTx() )
                {
                    assertEquals( 1, IteratorUtil.count( GlobalGraphOperations.at( db ).getAllNodes() ) );
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
    public void shouldFailWhenProvidingANonDirectoryAsSource() throws IOException
    {

        File notADirectory = File.createTempFile( "prefix", "suffix" );
        assertFalse( notADirectory.isDirectory() );

        try ( ServerControls server = newInProcessBuilder( ).copyFrom( notADirectory )
                .newServer() )
        {
            fail("server should not start");
        } catch (RuntimeException rte) {
            Throwable cause = rte.getCause();
            assertTrue( cause instanceof IOException);
            assertTrue( cause.getMessage().contains( "exists but is not a directory" ));
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
            if(attribute.get("name").asText().equals( key ))
            {
                assertThat(attribute.get("value").asText(), equalTo( expected ));
                foundKey = true;
                break;
            }
        }
        if(!foundKey)
        {
            fail("No config key '" + key + "'.");
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
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager()
        {
            @Override
            public void checkClientTrusted( X509Certificate[] arg0, String arg1 )
                    throws CertificateException
            {
            }

            @Override
            public void checkServerTrusted( X509Certificate[] arg0, String arg1 )
                    throws CertificateException
            {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
        }};

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance( "TLS" );
        sc.init( null, trustAllCerts, new SecureRandom() );
        HttpsURLConnection.setDefaultSSLSocketFactory( sc.getSocketFactory() );
    }
}
