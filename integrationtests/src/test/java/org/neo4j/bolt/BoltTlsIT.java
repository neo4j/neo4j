/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.ssl.SecureClient;
import org.neo4j.ssl.SslContextFactory;
import org.neo4j.ssl.SslResource;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.ssl.SslContextFactory.SslParameters.protocols;
import static org.neo4j.ssl.SslContextFactory.makeSslContext;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;

@RunWith( Parameterized.class )
public class BoltTlsIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private SslPolicyConfig sslPolicy = new SslPolicyConfig( "bolt" );

    private GraphDatabaseAPI db;
    private SslResource sslResource;

    private BoltConnector bolt = new BoltConnector( "bolt" );

    @Before
    public void setup() throws IOException
    {
        File sslObjectsDir = new File( testDirectory.graphDbDir(), "certificates" );
        assertTrue( sslObjectsDir.mkdirs() );

        sslResource = selfSignedKeyId( 0 ).trustKeyId( 0 ).install( sslObjectsDir );

        createAndStartDb();
    }

    static class TestSetup
    {
        private final String clientTlsVersions;
        private final String boltTlsVersions;
        private final boolean shouldSucceed;

        TestSetup( String clientTlsVersions, String boltTlsVersion, boolean shouldSucceed )
        {
            this.clientTlsVersions = clientTlsVersions;
            this.boltTlsVersions = boltTlsVersion;
            this.shouldSucceed = shouldSucceed;
        }

        @Override
        public String toString()
        {
            return "TestSetup{"
                    + "clientTlsVersions='" + clientTlsVersions + '\''
                    + ", boltTlsVersions='" + boltTlsVersions + '\''
                    + ", shouldSucceed=" + shouldSucceed + '}';
        }
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Object[] params()
    {
        return new TestSetup[]{
                new TestSetup( "TLSv1.1", "TLSv1.2", false ),
                new TestSetup( "TLSv1.2", "TLSv1.1", false ),
                new TestSetup( "TLSv1", "TLSv1.1", false ),
                new TestSetup( "TLSv1.1", "TLSv1.2", false ),

                new TestSetup( "TLSv1", "TLSv1", true ),
                new TestSetup( "TLSv1.1", "TLSv1.1", true ),
                new TestSetup( "TLSv1.2", "TLSv1.2", true ),

                new TestSetup( "SSLv3,TLSv1", "TLSv1.1,TLSv1.2", false ),
                new TestSetup( "TLSv1.1,TLSv1.2", "TLSv1.1,TLSv1.2", true ),
        };
    }

    @Parameter()
    public TestSetup setup;

    private void createAndStartDb()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( bolt.enabled, "true" )
                .setConfig( bolt.listen_address, ":" + PortAuthority.allocatePort() )
                .setConfig( BoltKernelExtension.Settings.ssl_policy, "bolt" )
                .setConfig( sslPolicy.allow_key_generation, "true" )
                .setConfig( sslPolicy.base_directory, "certificates" )
                .setConfig( sslPolicy.tls_versions, setup.boltTlsVersions )
                .setConfig( sslPolicy.client_auth, "none" )
                .newGraphDatabase();
    }

    @After
    public void teardown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldRespectProtocolSelection() throws Exception
    {
        // given
        Config config = db.getDependencyResolver().resolveDependency( Config.class );
        int boltPort = config.get( bolt.advertised_address ).getPort();
        SslContextFactory.SslParameters params = protocols( setup.clientTlsVersions ).ciphers();
        SecureClient client = new SecureClient( makeSslContext( sslResource, false, params ) );

        // when
        client.connect( boltPort );

        // then
        assertTrue( client.sslHandshakeFuture().await( 1, MINUTES ) );

        if ( setup.shouldSucceed )
        {
            assertNull( client.sslHandshakeFuture().cause() );
        }
        else
        {
            assertNotNull( client.sslHandshakeFuture().cause() );
        }
    }
}
