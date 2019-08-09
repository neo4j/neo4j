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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.getDefaultRelativeProperties;
import static org.neo4j.server.ServerTestUtils.verifyConnector;
import static org.neo4j.test.assertion.Assert.assertEventually;

public abstract class BaseBootstrapperIT extends ExclusiveServerTestBase
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    protected ServerBootstrapper bootstrapper;

    @Before
    public void before() throws IOException
    {
        bootstrapper = newBootstrapper();
        SelfSignedCertificateFactory.create( testDirectory.storeDir() );
    }

    @After
    public void after()
    {
        if ( bootstrapper != null )
        {
            bootstrapper.stop();
        }
    }

    protected abstract ServerBootstrapper newBootstrapper();

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles() throws Throwable
    {
        // When
        int resultCode = ServerBootstrapper.start( bootstrapper, withConnectorsOnRandomPortsConfig( getAdditionalArguments() ) );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    protected String[] getAdditionalArguments() throws IOException
    {
        return new String[]{"--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "-c", configOption( data_directory, testDirectory.storeDir().getAbsolutePath() ),
                "-c", configOption( logs_directory, testDirectory.storeDir().getAbsolutePath() )};
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String,String> properties = stringMap( forced_kernel_id.name(), "ourcustomvalue" );
        properties.putAll( getDefaultRelativeProperties( testDirectory.storeDir() ) );
        properties.putAll( connectorsOnRandomPortsConfig() );

        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper,
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String,String> properties = stringMap( forced_kernel_id.name(), "thisshouldnotshowup" );
        properties.putAll( getDefaultRelativeProperties( testDirectory.storeDir() ) );
        properties.putAll( connectorsOnRandomPortsConfig() );

        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper,
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath(),
                "-c", configOption( forced_kernel_id, "mycustomvalue" ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "mycustomvalue" ) );
    }

    @Test
    public void shouldStartWithHttpHttpsAndBoltDisabled() throws Exception
    {
        testStartupWithConnectors( false, false, false );
    }

    @Test
    public void shouldStartWithHttpEnabledAndHttpsBoltDisabled() throws Exception
    {
        testStartupWithConnectors( true, false, false );
    }

    @Test
    public void shouldStartWithHttpsEnabledAndHttpBoltDisabled() throws Exception
    {
        testStartupWithConnectors( false, true, false );
    }

    @Test
    public void shouldStartWithBoltEnabledAndHttpHttpsDisabled() throws Exception
    {
        testStartupWithConnectors( false, false, true );
    }

    @Test
    public void shouldStartWithHttpHttpsEnabledAndBoltDisabled() throws Exception
    {
        testStartupWithConnectors( true, true, false );
    }

    @Test
    public void shouldStartWithHttpBoltEnabledAndHttpsDisabled() throws Exception
    {
        testStartupWithConnectors( true, false, true );
    }

    @Test
    public void shouldStartWithHttpsBoltEnabledAndHttpDisabled() throws Exception
    {
        testStartupWithConnectors( false, true, true );
    }

    private void testStartupWithConnectors( boolean httpEnabled, boolean httpsEnabled, boolean boltEnabled ) throws Exception
    {
        PemSslPolicyConfig httpsPolicy = PemSslPolicyConfig.forScope( SslPolicyScope.HTTPS );
        if ( httpsEnabled )
        {
            //create self signed
            SelfSignedCertificateFactory.create( testDirectory.storeDir().getAbsoluteFile() );
        }

        int resultCode = ServerBootstrapper.start( bootstrapper,
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "-c", configOption( data_directory, testDirectory.storeDir().getAbsolutePath() ),
                "-c", configOption( logs_directory, testDirectory.storeDir().getAbsolutePath() ),
                "-c", httpsEnabled ? configOption( httpsPolicy.base_directory, testDirectory.storeDir().getAbsolutePath() ) : "",

                "-c", HttpConnector.enabled.name() + "=" + httpEnabled,
                "-c", HttpConnector.listen_address.name() + "=localhost:0",

                "-c", HttpsConnector.enabled.name() + "=" + httpsEnabled,
                "-c", HttpsConnector.listen_address.name() + "=localhost:0",

                "-c", BoltConnector.enabled.name() + "=" + boltEnabled,
                "-c", BoltConnector.listen_address.name() + "=localhost:0"
        );

        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        assertDbAccessibleAsEmbedded();

        verifyConnector( db(), HttpConnector.NAME, httpEnabled );
        verifyConnector( db(), HttpsConnector.NAME, httpsEnabled );
        verifyConnector( db(), BoltConnector.NAME, boltEnabled );
    }

    protected String configOption( Setting<?> setting, String value )
    {
        return setting.name() + "=" + value;
    }

    protected static String[] withConnectorsOnRandomPortsConfig( String... otherConfigs )
    {
        Stream<String> configs = Stream.of( otherConfigs );

        Stream<String> connectorsConfig = connectorsOnRandomPortsConfig().entrySet()
                .stream()
                .map( entry -> entry.getKey() + "=" + entry.getValue() )
                .flatMap( config -> Stream.of( "-c", config ) );

        return Stream.concat( configs, connectorsConfig ).toArray( String[]::new );
    }

    protected static Map<String,String> connectorsOnRandomPortsConfig()
    {
        return stringMap(
                HttpConnector.listen_address.name(), "localhost:0",
                HttpConnector.enabled.name(), TRUE,

                HttpsConnector.listen_address.name(), "localhost:0",
                HttpsConnector.enabled.name(), FALSE,

                BoltConnector.listen_address.name(), "localhost:0",
                BoltConnector.encryption_level.name(), "DISABLED",
                BoltConnector.enabled.name(), TRUE
        );
    }

    private void assertDbAccessibleAsEmbedded()
    {
        GraphDatabaseAPI db = db();

        Label label = () -> "Node";
        String propertyKey = "key";
        String propertyValue = "value";

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( propertyKey, propertyValue );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.findNodes( label ) );
            assertEquals( propertyValue, node.getProperty( propertyKey ) );
            tx.commit();
        }
    }

    private GraphDatabaseAPI db()
    {
        return bootstrapper.getServer().getDatabaseService().getDatabase();
    }
}
