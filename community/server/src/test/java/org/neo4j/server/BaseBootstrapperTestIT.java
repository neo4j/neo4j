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
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.ServerSettings.script_enabled;
import static org.neo4j.test.assertion.Assert.assertEventually;

public abstract class BaseBootstrapperTestIT extends ExclusiveServerTestBase
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    protected ServerBootstrapper bootstrapper;

    @Before
    public void before()
    {
        bootstrapper = newBootstrapper();
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
        int resultCode = ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "-c", configOption( data_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                // The `script_enabled=true` setting is needed because the global javascript context must be
                // initialised in sandboxed mode to allow testing traversal endpoint scripting:
                "-c", configOption( script_enabled, Settings.TRUE ),
                "-c", "dbms.connector.https.listen_address=localhost:0",
                "-c", "dbms.connector.http.type=HTTP",
                "-c", "dbms.connector.http.enabled=true",
                "-c", "dbms.connector.http.listen_address=localhost:" + PortAuthority.allocatePort(),
                "-c", "dbms.connector.https.type=HTTP",
                "-c", "dbms.connector.https.enabled=true",
                "-c", "dbms.connector.https.listen_address=localhost:" + PortAuthority.allocatePort(),
                "-c", new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name() + "=localhost:" + PortAuthority.allocatePort(),
                "-c", "dbms.backup.enabled=false"
        );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "ourcustomvalue" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.http.type", "HTTP" );
        properties.put( "dbms.connector.http.enabled", "true" );
        properties.put( "dbms.connector.http.listen_address", "localhost:0" );
        properties.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );

        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME);

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "thisshouldnotshowup" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.http.type", "HTTP" );
        properties.put( "dbms.connector.http.enabled", "true" );
        properties.put( "dbms.connector.http.listen_address", "localhost:0" );
        properties.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );

        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath(),
                "-c", configOption( forced_kernel_id, "mycustomvalue" ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "mycustomvalue" ) );
    }

    protected String configOption( Setting<?> setting, String value )
    {
        return setting.name() + "=" + value;
    }
}
