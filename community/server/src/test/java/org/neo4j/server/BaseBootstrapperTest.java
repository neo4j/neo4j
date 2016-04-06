/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;
import static org.neo4j.test.Assert.assertEventually;

public abstract class BaseBootstrapperTest extends ExclusiveServerTestBase
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    protected ServerBootstrapper bootstrapper;

    @Before
    public void before() throws IOException
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

    protected abstract ServerBootstrapper newBootstrapper() throws IOException;

    protected abstract void start( String[] args );

    protected abstract void stop( String[] args );

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles() throws IOException
    {
        // When
        int resultCode = ServerBootstrapper.start( bootstrapper,
                "-c", configOption( data_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( tls_certificate_file,
                        new File( tempDir.getRoot(), "cert.cert" ).getAbsolutePath() ),
                "-c", configOption( tls_key_file, new File( tempDir.getRoot(), "key.key" ).getAbsolutePath() ),
                "-c", "dbms.connector.1.type=HTTP",
                "-c", "dbms.connector.1.enabled=true"
        );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( ConfigLoader.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "ourcustomvalue" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.enabled", "true" );
        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper, "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( ConfigLoader.DEFAULT_CONFIG_FILE_NAME);

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "thisshouldnotshowup" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.enabled", "true" );
        store( properties, configFile );

        // When
        ServerBootstrapper.start( bootstrapper,
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
