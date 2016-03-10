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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_store;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.Bootstrapper.start;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;
import static org.neo4j.test.Assert.assertEventually;

public abstract class BaseBootstrapperTest extends ExclusiveServerTestBase
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    protected Bootstrapper bootstrapper;

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

    protected abstract Bootstrapper newBootstrapper() throws IOException;

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles() throws IOException
    {
        // When
        int resultCode = start( bootstrapper, commandLineConfig(
                "-c", configOption( data_directory.name(), tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( auth_store.name(), tempDir.newFile().getAbsolutePath() ),
                "-c", configOption( tls_certificate_file.name(), new File( tempDir.getRoot(), "cert.cert" )
                        .getAbsolutePath() ),
                "-c", configOption( tls_key_file.name(), new File( tempDir.getRoot(), "key.key" ).getAbsolutePath() )
        ) );

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( "neo4j.config" );

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "ourcustomvalue" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        store( properties, configFile );

        // When
        start( bootstrapper, commandLineConfig( "-C", configFile.getAbsolutePath() ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( "neo4j.config" );

        Map<String, String> properties = stringMap( forced_kernel_id.name(), "thisshouldnotshowup" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        store( properties, configFile );

        // When
        start( bootstrapper, commandLineConfig(
                "-C", configFile.getAbsolutePath(),
                "-c", configOption( forced_kernel_id.name(), "mycustomvalue" ) ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "mycustomvalue" ) );
    }

    @Test
    public void shouldStopTheServerWhenTheRunFileIsDeleted() throws InterruptedException
    {
        File runFile = new File( tempDir.getRoot(), ".run-file" );
        AtomicBoolean exited = new AtomicBoolean( false );

        new Thread()
        {
            @Override
            public void run()
            {
                Bootstrapper.start( bootstrapper, commandLineConfig( "--run-file", runFile.getAbsolutePath() ) );
                exited.set( true );
            }
        }.start();

        assertEventually( "Run-file was not created", runFile::exists, is( true ), 1, TimeUnit.MINUTES );
        if ( !runFile.delete() )
        {
            throw new RuntimeException( "Could not delete " + runFile );
        }
        assertEventually( "Bootstrapper did not exit", exited::get, is( true ), 1, TimeUnit.MINUTES );
        assertThat( "Server was not stopped", bootstrapper.getServer().getDatabase().isRunning(), is( false ) );
    }

    protected String[] commandLineConfig( String... params )
    {
        ArrayList<String> config = new ArrayList<>();
        Collections.addAll( config, params );
        return config.toArray( new String[config.size()] );
    }

    protected String configOption( String key, String value )
    {
        return key + "=" + value;
    }
}
