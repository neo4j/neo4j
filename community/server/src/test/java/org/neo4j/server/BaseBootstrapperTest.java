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
package org.neo4j.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;

import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.CommunityBootstrapper.start;
import static org.neo4j.server.web.ServerInternalSettings.legacy_db_location;

public abstract class BaseBootstrapperTest extends ExclusiveServerTestBase
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    protected Bootstrapper bootstrapper;

    protected String[] commandLineConfig( String... params )
    {
        ArrayList<String> config = new ArrayList<>();

        for ( String param : params )
        {
            config.add( param );
        }

        return config.toArray( new String[config.size()] );
    }

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

    protected abstract Bootstrapper newBootstrapper();

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles()
    {
        // When
        int resultCode = start( bootstrapper, commandLineConfig( "-c", configOption( legacy_db_location.name(), tempDir.getRoot().getAbsolutePath() ) ) );

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( "neo4j.config" );

        store( stringMap(
                forced_kernel_id.name(), "ourcustomvalue"
        ), configFile );

        // When
        start( bootstrapper, commandLineConfig(
                "-C", configFile.getAbsolutePath(),
                "-c", configOption( legacy_db_location.name(), tempDir.getRoot().getAbsolutePath() ) ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        File configFile = tempDir.newFile( "neo4j.config" );

        store( stringMap(
                forced_kernel_id.name(), "thisshouldnotshowup"
        ), configFile );

        // When
        start( bootstrapper, commandLineConfig(
                "-C", configFile.getAbsolutePath(),
                "-c", configOption( forced_kernel_id.name(), "mycustomvalue" ),
                "-c", configOption( legacy_db_location.name(), tempDir.getRoot().getAbsolutePath() ) ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "mycustomvalue" ) );
    }

    protected String configOption( String key, String value )
    {
        return key + "=" + value;
    }
}
