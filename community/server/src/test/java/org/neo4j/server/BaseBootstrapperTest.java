/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.CommunityBootstrapper.start;

public abstract class BaseBootstrapperTest extends ExclusiveServerTestBase
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Bootstrapper bootstrapper;

    protected abstract Bootstrapper newBootstrapper() throws IOException;

    @Test
    public void shouldStartStopNeoServerWithoutAnyConfigFiles() throws IOException
    {
        // Given
        bootstrapper = newBootstrapper();

        // When
        String[] propertiesArray = getDefaultPropertiesArray();

        int resultCode = start( bootstrapper, propertiesArray);

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );
    }

    @Test
    public void canSpecifyConfigFile() throws Throwable
    {
        // Given
        bootstrapper = newBootstrapper();
        File configFile = tempDir.newFile( "neo4j.config" );

        Map<String,String> properties = stringMap( forced_kernel_id.name(), "ourcustomvalue" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        store( properties, configFile );

        // When
        start( bootstrapper, array( "-C", configFile.getAbsolutePath() ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "ourcustomvalue" ) );
    }

    @Test
    public void canOverrideConfigValues() throws Throwable
    {
        // Given
        bootstrapper = newBootstrapper();
        File configFile = tempDir.newFile( "neo4j.config" );

        Map<String,String> properties = stringMap( forced_kernel_id.name(), "thisshouldnotshowup" );
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        store( properties, configFile );

        // When
        start( bootstrapper, array(
                "-C", configFile.getAbsolutePath(),
                "-c", configOption( forced_kernel_id.name(), "mycustomvalue" ) ) );

        // Then
        assertThat( bootstrapper.getServer().getConfig().get( forced_kernel_id ), equalTo( "mycustomvalue" ) );
    }

    @After
    public void cleanup()
    {
        if ( bootstrapper != null )
        {
            bootstrapper.stop();
        }
    }

    private String[] getDefaultPropertiesArray() throws IOException
    {
        Map<String,String> properties = ServerTestUtils.getDefaultRelativeProperties();
        List<String> values = new ArrayList<>();
        for ( Map.Entry<String,String> entry : properties.entrySet() )
        {
            values.add( "-c" );
            values.add( configOption( entry.getKey(), entry.getValue() ) );
        }
        return values.toArray( new String[values.size()] );
    }

    private String configOption( String key, String value )
    {
        return key + "=" + value;
    }
}
