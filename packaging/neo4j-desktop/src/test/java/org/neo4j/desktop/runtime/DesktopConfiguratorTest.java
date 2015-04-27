/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.runtime;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.desktop.config.Installation;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DesktopConfiguratorTest
{
    private File emptyServerConfigFile;

    @Before
    public void setUp() throws Throwable
    {
        emptyServerConfigFile = File.createTempFile( "emptyFile", "tmp" );
    }

    @After
    public void tearDown()
    {
        emptyServerConfigFile.delete();
    }

    @Test
    public void dbPropertiesShouldContainStoreDirProperty() throws Exception
    {
        // Given
        Installation installation = mock( Installation.class );
        when( installation.getServerConfigurationsFile() ).thenReturn( emptyServerConfigFile );
        DesktopConfigurator config = new DesktopConfigurator( installation );

        File storeDir = new File( "graph.db" ); // will not create any file

        // When
        config.setDatabaseDirectory( storeDir );

        // Then
        assertEquals( storeDir.getAbsolutePath(), config.getDatabaseDirectory() );

        String pathToStoreDir = config.getDatabaseTuningProperties().get( GraphDatabaseSettings.store_dir.name() );
        assertEquals( storeDir.getAbsolutePath(), pathToStoreDir );
    }
}
