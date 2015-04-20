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
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.desktop.config.Installation;
import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.server.ServerTestUtils.writePropertiesToFile;

public class DatabaseActionsTest
{
    @Rule
    public TargetDirectory.TestDirectory baseDir = TargetDirectory.testDirForTest( getClass() );

    private File storeDir;
    private File serverConfigFile;
    private File dbConfigFile;

    @Test
    public void shouldCreateMessagesLogInDbDirWithClassicLog() throws Exception
    {
        // Given
        Installation installation = mock( Installation.class );
        when( installation.getDatabaseDirectory() ).thenReturn( storeDir );
        when( installation.getServerConfigurationsFile() ).thenReturn( serverConfigFile );

        DesktopModel model = new DesktopModel( installation );
        DatabaseActions databaseActions = new DatabaseActions( model );

        try
        {
            // when
            databaseActions.start();

            // Then
            File logFile = new File( storeDir, "messages.log" );
            assertTrue( logFile.exists() );
        }
        finally
        {
            // After
            databaseActions.stop(); // do not need to wait for the server to finish all its start procedure
        }
    }

    @Before
    public void createFiles() throws IOException
    {
        storeDir = new File( baseDir.directory(), "store_dir" );
        serverConfigFile = new File( storeDir, "neo4j-server.properties" );
        dbConfigFile = new File( storeDir, "neo4j.properties" );
        storeDir.mkdirs();

        Map<String,String> properties = MapUtil.stringMap( GraphDatabaseSettings.store_dir.name(),
                storeDir.getAbsolutePath());
        writePropertiesToFile( properties, serverConfigFile );

        dbConfigFile.createNewFile();
    }
}
