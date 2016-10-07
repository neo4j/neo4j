/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.neo4j.desktop.Parameters;
import org.neo4j.desktop.config.Installation;
import org.neo4j.desktop.model.DesktopModel;
import org.neo4j.server.configuration.ClientConnectorSettings;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.configuration.ClientConnectorSettings.httpConnector;

public class DatabaseActionsTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private File storeDir;
    private File configFile;

    @Test
    public void shouldCreateMessagesLogBelowStoreDir() throws Exception
    {
        // Given
        Installation installation = mock( Installation.class );
        when( installation.getDatabaseDirectory() ).thenReturn( storeDir );
        when( installation.getConfigurationsFile() ).thenReturn( configFile );

        DesktopModel model = new DesktopModel( installation, new Parameters( new String[] {} ) );
        DatabaseActions databaseActions = new DatabaseActions( model );

        try
        {
            // when
            databaseActions.start();

            // Then
            File logFile = new File( new File( storeDir, "logs" ), "debug.log" );
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
        storeDir = new File( testDirectory.directory(), "store_dir" );
        storeDir.mkdirs();

        configFile = new File( testDirectory.directory(), "neo4j.conf" );
        Properties props = new Properties();
        props.setProperty( ClientConnectorSettings.httpConnector( "1" ).type.name(), "HTTP" );
        props.setProperty( ClientConnectorSettings.httpConnector( "1" ).encryption.name(), "NONE" );
        props.setProperty( ClientConnectorSettings.httpConnector( "1" ).enabled.name(), "true" );
        try ( FileWriter writer = new FileWriter( configFile ) )
        {
            props.store( writer, "" );
        }
    }
}
