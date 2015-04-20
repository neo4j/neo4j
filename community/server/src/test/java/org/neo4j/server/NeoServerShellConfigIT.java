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

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class NeoServerShellConfigIT extends ExclusiveServerTestBase
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldBeAbleToOverrideShellConfig()  throws Throwable
    {
        final int customPort = findFreeShellPortToUse( 8881 );
        // Given
        CommunityNeoServer server = new CommunityNeoServer( new ConfigurationBuilder()
        {
            @Override
            public Config configuration()
            {
                return new Config( stringMap(
                        Configurator.DATABASE_LOCATION_PROPERTY_KEY, testDir.absolutePath() ) );
            }

            @Override
            public Map<String, String> getDatabaseTuningProperties()
            {
                return stringMap(
                        ShellSettings.remote_shell_enabled.name(), Settings.TRUE,
                        ShellSettings.remote_shell_port.name(), "" + customPort );
            }
        }, GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL) );

        // When
        server.start();

        // Then
        // Try to connect with a shell client to that custom port.
        // Throws exception if unable to connect
        ShellLobby.newClient( customPort )
                .shutdown();

        server.stop();
    }

    @Test
    public void connectWithShellOnDefaultPortWhenNoShellConfigSupplied() throws Throwable
    {
        // Given
        CommunityNeoServer server = new CommunityNeoServer( new ConfigurationBuilder()
        {
            @Override
            public Config configuration()
            {
                return new Config( stringMap(
                        Configurator.DATABASE_LOCATION_PROPERTY_KEY, testDir.absolutePath() ) );
            }

            @Override
            public Map<String, String> getDatabaseTuningProperties()
            {
                return stringMap();
            }
        }, GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL) );

        // When
        server.start();

        // Then
        ShellLobby.newClient()
                .shutdown();

        server.stop();
    }

    private int findFreeShellPortToUse( int startingPort )
    {
        // Make sure there's no other random stuff on that port
        while ( true )
        {
            try
            {
                ShellLobby.newClient( startingPort++ ).shutdown();
            }
            catch ( ShellException e )
            {   // Good
                return startingPort;
            }
        }
    }

}
