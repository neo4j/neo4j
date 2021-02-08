/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.startup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.List;
import java.util.Map;

import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class Neo4jCommandIT extends Neo4jCommandTestBase
{
    @Override
    @BeforeEach
    public void setUp() throws Exception
    {
        super.setUp();
        //VM
        addConf( BootloaderSettings.max_heap_size, "100m" );
        addConf( BootloaderSettings.initial_heap_size, "10m" );
        //DBMS
        addConf( GraphDatabaseSettings.pagecache_memory, "8m" );
        addConf( GraphDatabaseSettings.logical_log_rotation_threshold, "128k" );
        addConf( GraphDatabaseSettings.store_internal_log_level, "debug" );
        //Connectors
        addConf( HttpConnector.enabled, "true" );
        addConf( HttpConnector.listen_address, "localhost:0" );
        addConf( HttpsConnector.enabled, "false" );
        addConf( BoltConnector.enabled, "false" );
    }

    @DisabledOnOs( OS.WINDOWS )
    @Test
    void shouldBeAbleToStartAndStopRealServerOnNonWindows()
    {
        shouldBeAbleToStartAndStopRealServer();
        assertThat( err.toString() ).isEmpty();
    }

    @EnabledOnOs( OS.WINDOWS )
    @Test
    void shouldBeAbleToStartAndStopRealServerOnWindows()
    {
        assumeThat( isCurrentlyRunningAsWindowsAdmin() ).isTrue();
        addConf( BootloaderSettings.windows_service_name, "neo4j-" + currentTimeMillis() );
        try
        {
            assertThat( execute( "install-service" ) ).isEqualTo( 0 );
            shouldBeAbleToStartAndStopRealServer();
        }
        finally
        {
            assertThat( execute( "uninstall-service" ) ).isEqualTo( 0 );
        }
        assertThat( err.toString() ).isEmpty();
    }

    private void shouldBeAbleToStartAndStopRealServer()
    {
        assertThat( execute( List.of( "start" ), Map.of( Bootloader.ENV_NEO4J_START_WAIT, "3" ) ) ).isEqualTo( 0 );
        assertEventually( this::getDebugLogLines, s -> s.contains( getVersion() + "NeoWebServer] ========" ), 5, MINUTES );
        assertEventually( this::getUserLogLines, s -> s.contains( "Remote interface available at" ), 5, MINUTES );
        assertThat( execute( "stop" ) ).isEqualTo( 0 );
    }

    protected String getVersion()
    {
        return "Community";
    }

    protected Class<? extends EntryPoint> entrypoint()
    {
        return EntryPoint.serviceloadEntryPoint();
    }
}
