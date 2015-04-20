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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.Test;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.test.BufferingLogging;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class NeoServerPortConflictDocIT extends ExclusiveServerTestBase
{
    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException
    {
        int contestedPort = 9999;
        try ( ServerSocket ignored = new ServerSocket( contestedPort, 0, InetAddress.getByName(Jetty9WebServer.DEFAULT_ADDRESS ) ) )
        {
            Logging logging = new BufferingLogging();
            CommunityNeoServer server = CommunityServerBuilder.server()
                    .onPort( contestedPort )
                    .usingDatabaseDir( folder.cleanDirectory( name.getMethodName() ).getAbsolutePath() )
                    .onHost( Jetty9WebServer.DEFAULT_ADDRESS )
                    .withLogging( logging )
                    .build();
            try
            {
                server.start();

                fail( "Should have reported failure to start" );
            }
            catch ( ServerStartupException e )
            {
                assertThat( e.getMessage(), containsString( "Starting Neo4j Server failed" ) );
            }

            // Don't include the SEVERE string since it's
            // OS-regional-settings-specific
            assertThat(
                    logging.toString(),
                    containsString( String.format( "Failed to start Neo Server" ) ) );
            server.stop();
        }
    }
}
