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

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class NeoServerPortConflictDocIT extends ExclusiveServerTestBase
{
    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException
    {
        int contestedPort = 9999;
        try ( ServerSocket ignored = new ServerSocket( contestedPort, 0, InetAddress.getByName(Jetty9WebServer.DEFAULT_ADDRESS ) ) )
        {
            AssertableLogProvider logProvider = new AssertableLogProvider();
            CommunityNeoServer server = CommunityServerBuilder.server( logProvider )
                    .onPort( contestedPort )
                    .usingDatabaseDir( folder.directory( name.getMethodName() ).getAbsolutePath() )
                    .onHost( Jetty9WebServer.DEFAULT_ADDRESS )
                    .build();
            try
            {
                server.start();

                fail( "Should have reported failure to start" );
            }
            catch ( ServerStartupException e )
            {
                assertThat( e.getMessage(), containsString( "Starting Neo4j failed" ) );
            }

            logProvider.assertAtLeastOnce(
                    AssertableLogProvider.inLog( containsString( "CommunityNeoServer" ) ).error(
                            "Failed to start Neo Server on port %d: %s",
                            9999, startsWith( "Address 0.0.0.0:9999 is already in use, cannot bind to it." )
                    )
            );
            server.stop();
        }
    }
}
