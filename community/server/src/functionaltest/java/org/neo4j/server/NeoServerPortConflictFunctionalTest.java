/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.Test;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.logging.InMemoryAppender;

public class NeoServerPortConflictFunctionalTest
{

    @Test
    public void shouldComplainIfServerPortIsAlreadyTaken() throws IOException
    {
        int contestedPort = 9999;
        ServerSocket socket = new ServerSocket( contestedPort, 0, InetAddress.getLocalHost() );
        InMemoryAppender appender = new InMemoryAppender( NeoServerWithEmbeddedWebServer.log );
        NeoServerWithEmbeddedWebServer server = ServerBuilder.server()
                .onPort( contestedPort )
                .onHost( InetAddress.getLocalHost().toString() )
                .build();
        server.start();

        // Don't include the SEVERE string since it's
        // OS-regional-settings-specific
        assertThat(
                appender.toString(),
                containsString( String.format( ": Failed to start Neo Server on port [%s]", server.getWebServerPort() ) ) );
        socket.close();
        server.stop();
    }
}
