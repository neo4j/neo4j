/**
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
package org.neo4j.server.logging;

import org.junit.After;
import org.junit.Test;

import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.eclipse.jetty.server.Server.getVersion;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

public class LoggingIT extends ExclusiveServerTestBase
{
    private CommunityNeoServer server;

    @Test
    public void shouldNotLogToStandardOutByDefault() throws Throwable
    {
        // Given
        // The mute assertion should not fail, meaning nothing has logged to stdout/stderr
        mute.withSilenceAssertion();

        TestLogging logging = new TestLogging();
        server = CommunityServerBuilder.server( logging ).build();

        // When
        server.start();
        server.stop();
        server = null;
    }

    @Test
    public void jettyOutputShouldBeInOurLog() throws Exception
    {
        // Given
        TestLogging logging = new TestLogging();
        server = CommunityServerBuilder.server( logging ).build();

        // When
        server.start();
        server.stop();
        server = null;

        // Then
        logging.getMessagesLog( WebServer.class ).assertAtLeastOnce(
                // This is a terrible thing to assert on, but it was the only log message I could see from
                // jetty that is stable across runs. The point here is to ensure jetty does forward its log messages
                // to Neo logging.
                info( "jetty-"+getVersion() )
        );
    }

    @After
    public void cleanup()
    {
        if( server != null )
        {
            server.stop();
        }
    }
}