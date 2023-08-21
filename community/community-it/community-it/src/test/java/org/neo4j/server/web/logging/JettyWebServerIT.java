/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.web.logging;

import static org.mockito.Mockito.mock;

import org.eclipse.jetty.io.ByteBufferPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.web.JettyWebServer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class JettyWebServerIT extends ExclusiveWebContainerTestBase {
    private JettyWebServer webServer;

    @Test
    void shouldBeAbleToUsePortZero() throws Exception {
        // Given
        webServer = new JettyWebServer(
                NullLogProvider.getInstance(),
                Config.defaults(),
                NetworkConnectionTracker.NO_OP,
                mock(ByteBufferPool.class));

        webServer.setHttpAddress(new SocketAddress("localhost", 0));

        // When
        webServer.start();

        // Then no exception
    }

    @Test
    void shouldBeAbleToRestart() throws Throwable {
        // given
        webServer = new JettyWebServer(
                NullLogProvider.getInstance(),
                Config.defaults(),
                NetworkConnectionTracker.NO_OP,
                mock(ByteBufferPool.class));
        webServer.setHttpAddress(new SocketAddress("127.0.0.1", 7878));

        // when
        webServer.start();
        webServer.stop();
        webServer.start();

        // then no exception
    }

    @Test
    void shouldStopCleanlyEvenWhenItHasntBeenStarted() {
        new JettyWebServer(
                        NullLogProvider.getInstance(),
                        Config.defaults(),
                        NetworkConnectionTracker.NO_OP,
                        mock(ByteBufferPool.class))
                .stop();
    }

    @AfterEach
    void cleanup() {
        if (webServer != null) {
            webServer.stop();
        }
    }
}
