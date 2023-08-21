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
package org.neo4j.server;

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.AbstractNeoWebServer.NEO4J_IS_STARTING_MESSAGE;
import static org.neo4j.server.helpers.WebContainerHelper.createNonPersistentContainer;

import java.io.ByteArrayOutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

class NeoWebServerStartupLoggingIT extends ExclusiveWebContainerTestBase {
    private static ByteArrayOutputStream out;
    private static TestWebContainer webContainer;

    @BeforeAll
    static void setupServer() throws Exception {
        out = new ByteArrayOutputStream();
        webContainer = createNonPersistentContainer(new Log4jLogProvider(out));
    }

    @AfterAll
    static void stopServer() {
        webContainer.shutdown();
    }

    @Test
    void shouldLogStartup() throws Exception {
        // Check the logs
        var logContent = out.toString();
        assertThat(logContent.length()).isGreaterThan(0);
        assertThat(logContent).contains(NEO4J_IS_STARTING_MESSAGE);

        // Check the server is alive
        var request = HttpRequest.newBuilder(webContainer.getBaseUri()).GET().build();
        var client = HttpClient.newBuilder().followRedirects(NEVER).build();
        var response = client.send(request, discarding());
        assertThat(response.statusCode()).isGreaterThan(199);
    }
}
