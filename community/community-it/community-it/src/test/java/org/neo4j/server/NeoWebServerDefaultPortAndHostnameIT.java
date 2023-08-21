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

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Test;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;

public class NeoWebServerDefaultPortAndHostnameIT extends AbstractRestFunctionalTestBase {
    @Test
    public void shouldDefaultToSensiblePortIfNoneSpecifiedInConfig() throws Exception {
        var functionalTestHelper = new FunctionalTestHelper(container());

        var request =
                HttpRequest.newBuilder(functionalTestHelper.baseUri()).GET().build();
        var httpClient = HttpClient.newBuilder().followRedirects(NORMAL).build();
        var response = httpClient.send(request, discarding());

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Test
    public void shouldDefaultToLocalhostOfNoneSpecifiedInConfig() {
        assertThat(container().getBaseUri().getHost()).isEqualTo("localhost");
    }
}
