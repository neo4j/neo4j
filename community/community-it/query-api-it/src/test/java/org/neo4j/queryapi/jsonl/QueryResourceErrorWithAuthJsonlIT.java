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
package org.neo4j.queryapi.jsonl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.test.extension.SkipOnSpd;

@QueryAPITestExtension(authEnabled = true)
class QueryResourceErrorWithAuthJsonlIT {

    private final HttpClient client;
    private final String queryEndpoint;

    QueryResourceErrorWithAuthJsonlIT(QueryAPITestClient testClient) {
        this.queryEndpoint = testClient.getEndpoint();
        this.client = HttpClient.newBuilder().build();
    }

    @Test
    @SkipOnSpd(reason = "SPD doesn't run community")
    void impersonationOnCommunityEditionAuthEnabled() throws IOException, InterruptedException {

        var body = """
            {"statement": "RETURN 1 AS n", "impersonatedUser": "Waldo"}
            """;

        var request = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/json")
                .header("Accept", "application/jsonl")
                .header(
                        "Authorization",
                        "Basic " + Base64.getEncoder().encodeToString("neo4j:neo4j".getBytes(StandardCharsets.UTF_8)))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.ArgumentError, "Impersonation is not supported in community edition.")
                .hasNoRemainingEvents();
    }
}
