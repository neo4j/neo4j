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
package org.neo4j.queryapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension
class QueryResourceQueryStatsIT {

    private final QueryAPITestClient testClient;

    QueryResourceQueryStatsIT(QueryAPITestClient testClient) {
        this.testClient = testClient;
    }

    @Test
    void shouldIncludeQueryStats() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .includeCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var queryStatsMap = response.body().counters();

        assertThat(response.body().data().get("fields").size()).isEqualTo(1);
        assertThat(queryStatsMap.size()).isEqualTo(14);
        assertThat(queryStatsMap.get("containsUpdates").asBoolean()).isEqualTo(false);
        assertThat(queryStatsMap.get("containsSystemUpdates").asBoolean()).isEqualTo(false);
        assertThat(queryStatsMap.get("nodesCreated").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("nodesDeleted").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("propertiesSet").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("relationshipsCreated").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("relationshipsDeleted").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("labelsAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("labelsRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("indexesAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("indexesRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("constraintsAdded").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("constraintsRemoved").asInt()).isEqualTo(0);
        assertThat(queryStatsMap.get("systemUpdates").asInt()).isEqualTo(0);
    }

    @Test
    void shouldNotIncludeQueryStats() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .withoutCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        assertThat(response.body().data().get(VALUES_KEY).size()).isEqualTo(1);
        assertThat(response.body().counters()).isNull();
    }

    @Test
    void shouldNotIncludeQueryStatsByDefault() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.returnOne());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        assertThat(response.body().data().get(VALUES_KEY).size()).isEqualTo(1);
        assertThat(response.body().counters()).isNull();
    }

    @Test
    void shouldErrorIfInvalidInput() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN 1\", " + "\"includeCounters\": \"banana\"}");

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
    }
}
