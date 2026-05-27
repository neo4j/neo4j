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

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension
class QueryResourceNotificationsIT {

    private final QueryAPITestClient testClient;

    QueryResourceNotificationsIT(QueryAPITestClient testClient) {
        this.testClient = testClient;
    }

    @Test
    void shouldReturnLabelDoesNotExistNotification() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:thisLabelDoesNotExist) return n")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var notificationsJson = response.body().notifications();

        assertThat(notificationsJson.size()).isEqualTo(1);
        assertThat(notificationsJson.get(0).get("code").asText())
                .isEqualTo(NotificationCodeWithDescription.MISSING_LABEL
                        .getStatus()
                        .code()
                        .serialize());
        assertThat(notificationsJson.get(0).get("title").asText())
                .isEqualTo(NotificationCodeWithDescription.MISSING_LABEL
                        .getStatus()
                        .code()
                        .description());
        assertThat(notificationsJson.get(0).get("description").asText())
                .isEqualTo(
                        "One of the labels in your query is not available in the database, make sure you didn't misspell it or that the label is available when you run this statement in your application (the missing label name is: thisLabelDoesNotExist)");
        assertThat(notificationsJson.get(0).get("position").get("offset").asInt())
                .isEqualTo(9);
        assertThat(notificationsJson.get(0).get("position").get("line").asInt()).isEqualTo(1);
        assertThat(notificationsJson.get(0).get("position").get("column").asInt())
                .isEqualTo(10);
        assertThat(notificationsJson.get(0).get("severity").asText()).isEqualTo("WARNING");
        assertThat(notificationsJson.get(0).get("category").asText()).isEqualTo("UNRECOGNIZED");
    }

    @Test
    void shouldReturnMultipleNotifications() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var notificationsJson = response.body().notifications();

        assertThat(notificationsJson.get(0).get("code").asText())
                .isEqualTo(Status.Statement.UnknownLabelWarning.code().serialize());
        assertThat(notificationsJson.get(1).get("code").asText())
                .isEqualTo(Status.Statement.UnknownLabelWarning.code().serialize());
        assertThat(notificationsJson.get(2).get("code").asText())
                .isEqualTo(Status.Statement.CartesianProduct.code().serialize());
    }

    @Test
    void shouldNotReturnNotificationsIfNonePresent() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.returnOne());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        assertThat(response.body().notifications()).isNull();
    }
}
