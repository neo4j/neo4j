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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOutClientConfiguration;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

class TransactionTimeoutIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @AfterEach
    void stopTheServer() {
        testWebContainer.shutdown();
    }

    @Test
    void shouldHonorReallyLowTransactionTimeout() throws Exception {
        // Given
        testWebContainer = serverOnRandomPorts()
                .withProperty(ServerSettings.http_transaction_timeout.name(), "1")
                .build();

        String tx = HTTP.POST(txURI(), map("statements", singletonList(map("statement", "CREATE (n)"))))
                .location();

        // When
        Thread.sleep(1000 * 5);
        Map<String, Object> response = HTTP.POST(tx + "/commit").content();

        // Then
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
        assertThat(errors.get(0).get("code"))
                .isEqualTo(TransactionNotFound.code().serialize());
    }

    @Test
    void shouldEventuallyTimeoutATransaction() throws Exception {
        // Given
        testWebContainer = serverOnRandomPorts()
                .withProperty(ServerSettings.http_transaction_timeout.name(), "10")
                .build();

        String tx = HTTP.POST(txURI(), map("statements", singletonList(map("statement", "CREATE (n)"))))
                .location();

        Thread.sleep(1000 * 5);

        // tx should still be around and refreshes the timeout to 10s
        assertThat(HTTP.POST(tx).status()).isEqualTo(200);

        // When
        Thread.sleep(1000 * 15);
        Map<String, Object> response = HTTP.POST(tx + "/commit").content();

        // Then
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
        assertThat(errors.get(0).get("code"))
                .isEqualTo(TransactionNotFound.code().serialize());
    }

    @Test
    void shouldTimeoutAtDBMSLevel() throws Exception {
        // Given
        testWebContainer = serverOnRandomPorts()
                .withProperty(ServerSettings.http_transaction_timeout.name(), "30")
                .withProperty(GraphDatabaseSettings.transaction_timeout.name(), "5")
                .build();

        String tx = HTTP.POST(txURI(), map("statements", singletonList(map("statement", "CREATE (n)"))))
                .location();

        // When
        Thread.sleep(1000 * 15);
        var respA = HTTP.POST(tx + "/commit");
        Map<String, Object> response = respA.content();

        // Then
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
        assertThat(errors.get(0).get("code"))
                .isEqualTo(TransactionTimedOutClientConfiguration.code().serialize());

        // And then transaction is cleaned from the registry
        Map<String, Object> respB = HTTP.POST(tx + "/commit").content();

        // Then
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errors2 = (List<Map<String, Object>>) respB.get("errors");
        assertThat(errors2.get(0).get("code"))
                .isEqualTo(TransactionNotFound.code().serialize());
    }

    private String txURI() {
        return testWebContainer.getBaseUri() + txEndpoint();
    }
}
