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
package org.neo4j.server.http.cypher.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.server.helpers.WebContainerHelper.cleanTheDatabase;
import static org.neo4j.server.helpers.WebContainerHelper.createReadOnlyContainer;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

class ReadOnlyIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer readOnlyContainer;
    private HTTP.Builder http;

    @BeforeEach
    void setup() throws Exception {
        cleanTheDatabase(readOnlyContainer);
        readOnlyContainer = createReadOnlyContainer(testDirectory.homePath());
        http = HTTP.withBaseUri(readOnlyContainer.getBaseUri());
    }

    @AfterEach
    void teardown() {
        if (readOnlyContainer != null) {
            readOnlyContainer.shutdown();
        }
    }

    @Test
    void shouldReturnReadOnlyStatusWhenCreatingNodes() throws Exception {
        // Given
        HTTP.Response response =
                http.POST(txEndpoint(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (node)' } ] }"));

        // Then
        JsonNode error = response.get("errors").get(0);
        String code = error.get("code").asText();
        String message = error.get("message").asText();

        assertEquals("Neo.ClientError.General.WriteOnReadOnlyAccessDatabase", code);
        assertThat(message).contains("The database is in read-only mode on this Neo4j instance");
    }

    @Test
    void shouldReturnReadOnlyStatusWhenCreatingNodesWhichTransitivelyCreateTokens() throws Exception {
        // Given
        // When
        HTTP.Response response =
                http.POST(txEndpoint(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (node:Node)' } ] }"));

        // Then
        JsonNode error = response.get("errors").get(0);
        String code = error.get("code").asText();
        String message = error.get("message").asText();

        assertEquals("Neo.ClientError.General.WriteOnReadOnlyAccessDatabase", code);
        assertThat(message).contains("The database is in read-only mode on this Neo4j instance");
    }
}
