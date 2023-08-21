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
package org.neo4j.server.security.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

class AuthorizationDisabledIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @Test
    void shouldAllowDisablingAuthorization() throws Exception {
        // Given
        testWebContainer = serverOnRandomPorts()
                .withProperty(GraphDatabaseSettings.auth_enabled.name(), FALSE)
                .build();

        // When

        // Then I should have write access
        HTTP.Response response = HTTP.POST(
                testWebContainer.getBaseUri().resolve(txCommitEndpoint()).toString(),
                rawPayload("{\"statements\": [{\"statement\": \"CREATE ({name:'My Node'})\"}]}"));
        assertThat(response.status()).isEqualTo(200);

        // Then I should have read access
        response = HTTP.POST(
                testWebContainer.getBaseUri().resolve(txCommitEndpoint()).toString(),
                rawPayload("{\"statements\": [{\"statement\": \"MATCH (n {name:'My Node'}) RETURN n\"}]}"));
        assertThat(response.status()).isEqualTo(200);
        String responseBody = response.rawContent();
        assertThat(responseBody).contains("My Node");
    }

    @AfterEach
    void cleanup() {
        if (testWebContainer != null) {
            testWebContainer.shutdown();
        }
    }
}
