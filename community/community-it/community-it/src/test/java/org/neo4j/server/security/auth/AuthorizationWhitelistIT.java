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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.server.helpers.CommunityWebContainerBuilder.serverOnRandomPorts;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;
import org.neo4j.test.server.HTTP;

class AuthorizationWhitelistIT extends ExclusiveWebContainerTestBase {
    private TestWebContainer testWebContainer;

    @Test
    void shouldWhitelistBrowser() throws Exception {
        // Given
        assumeTrue(browserIsLoaded());
        testWebContainer =
                serverOnRandomPorts().withProperty(auth_enabled.name(), TRUE).build();

        // Then I should be able to access the browser
        HTTP.Response response = HTTP.GET(
                testWebContainer.getBaseUri().resolve("browser/index.html").toString());
        assertThat(response.status()).isEqualTo(200);
    }

    @Test
    void shouldNotWhitelistConsoleService() throws Exception {
        // Given
        testWebContainer =
                serverOnRandomPorts().withProperty(auth_enabled.name(), TRUE).build();

        // Then I should not be able to access the console service
        HTTP.Response response = HTTP.GET(testWebContainer
                .getBaseUri()
                .resolve("db/manage/server/console")
                .toString());
        assertThat(response.status()).isEqualTo(401);
    }

    @Test
    void shouldNotWhitelistDB() throws Exception {
        // Given
        testWebContainer =
                serverOnRandomPorts().withProperty(auth_enabled.name(), TRUE).build();

        // Then I should get a unauthorized response for access to the DB
        HTTP.Response response =
                HTTP.GET(testWebContainer.getBaseUri().resolve("db/neo4j").toString());
        assertThat(response.status()).isEqualTo(401);
    }

    @AfterEach
    void cleanup() {
        if (testWebContainer != null) {
            testWebContainer.shutdown();
        }
    }

    private boolean browserIsLoaded() {
        // In some automatic builds, the Neo4j browser is not built, and it is subsequently not present for these
        // tests. So - only run these tests if the browser artifact is on the classpath
        return getClass().getClassLoader().getResource("browser") != null;
    }
}
