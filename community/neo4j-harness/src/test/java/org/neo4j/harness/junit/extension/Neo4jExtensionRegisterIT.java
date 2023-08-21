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
package org.neo4j.harness.junit.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.extensionpackage.MyUnmanagedExtension;
import org.neo4j.test.server.HTTP;

class Neo4jExtensionRegisterIT {
    private static final String REGISTERED_TEMP_PREFIX = "registeredTemp";

    @RegisterExtension
    static Neo4jExtension neo4jExtension = Neo4jExtension.builder()
            .withFolder(createTempDirectory())
            .withFixture("CREATE (u:User)")
            .withFixture(graphDatabaseService -> {
                try (Transaction tx = graphDatabaseService.beginTx()) {
                    tx.createNode(Label.label("User"));
                    tx.commit();
                }
                return null;
            })
            .withUnmanagedExtension("/test", MyUnmanagedExtension.class)
            .build();

    @Test
    void neo4jAvailable(Neo4j neo4j) {
        assertNotNull(neo4j);
        assertThat(HTTP.GET(neo4j.httpURI().toString()).status()).isEqualTo(200);
    }

    @Test
    void graphDatabaseServiceIsAvailable(GraphDatabaseService databaseService) {
        assertNotNull(databaseService);
        assertDoesNotThrow(() -> {
            try (Transaction transaction = databaseService.beginTx()) {
                transaction.createNode();
                transaction.commit();
            }
        });
    }

    @Test
    void customExtensionWorkingDirectory(Neo4j neo4j) {
        assertThat(neo4j.config()
                        .get(GraphDatabaseSettings.neo4j_home)
                        .getParent()
                        .getFileName()
                        .toString())
                .startsWith(REGISTERED_TEMP_PREFIX);
    }

    @Test
    void unmanagedExtensionRegistered(Neo4j neo4j) {
        assertThat(HTTP.GET(neo4j.httpURI().resolve("test/myExtension").toString())
                        .status())
                .isEqualTo(234);
    }

    @Test
    void fixturesRegistered(Neo4j neo4j) throws Exception {
        // Then
        HTTP.Response response = HTTP.POST(
                neo4j.httpURI() + "db/neo4j/tx/commit",
                quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

        assertThat(response.get("results").get(0).get("data").size()).isEqualTo(2);
    }

    private static Path createTempDirectory() {
        try {
            return Files.createTempDirectory(REGISTERED_TEMP_PREFIX);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
