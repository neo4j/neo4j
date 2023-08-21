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
package org.neo4j.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class FixturesTestIT {
    @Inject
    private TestDirectory testDir;

    @Test
    void shouldAcceptSingleCypherFileAsFixture() throws Exception {
        // Given
        Path targetFolder = testDir.homePath();
        Path fixture = targetFolder.resolve("fixture.cyp");
        writeFixture(fixture, "CREATE (u:User)" + "CREATE (a:OtherUser)");

        // When
        try (Neo4j server = getServerBuilder(targetFolder).withFixture(fixture).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.status()).isEqualTo(200);
            assertThat(response.get("results").get(0).get("data").size()).isEqualTo(1);
        }
    }

    @Test
    void shouldAcceptFolderWithCypFilesAsFixtures() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();
        writeFixture(targetFolder.resolve("fixture1.cyp"), "CREATE (u:User)\n" + "CREATE (a:OtherUser)");
        writeFixture(targetFolder.resolve("fixture2.cyp"), "CREATE (u:User)\n" + "CREATE (a:OtherUser)");

        // And given one file in a sub directory
        Path subDir = targetFolder.resolve("subdirectory");
        Files.createDirectories(subDir);
        writeFixture(subDir.resolve("subDirFixture.cyp"), "CREATE (u:User)\n" + "CREATE (a:OtherUser)");

        // When
        try (Neo4j server =
                getServerBuilder(targetFolder).withFixture(targetFolder).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.get("results").get(0).get("data").size())
                    .as(response.toString())
                    .isEqualTo(3);
        }
    }

    @Test
    void shouldHandleMultipleFixtures() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();
        Path fixture1 = targetFolder.resolve("fixture1.cyp");
        writeFixture(fixture1, "CREATE (u:User)\n" + "CREATE (a:OtherUser)");
        Path fixture2 = targetFolder.resolve("fixture2.cyp");
        writeFixture(fixture2, "CREATE (u:User)\n" + "CREATE (a:OtherUser)");

        // When
        try (Neo4j server = getServerBuilder(targetFolder)
                .withFixture(fixture1)
                .withFixture(fixture2)
                .build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.get("results").get(0).get("data").size()).isEqualTo(2);
        }
    }

    @Test
    void shouldHandleStringFixtures() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();

        // When
        try (Neo4j server =
                getServerBuilder(targetFolder).withFixture("CREATE (a:User)").build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.get("results").get(0).get("data").size()).isEqualTo(1);
        }
    }

    @Test
    void shouldIgnoreEmptyFixtureFiles() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();
        writeFixture(targetFolder.resolve("fixture1.cyp"), "CREATE (u:User)\n" + "CREATE (a:OtherUser)");
        writeFixture(targetFolder.resolve("fixture2.cyp"), "");

        // When
        try (Neo4j server =
                getServerBuilder(targetFolder).withFixture(targetFolder).build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.get("results").get(0).get("data").size()).isEqualTo(1);
        }
    }

    @Test
    void shouldHandleFixturesWithSyntaxErrorsGracefully() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();
        writeFixture(targetFolder.resolve("fixture1.cyp"), "this is not a valid cypher statement");

        // When
        try (Neo4j ignore =
                getServerBuilder(targetFolder).withFixture(targetFolder).build()) {
            fail("Should have thrown exception");
        } catch (QueryExecutionException e) {
            assertThat(e.getStatusCode()).isEqualTo("Neo.ClientError.Statement.SyntaxError");
        }
    }

    @Test
    void shouldHandleFunctionFixtures() throws Exception {
        // Given two files in the root folder
        Path targetFolder = testDir.homePath();

        // When
        try (Neo4j server = getServerBuilder(targetFolder)
                .withFixture(graphDatabaseService -> {
                    try (Transaction tx = graphDatabaseService.beginTx()) {
                        tx.createNode(Label.label("User"));
                        tx.commit();
                    }
                    return null;
                })
                .build()) {
            // Then
            HTTP.Response response = HTTP.POST(
                    server.httpURI() + "db/neo4j/tx/commit",
                    quotedJson("{'statements':[{'statement':'MATCH (n:User) RETURN n'}]}"));

            assertThat(response.get("results").get(0).get("data").size()).isEqualTo(1);
        }
    }

    private Neo4jBuilder getServerBuilder(Path targetFolder) {
        SelfSignedCertificateFactory.create(testDir.getFileSystem(), testDir.homePath());
        return Neo4jBuilders.newInProcessBuilder(targetFolder)
                .withConfig(SslPolicyConfig.forScope(BOLT).enabled, Boolean.TRUE)
                .withConfig(SslPolicyConfig.forScope(BOLT).base_directory, testDir.homePath())
                .withConfig(SslPolicyConfig.forScope(HTTPS).enabled, Boolean.TRUE)
                .withConfig(SslPolicyConfig.forScope(HTTPS).base_directory, testDir.homePath());
    }

    private static void writeFixture(Path fixture, String cypher) throws IOException {
        Files.writeString(
                fixture,
                cypher,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
