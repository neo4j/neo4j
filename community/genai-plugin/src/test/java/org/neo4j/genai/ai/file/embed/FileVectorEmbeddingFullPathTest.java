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
package org.neo4j.genai.ai.file.embed;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.genai.GenAiPluginExtension;
import org.neo4j.genai.ai.text.embed.provider.azure.AzureOpenAi;
import org.neo4j.genai.ai.text.embed.provider.bedrock.BedrockTitan;
import org.neo4j.genai.ai.text.embed.provider.openai.OpenAi;
import org.neo4j.genai.ai.text.embed.provider.vertexai.VertexAi;
import org.neo4j.genai.util.GenAITestExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImpermanentDbmsExtension(configurationCallback = "configure")
public class FileVectorEmbeddingFullPathTest implements GenAITestExtension {

    private static final String EMBED_QUERY = """
            WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
            CALL ai.file.embedBatch($file, 'openai', conf)
            YIELD index, vector, resource
            RETURN *
            """;

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private TestDirectory testDirectory;

    private WireMockServer wireMock;

    private Path testFile;

    private final String fileText = """
                This is 1 text used for file reading and embedding.
                This is 2 text used for file reading and embedding.
                This is 3 text used for file reading and embedding.
                This is 4 text used for file reading and embedding.""";

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        installPlugin(testDirectory);
        Path insideDirectory = testDirectory.directory("inside");

        // A file sitting inside the import root
        testFile = insideDirectory.resolve("test.txt");
        Files.createDirectories(testFile.getParent());
        Files.writeString(testFile, fileText);

        this.wireMock = new WireMockServer(options()
                .dynamicPort()
                .usingFilesUnderClasspath("wiremock/embeddings")
                // Uncomment for wiremock debug prints:
                // .notifier(new ConsoleNotifier(true))
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new AzureOpenAi(),
                new OpenAi(baseUrl + "/v1"),
                new VertexAi(p -> URI.create(baseUrl)),
                new BedrockTitan(p -> URI.create(baseUrl))));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25);
        builder.setConfig(GraphDatabaseSettings.allow_file_urls, true);
    }

    @AfterAll
    void tearDown() {
        if (wireMock != null) {
            wireMock.stop();
        }
    }

    @Test
    void shouldEmbedLocalFile() {
        var result = db.executeTransactionally(
                EMBED_QUERY,
                Map.of("file", testFile.toUri().toString()),
                res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void needsFullPathToWork() {
        assertThatThrownBy(() -> db.executeTransactionally(
                        EMBED_QUERY, Map.of("file", "file:///this/path/does/not/exist.txt"), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("File not found: file:///this/path/does/not/exist.txt");
    }
}
