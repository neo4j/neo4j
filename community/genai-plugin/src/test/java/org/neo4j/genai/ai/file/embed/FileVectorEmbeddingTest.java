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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.permanentRedirect;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
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
public class FileVectorEmbeddingTest implements GenAITestExtension {

    private final String QUERY = """
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
    private FakeFtpServer ftpServer;

    private Path outsideDirectory;

    private Path testFile;
    private final String testFileName = "test.txt";
    private Path testFileEmpty;

    private final String fileText = """
                This is 1 text used for file reading and embedding.
                This is 2 text used for file reading and embedding.
                This is 3 text used for file reading and embedding.
                This is 4 text used for file reading and embedding.""";

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        installPlugin(testDirectory);
        Path insideDirectory = testDirectory.directory("inside");
        outsideDirectory = testDirectory.directory("outside");

        // A file sitting outside the import root
        Path outsideFile = outsideDirectory.resolve("test.txt");
        Files.createDirectories(outsideFile.getParent());
        Files.writeString(outsideFile, "Outside!");

        // A file sitting inside the import root
        testFile = insideDirectory.resolve("test.txt");
        testFileEmpty = insideDirectory.resolve("empty-test.txt");
        Files.createDirectories(testFile.getParent());
        Files.writeString(testFile, fileText);
        Files.writeString(testFileEmpty, "");

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
        // Set the root to the inside directory
        builder.setConfig(GraphDatabaseSettings.load_csv_file_url_root, insideDirectory.toAbsolutePath());

        setupFtpServer();
    }

    private void setupFtpServer() {
        ftpServer = new FakeFtpServer();
        ftpServer.setServerControlPort(0);
        var fileSystem = new UnixFakeFileSystem();
        var ftpDir = "/ftpDir";
        fileSystem.add(new DirectoryEntry(ftpDir));
        fileSystem.add(new FileEntry(ftpDir + "/ftp-test.txt", fileText));
        ftpServer.setFileSystem(fileSystem);
        ftpServer.addUserAccount(new UserAccount("user", "pass", ftpDir));
        ftpServer.start();
    }

    @AfterAll
    void tearDown() {
        if (wireMock != null) {
            wireMock.stop();
        }
        if (ftpServer != null) {
            ftpServer.stop();
        }
    }

    @Test
    void shouldEmbedLocalFile() {
        var result = db.executeTransactionally(
                QUERY,
                Map.of("file", "file:///" + testFileName),
                res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedFileWithChunking() {
        String query = """
            WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
            CALL ai.file.embedBatch($file, 'openai', conf, 11)
            YIELD index, vector, resource
            RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
        """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + testFileName), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Map.of(
                                "vector",
                                true,
                                "index",
                                0L,
                                "resource",
                                "This is 1 text used for file reading and embedding."),
                        Map.of(
                                "vector",
                                true,
                                "index",
                                1L,
                                "resource",
                                "This is 2 text used for file reading and embedding."),
                        Map.of(
                                "vector",
                                true,
                                "index",
                                2L,
                                "resource",
                                "This is 3 text used for file reading and embedding."),
                        Map.of(
                                "vector",
                                true,
                                "index",
                                3L,
                                "resource",
                                "This is 4 text used for file reading and embedding."));
    }

    @Test
    void shouldEmbedFileFromWebUrl() {
        this.wireMock.stubFor(get(urlEqualTo("/web-test.txt"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody(fileText)));

        String webUrl = this.wireMock.baseUrl() + "/web-test.txt";
        var result = db.executeTransactionally(
                QUERY, Map.of("file", webUrl), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedFileFromFtpUrl() {
        int ftpPort = ftpServer.getServerControlPort();
        String ftpUrl = String.format("ftp://user:pass@localhost:%d/ftp-test.txt", ftpPort);

        var result = db.executeTransactionally(
                QUERY, Map.of("file", ftpUrl), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldFailForUnsupportedProtocol() {
        String query = "CALL ai.file.embedBatch('mailto:test@example.com', 'openai', {})";
        assertThatThrownBy(() -> db.executeTransactionally(query, Map.of(), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining("Unsupported protocol: mailto");
    }

    @Test
    void shouldFailForNullFile() {
        String query = "CALL ai.file.embedBatch(null, 'openai', {})";
        assertThatThrownBy(() -> db.executeTransactionally(query, Map.of(), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining("'file' must not be null");
    }

    @Test
    void shouldFailForNullProvider() {
        String query = "CALL ai.file.embedBatch($file, null, {})";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of("file", testFile.toUri().toString()), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("'provider' must not be null");
    }

    @Test
    void shouldFailForNullConfiguration() {
        String query = "CALL ai.file.embedBatch($file, 'openai', null)";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of("file", testFile.toUri().toString()), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("'configuration' must not be null");
    }

    @Test
    void shouldFailForNonPositiveLimit() {
        String query = "CALL ai.file.embedBatch($file, 'openai', {}, 0)";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of("file", testFile.toUri().toString()), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("'limit' must be greater than 0");
    }

    @Test
    void shouldFailForLimitGreaterThanMaxInt() {
        long tooBigLimit = (long) Integer.MAX_VALUE + 1;
        String query = "CALL ai.file.embedBatch($file, 'openai', {}, $limit)";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of("file", testFile.toUri().toString(), "limit", tooBigLimit), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("'limit' must be less than or equal to 2147483647");
    }

    @Test
    void shouldFollowRedirectsHappyPath() {
        this.wireMock.stubFor(get(urlEqualTo("/redirect")).willReturn(permanentRedirect("/web-test.txt")));
        this.wireMock.stubFor(get(urlEqualTo("/web-test.txt"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody(fileText)));

        String redirectUrl = this.wireMock.baseUrl() + "/redirect";
        var result = db.executeTransactionally(
                QUERY, Map.of("file", redirectUrl), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldFollowUpToTenRedirects() {
        for (int i = 1; i <= 10; i++) {
            String from = "/redirect" + i;
            String to = (i == 10) ? "/web-test.txt" : "/redirect" + (i + 1);
            this.wireMock.stubFor(get(urlEqualTo(from)).willReturn(permanentRedirect(to)));
        }
        this.wireMock.stubFor(get(urlEqualTo("/web-test.txt"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody(fileText)));

        String redirectUrl = this.wireMock.baseUrl() + "/redirect1";
        var result = db.executeTransactionally(
                QUERY, Map.of("file", redirectUrl), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
    }

    @Test
    void shouldFailIfMoreThanTenRedirects() {
        for (int i = 1; i <= 11; i++) {
            String from = "/fail-redirect" + i;
            String to = "/fail-redirect" + (i + 1);
            this.wireMock.stubFor(get(urlEqualTo(from)).willReturn(permanentRedirect(to)));
        }

        String redirectUrl = this.wireMock.baseUrl() + "/fail-redirect1";
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", redirectUrl), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining("Too many redirects");
    }

    @Test
    void shouldChunkMultipleLinesIntoSingleBatch() throws IOException {
        // "One", "Two", "Three" are each ~1 token
        // With limit = 2: "One"+"Two" (2 tokens) accumulates into chunk 0; "Three" starts chunk 1.
        Path chunkFile = testDirectory.createFile("inside/chunk-batch-test.txt");
        Files.writeString(chunkFile, "One\nTwo\nThree");

        String embeddingResponse = """
                {"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]}""";

        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("One\\nTwo"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"Three\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));

        String query = """
                WITH { token: 'dummy-openai-token', model: 'gpt-4' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 2)
                YIELD index, vector, resource
                RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + chunkFile.getFileName()), consume()))
                .as("Two short lines should accumulate into one chunk; the third starts a new chunk")
                .containsExactly(
                        Map.of("vector", true, "index", 0L, "resource", "One\nTwo"),
                        Map.of("vector", true, "index", 1L, "resource", "Three"));
    }

    @Test
    void shouldChunkASingleLine() throws IOException {
        Path chunkFile = testDirectory.createFile("inside/chunk-batch-test.txt");
        String unbreakable = "a".repeat(1000);
        Files.writeString(chunkFile, unbreakable);
        String eighty_a = "a".repeat(80);
        String forty_a = "a".repeat(40);

        String embeddingResponse = """
                {"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]}""";

        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing(eighty_a))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing(forty_a))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));

        String query = """
                WITH { token: 'dummy-openai-token', model: 'gpt-4' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 10)
                YIELD index, vector, resource
                RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + chunkFile.getFileName()), consume()))
                .as("Two short lines should accumulate into one chunk; the third starts a new chunk")
                .containsExactly(
                        Map.of("vector", true, "index", 0L, "resource", eighty_a),
                        Map.of("vector", true, "index", 1L, "resource", eighty_a),
                        Map.of("vector", true, "index", 2L, "resource", eighty_a),
                        Map.of("vector", true, "index", 3L, "resource", eighty_a),
                        Map.of("vector", true, "index", 4L, "resource", eighty_a),
                        Map.of("vector", true, "index", 5L, "resource", eighty_a),
                        Map.of("vector", true, "index", 6L, "resource", eighty_a),
                        Map.of("vector", true, "index", 7L, "resource", eighty_a),
                        Map.of("vector", true, "index", 8L, "resource", eighty_a),
                        Map.of("vector", true, "index", 9L, "resource", eighty_a),
                        Map.of("vector", true, "index", 10L, "resource", eighty_a),
                        Map.of("vector", true, "index", 11L, "resource", eighty_a),
                        Map.of("vector", true, "index", 12L, "resource", forty_a));
    }

    @Test
    void shouldNotPrependNewlineAtChunkBoundaryAfterOversizedLine() throws IOException {
        String fileName = "mixed-chunk-test.txt";
        Path mixedFile = testDirectory.createFile("inside/" + fileName);
        Files.writeString(mixedFile, "One Two Three\nA");

        String embeddingResponse = """
                {"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]}""";
        // "One Two Three" (3 tokens) exceeds the limit of 2 and is split into sub-chunks.
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"One Two \""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"Three\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"A\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));

        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 2)
                YIELD index, resource
                RETURN index, resource
                """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + fileName), consume()))
                .as("The line following oversized sub-chunks must not start with a leading newline")
                .containsExactly(
                        Map.of("index", 0L, "resource", "One Two "),
                        Map.of("index", 1L, "resource", "Three"),
                        Map.of("index", 2L, "resource", "A"));
    }

    @Test
    void shouldChunkOversizedSingleLineIntoSmallerChunks() throws IOException {
        // A single line with more tokens than the limit is split via RecursiveTokenSplitter
        // into multiple sub-chunks, each within the limit.
        String fileName = "oversized-line-test.txt";
        Path file = testDirectory.createFile("inside/" + fileName);
        Files.writeString(file, "One Two Three Four");

        String embeddingResponse = """
                {"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]}""";
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"One Two \""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"Three Four\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));

        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 2)
                YIELD index, resource
                RETURN index, resource
                """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + fileName), consume()))
                .as("Oversized single line should be split into sub-chunks each within the limit")
                .containsExactly(
                        Map.of("index", 0L, "resource", "One Two "), Map.of("index", 1L, "resource", "Three Four"));
    }

    @Test
    void shouldFlushAccumulatedBucketBeforeChunkingOversizedLine() throws IOException {
        // Short lines accumulate into a bucket up to the limit. When an oversized line is
        // encountered, the accumulator must be flushed first, then the oversized line gets
        // split into its own sub-chunks.
        String fileName = "flush-then-chunk-test.txt";
        Path file = testDirectory.createFile("inside/" + fileName);
        Files.writeString(file, "A\nB\nOne Two Three Four\nC");

        String embeddingResponse = """
                {"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]}""";
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"A\\nB\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"One Two \""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"Three Four\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));
        this.wireMock.stubFor(post(urlEqualTo("/v1/embeddings"))
                .withRequestBody(containing("\"C\""))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(embeddingResponse)));

        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 2)
                YIELD index, resource
                RETURN index, resource
                """;

        assertThat(db.executeTransactionally(query, Map.of("file", "file:///" + fileName), consume()))
                .as("Accumulated lines should be flushed as a chunk before chunking the oversized line")
                .containsExactly(
                        Map.of("index", 0L, "resource", "A\nB"),
                        Map.of("index", 1L, "resource", "One Two "),
                        Map.of("index", 2L, "resource", "Three Four"),
                        Map.of("index", 3L, "resource", "C"));
    }

    @Test
    void shouldHandleEmptyFile() {
        var result = db.executeTransactionally(
                QUERY,
                Map.of("file", "file:///" + testFileEmpty.getFileName()),
                res -> res.stream().toList());
        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("index")).isEqualTo(0L);
        assertThat(row.get("resource")).isNull();
        assertThat(row.get("vector")).isNull();
    }

    @Test
    void shouldHandleEmptyFileWithChunking() {
        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf, 10)
                YIELD index, resource, vector
                RETURN index, resource, vector
                """;

        var result = db.executeTransactionally(
                query,
                Map.of("file", "file:///" + testFileEmpty.getFileName()),
                res -> res.stream().toList());
        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("index")).isEqualTo(0L);
        assertThat(row.get("resource")).isNull();
        assertThat(row.get("vector")).isNull();
    }

    @Test
    void shouldErrorOnPathWithoutFileScheme() {
        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf)
                YIELD index, vector, resource
                RETURN *
                """;
        assertThatThrownBy(() -> db.executeTransactionally(query, Map.of("file", testFileName), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining(" Missing protocol: `test.txt`. Files must be prepended with `file:///");
    }

    @Test
    void shouldFailForNonExistentFile() {
        String query = """
                WITH { token: 'dummy-openai-token', model: 'text-embedding-3-small' } AS conf
                CALL ai.file.embedBatch($file, 'openai', conf)
                YIELD index, vector, resource
                RETURN *
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of("file", "file:///this/path/does/not/exist.txt"), res -> {
                            res.accept(row -> true);
                            return null;
                        }))
                .hasMessageContaining("File not found: file:///this/path/does/not/exist.txt");
    }

    @Test
    void shouldFailIfProtocolChangesDuringRedirect() {
        int ftpPort = ftpServer.getServerControlPort();
        String ftpUrl = String.format("ftp://user:pass@localhost:%d/ftp-test.txt", ftpPort);

        this.wireMock.stubFor(get(urlEqualTo("/redirect-to-ftp")).willReturn(permanentRedirect(ftpUrl)));

        String redirectUrl = this.wireMock.baseUrl() + "/redirect-to-ftp";
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", redirectUrl), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining("Protocol change during redirect is not allowed for security reasons.");
    }

    @Test
    void shouldNotReadFilesOutsideImportDirectory() {
        Path outsideFile = outsideDirectory.resolve("test.txt");

        // The outside file is remapped to a non-existent path under insideDirectory
        assertThatThrownBy(() -> db.executeTransactionally(
                        QUERY, Map.of("file", outsideFile.toUri().toString()), res -> {
                            while (res.hasNext()) res.next();
                            return null;
                        }))
                .hasMessageContaining("File not found: " + outsideFile.toUri().toString());
    }

    @Test
    void shouldFailForFileUriWithAuthority() {
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", "file://host/some/file.txt"), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("file URL may not contain an authority section");
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:///some/file.txt?malicious=true
        file:/file?csv
        file:/?csv
        file:///?csv
        """)
    void shouldFailForFileUriWithQueryString(String path) {
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("file URL may not contain a query component");
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:///import/file%253Fcsv
        file:///import/%253Fcsv
        file:///import/%253F
        file:///import/%253F
        file:///import/file%25253Fcsv
        file:///import/file%2525253Fcsv
        file:///import/file%252525253Fcsv
        """)
    void shouldNotFailForFileUriWithQueryString(String path) {
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("File not found: " + path);
    }

    @DisabledOnOs(OS.WINDOWS)
    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/file%3Fcsv", "file:///import/file%3Fcsv
        file:/%3Fcsv", "file:///import/%3Fcsv
        file:///%3F", "file:///import/%3F
        file:/%3F", "file:///import/%3F
        """)
    void shouldNotFailForFileUriWithEncodedQueryChar(String path) {
        // %3F decodes to '?' which is a reserved character in Windows file names; skip on Windows
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("File not found: " + path);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:///some/file.txt#section
        file:/file#csv
        file:/#csv
        file:///#csv
        file:/#
        """)
    void shouldFailForFileUriWithFragment(String path) {
        // FileURIAccessRule rejects URIs with fragments
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("URI has a fragment component");
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/file%23csv
        file:/file%23csv
        file:/%23csv
        file:/%2523
        file:///%23csv
        file:/%23
        file:/file%2523csv
        file:/%2523csv
        file:///%2523csv
        file:/file%252523csv
        file:/file%25252523csv
        file:/file%2525252523csv
        """)
    void shouldNotFailForFileUriWithFragment(String path) {
        // As these are not rejected by the fragment error, they will just be not found
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("File not found: " + path);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file://localhost/some/file.txt
        file://dir1/file.csv
        file://w.me.com:80/file.csv
        file://me.com/file%252Ecsv
        file://[::]:80/file.csv
        file://%2F///////file.csv
        file://[1:1:1:1:1:1:1:1]:80/file.csv
        """)
    void shouldFailForFileUriWithLocalhostAuthority(String path) {
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("file URL may not contain an authority section");
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/file.csv
        file:///file.csv
        file:/%252F%252Ffile.csv
        file:/localhost/file.csv
        """)
    void shouldNotFailForFileUriWithLocalhostAuthority(String path) {
        // As these are not rejected by the authority section error, they will just be not found
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("File not found: " + path);
    }

    @DisabledOnOs(OS.WINDOWS)
    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/%2Ffile.csv
        file:/w.me.com:80/file.csv
        """)
    void shouldNotFailForFileUriWithWindowsIncompatiblePath(String path) {
        // file:/%2Ffile.csv triggers InvalidPathException on Windows (encoded leading slash);
        // file:/w.me.com:80/file.csv has ':' in the path which is reserved on Windows
        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", path), res -> {
                    while (res.hasNext()) res.next();
                    return null;
                }))
                .hasMessageContaining("File not found: " + path);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/test.txt
        file:///test.txt
        file:////test.txt
        file://///////test.txt
        """)
    void fileWithLeadingSlashesShouldBeNormalized(String path) {
        var result = db.executeTransactionally(
                QUERY, Map.of("file", path), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @DisabledOnOs(OS.WINDOWS)
    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/%2F%2Ftest.txt
        file:/%2F/test.txt
        file:/%2F/test.txt
        file:/%2F%2Ftest.txt
        file:/%2Ftest.txt
        file:/%2F%2Ftest.txt
        file:///%2F//%2F///test.txt
        file:/%2F////%2F/////test.txt
        file:/%2F%2F%2F%2F%2F%2F%2F%2Ftest.txt
        file:///%2F%2F%2F%2F%2F%2F%2Ftest.txt
        """)
    void fileWithEncodedLeadingSlashesShouldBeNormalized(String path) {
        // Paths with %2F (encoded '/') immediately after the scheme slashes trigger InvalidPathException
        // on Windows; these are valid on Unix/macOS only
        var result = db.executeTransactionally(
                QUERY, Map.of("file", path), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/test.txt
        file:/test.txt/
        file:/test.txt//
        file:/test.txt///
        file:/test.txt////
        file:///test.txt
        file:///test.txt/
        file:///test.txt//
        file:///test.txt///
        file:///test.txt////
        file:////test.txt
        file:////test.txt/
        file:////test.txt//
        file:////test.txt///
        file:////test.txt////
        file:///test.txt%2F
        file:///test.txt%2F%2F%2F%2F%2F
        """)
    void fileWithTrailingSlashesShouldBeNormalized(String path) {
        var result = db.executeTransactionally(
                QUERY, Map.of("file", path), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        file:/./test.txt
        file:/../~/../test.txt
        file:///./test.txt
        file:/../test.txt
        file:///../test.txt
        file:/../../test.txt
        file:///../../test.txt
        file:/dir1/../../test.txt
        file:///dir1/../../test.txt
        file:/..//dir1/../../test.txt
        file:/../../dir1/../../test.txt
        file:///import/../../test.txt
        file:///..//test.txt
        file:///..//..//..//..//test.txt
        file:////..//test.txt
        file:////..//..//test.txt
        file:////..//..//..//..//test.txt
        file:////..//..//..//../test.txt
        file:////..//..//../..//test.txt
        file:////../..//../..//test.txt
        file:////../../../..//test.txt
        file:////../../../../test.txt
        file:////..//../..//../../test.txt
        file://///..//test.txt
        file://///..//..//test.txt
        file://///..//..//..//..//test.txt
        file:/..%2Ftest.txt
        file:////..//..%2F%2Ftest.txt
        file:////../%2F..%2F/test.txt
        file:////%2e%2e//%2e%2e//test.txt
        file:////..%2F%2F..%2F%2Ftest.txt
        file:////%2E%2E%2F%2F%2E%2E%2F%2Ftest.txt
        file://////..//..//..//..//test.txt
        file:/..///..///..///test.txt
        file://///..///test.txt
        file://///..///..///test.txt
        file://///..///..///..///test.txt
        """)
    void fileWithTraversalShouldBeNormalizedWithinRootFolder(String path) {
        var result = db.executeTransactionally(
                QUERY, Map.of("file", path), res -> res.stream().toList());

        assertThat(result).isNotEmpty();
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedLocalZipFile() throws IOException {
        Path zipFile = testDirectory.createFile("inside/test.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("content.txt"));
            zos.write(fileText.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        var result = db.executeTransactionally(
                QUERY, Map.of("file", "file:///test.zip"), res -> res.stream().toList());

        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedLocalGzipFile() throws IOException {
        Path gzFile = testDirectory.createFile("inside/test.txt.gz");
        try (var gos = new GZIPOutputStream(Files.newOutputStream(gzFile))) {
            gos.write(fileText.getBytes(StandardCharsets.UTF_8));
        }

        var result = db.executeTransactionally(
                QUERY,
                Map.of("file", "file:///test.txt.gz"),
                res -> res.stream().toList());

        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedZipFileFromWebUrl() throws IOException {
        byte[] zipBytes = createZipBytes("content.txt", fileText);
        this.wireMock.stubFor(get(urlEqualTo("/web-test.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/zip")
                        .withBody(zipBytes)));

        String webUrl = this.wireMock.baseUrl() + "/web-test.zip";
        var result = db.executeTransactionally(
                QUERY, Map.of("file", webUrl), res -> res.stream().toList());

        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldEmbedGzipFileFromWebUrl() throws IOException {
        byte[] gzBytes = createGzipBytes(fileText);
        this.wireMock.stubFor(get(urlEqualTo("/web-test.txt.gz"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/gzip")
                        .withBody(gzBytes)));

        String webUrl = this.wireMock.baseUrl() + "/web-test.txt.gz";
        var result = db.executeTransactionally(
                QUERY, Map.of("file", webUrl), res -> res.stream().toList());

        assertThat(result).hasSize(1);
        var row = result.getFirst();
        assertThat(row.get("resource")).isEqualTo(fileText);
        assertThat(row.get("vector")).isNotNull();
    }

    @Test
    void shouldFailForZipWithMultipleEntries() throws IOException {
        Path zipFile = testDirectory.createFile("inside/multiple.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("file1.txt"));
            zos.write("content1".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("file2.txt"));
            zos.write("content2".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        assertThatThrownBy(() -> db.executeTransactionally(
                        QUERY,
                        Map.of("file", "file:///multiple.zip"),
                        res -> res.stream().toList()))
                .hasMessageContaining("ZIP archive contains more than one file");
    }

    @Test
    void shouldFailForZipWithNoSuitableEntry() throws IOException {
        Path zipFile = testDirectory.createFile("inside/empty.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("emptydir/"));
            zos.closeEntry();
        }

        assertThatThrownBy(() -> db.executeTransactionally(QUERY, Map.of("file", "file:///empty.zip"), res -> {
                    res.accept(row -> true);
                    return null;
                }))
                .hasMessageContaining("No suitable file found in ZIP archive");
    }

    @Test
    void shouldSkipMacOsJunkAndUseFirstSuitableZipEntry() throws IOException {
        Path zipFile = testDirectory.createFile("inside/macos.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("__MACOSX/._content.txt"));
            zos.write(new byte[0]);
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("content.txt"));
            zos.write(fileText.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        var result = db.executeTransactionally(
                QUERY, Map.of("file", "file:///macos.zip"), res -> res.stream().toList());

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().get("resource")).isEqualTo(fileText);
    }

    @Test
    void shouldFailForLargeZipFile() throws IOException {
        Path zipFile = testDirectory.createFile("inside/large.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("large.txt"));
            // 100 MiB + 1 byte
            long size = 100 * 1024 * 1024 + 1;
            byte[] buffer = new byte[8192];
            for (long i = 0; i < size; i += buffer.length) {
                int toWrite = (int) Math.min(buffer.length, size - i);
                zos.write(buffer, 0, toWrite);
            }
            zos.closeEntry();
        }

        assertThatThrownBy(() -> db.executeTransactionally(
                        QUERY,
                        Map.of("file", "file:///large.zip"),
                        res -> res.stream().toList()))
                .hasMessageContaining("Decompressed file size exceeds the limit of 100.0MiB");
    }

    @Test
    void shouldFailForLargeGzipFile() throws IOException {
        Path gzipFile = testDirectory.createFile("inside/large.gz");
        try (var gos = new GZIPOutputStream(Files.newOutputStream(gzipFile))) {
            // 100 MiB + 1 byte
            long size = 100 * 1024 * 1024 + 1;
            byte[] buffer = new byte[8192];
            for (long i = 0; i < size; i += buffer.length) {
                int toWrite = (int) Math.min(buffer.length, size - i);
                gos.write(buffer, 0, toWrite);
            }
        }

        assertThatThrownBy(() -> db.executeTransactionally(
                        QUERY,
                        Map.of("file", "file:///large.gz"),
                        res -> res.stream().toList()))
                .hasMessageContaining("Decompressed file size exceeds the limit of 100.0MiB");
    }

    private static byte[] createZipBytes(String entryName, String content) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry(entryName));
            zos.write(content.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        return baos.toByteArray();
    }

    private static byte[] createGzipBytes(String content) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var gos = new GZIPOutputStream(baos)) {
            gos.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return baos.toByteArray();
    }
}
