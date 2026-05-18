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
package org.neo4j.genai.ai.image.embed;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.genai.GenAiPluginExtension;
import org.neo4j.genai.ai.ProviderArgs;
import org.neo4j.genai.ai.ProviderArguments;
import org.neo4j.genai.ai.image.embed.provider.bedrock.BedrockTitanImage;
import org.neo4j.genai.ai.image.embed.provider.vertexai.VertexAiImage;
import org.neo4j.genai.util.GenAITestExtension;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImpermanentDbmsExtension(configurationCallback = "configure")
public class ImageVectorEmbeddingTest implements GenAITestExtension {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private TestDirectory testDirectory;

    private WireMockServer wireMock;

    static final String FAKE_IMAGE = "dGVzdA==";

    private Path insideDirectory;

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        installPlugin(testDirectory);
        insideDirectory = testDirectory.directory("inside");
        Files.createDirectories(insideDirectory);
        // Write the 4 bytes of "test" — base64-encodes to "dGVzdA==" which matches the WireMock stubs
        Files.write(insideDirectory.resolve("test-image.bin"), new byte[] {116, 101, 115, 116});
        this.wireMock = new WireMockServer(options()
                .dynamicPort()
                .usingFilesUnderClasspath("wiremock/image-embeddings")
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new VertexAiImage(p -> URI.create(baseUrl)), new BedrockTitanImage(p -> URI.create(baseUrl))));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25);
        builder.setConfig(GraphDatabaseSettings.allow_file_urls, true);
        builder.setConfig(GraphDatabaseSettings.load_csv_file_url_root, insideDirectory.toAbsolutePath());
    }

    @AfterAll
    void stopWireMock() {
        if (this.wireMock != null) this.wireMock.stop();
    }

    private final List<Map<String, Object>> EXPECTED_PROVIDERS = List.of(
            Map.of(
                    "name",
                    "Bedrock-Titan",
                    "requiredConfigType",
                    """
                    {
                      accessKeyId :: STRING NOT NULL,
                      secretAccessKey :: STRING NOT NULL,
                      region :: STRING NOT NULL,
                      model :: STRING NOT NULL
                    }""",
                    "optionalConfigType",
                    """
                    {
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of())),
            Map.of(
                    "name",
                    "VertexAI",
                    "requiredConfigType",
                    """
                    {
                      model :: STRING NOT NULL,
                      project :: STRING NOT NULL,
                      region :: STRING NOT NULL
                    }""",
                    "optionalConfigType",
                    """
                    {
                      token :: STRING,
                      apiKey :: STRING,
                      publisher :: STRING NOT NULL,
                      maxBatchSize :: INTEGER NOT NULL,
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("maxBatchSize", -1L, "publisher", "google", "vendorOptions", Map.of())));

    @Test
    void listProviders() {
        assertThat(db.executeTransactionally("CALL ai.image.embed.providers()", Map.of(), consume()))
                .containsExactlyElementsOf(EXPECTED_PROVIDERS);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.image.embed('%s', '%s', conf) IS :: VECTOR<FLOAT32> AS result""".formatted(args.conf(), FAKE_IMAGE, args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageAllOptionsArguments.class)
    void embeddingWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.image.embed('%s', '%s', conf) IS :: VECTOR<FLOAT32> AS result""".formatted(args.conf(), FAKE_IMAGE, args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void batchEmbeddingWithRequiredArgs1(ProviderArgs args) {
        final var query = """
                    CALL ai.image.embedBatch(
                      ['%s'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(FAKE_IMAGE, args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", FAKE_IMAGE));
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void batchEmbeddingWithRequiredArgs2(ProviderArgs args) {
        final var query = """
                    CALL ai.image.embedBatch(
                      [null, '%s'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    LET vectorResult = CASE WHEN vector IS NULL THEN null ELSE vector IS :: VECTOR<FLOAT32> END
                    RETURN index, resource, vectorResult AS vector
                """.formatted(FAKE_IMAGE, args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Maps.immutable
                                .of("vector", null, "index", (Object) 0L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 1L, "resource", FAKE_IMAGE));
    }

    @ParameterizedTest
    @ArgumentsSource(ImageAllOptionsArguments.class)
    void batchEmbeddingWithAllArgs1(ProviderArgs args) {
        final var query = """
                    CALL ai.image.embedBatch(
                      ['%s'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(FAKE_IMAGE, args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", FAKE_IMAGE));
    }

    @ParameterizedTest
    @ArgumentsSource(ImageAllOptionsArguments.class)
    void batchEmbeddingWithAllArgs2(ProviderArgs args) {
        final var query = """
                    CALL ai.image.embedBatch(
                      [null, '%s'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    LET vectorResult = CASE WHEN vector IS NULL THEN null ELSE vector IS :: VECTOR<FLOAT32> END
                    RETURN index, resource, vectorResult AS vector
                """.formatted(FAKE_IMAGE, args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Maps.immutable
                                .of("vector", null, "index", (Object) 0L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 1L, "resource", FAKE_IMAGE));
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingWithNullResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.image.embed(null, '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingBatchWithNullResources(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.image.embedBatch(null, '%s', conf)
                YIELD index, vector, resource
                RETURN vector AS result""".formatted(args.conf(), args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'resources' must not be null");
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingWithEmptyStringResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.image.embed('', '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingBatchWithEmptyStringResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.image.embedBatch([''], '%s', conf)
                YIELD index, vector, resource
                RETURN vector AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(ImageRequiredConfArguments.class)
    void embeddingBatchWithEmptyList(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.image.embedBatch([], '%s', conf)
                YIELD index, vector, resource
                RETURN count(index) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", 0L);
    }

    @Test
    void invalidProviderEmbed() {
        final var query1 = "RETURN ai.image.embed('%s', 'FakeProvider', {}) AS result".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Provider not supported: FakeProvider");

        final var query2 = "RETURN ai.image.embed('%s', null, {}) AS result".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'provider' must not be null");
    }

    @Test
    void invalidProviderEmbedBatch() {
        final var query1 = "CALL ai.image.embedBatch(['%s'], 'FakeProvider', {})".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Provider not supported: FakeProvider");

        final var query2 = "CALL ai.image.embedBatch(['%s'], null, {})".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'provider' must not be null");
    }

    @Test
    void invalidArgumentsEmbed() {
        final var query1 = "RETURN ai.image.embed('%s', 'VertexAI', 1) AS result".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Type mismatch: expected Map");

        final var query2 = "RETURN ai.image.embed('%s', 'VertexAI', null) AS result".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'configuration' must not be null");
    }

    @Test
    void invalidArgumentsEmbedBatch() {
        final var query1 = "CALL ai.image.embedBatch(['%s'], 'VertexAI', 1)".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Type mismatch: expected Map");

        final var query2 = "CALL ai.image.embedBatch(['%s'], 'VertexAI', null)".formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'configuration' must not be null");
    }

    @ParameterizedTest
    @ArgumentsSource(ImageMissingSecretsArguments.class)
    void missingSecretsEmbed(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.image.embed('%s', '%s', conf) AS result
                """.formatted(args.conf(), FAKE_IMAGE, args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*'(accessKeyId|secretAccessKey|token or apiKey)' is expected to have been set");
    }

    @ParameterizedTest
    @ArgumentsSource(ImageMissingSecretsArguments.class)
    void missingSecretsEmbedBatch(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.image.embedBatch(['%s'], '%s', conf)
                YIELD index, vector, resource
                RETURN index, vector, resource""".formatted(args.conf(), FAKE_IMAGE, args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*'(accessKeyId|secretAccessKey|token or apiKey)' is expected to have been set");
    }

    @Test
    void incorrectVertexAiAuth() {
        final var query = """
                WITH { token: 'token', apiKey: 'key', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('%s', 'vertexai', conf) AS result
                """.formatted(FAKE_IMAGE);
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*Only one of either 'token' or 'apiKey' is expected to have been set");
    }

    @Test
    void embeddingFromLocalFile() {
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///test-image.bin', 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void batchEmbeddingFromLocalFile() {
        final var query = """
                CALL ai.image.embedBatch(
                  ['file:///test-image.bin'],
                  'vertexai',
                  { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' }
                )
                YIELD index, vector, resource
                RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", FAKE_IMAGE));
    }

    @Test
    void embeddingFromHttpUrl() {
        wireMock.stubFor(get(urlEqualTo("/test-image.bin"))
                .willReturn(aResponse().withStatus(200).withBody("test")));
        final var imageUrl = wireMock.baseUrl() + "/test-image.bin";
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed($url, 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of("url", imageUrl), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void embeddingFromMissingFile() {
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///does-not-exist.bin', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("File not found: file:///does-not-exist.bin");
    }

    @Test
    void shouldEmbedLocalZipFile() throws IOException {
        Path zipFile = testDirectory.createFile("inside/test-image.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("test-image.bin"));
            zos.write(new byte[] {116, 101, 115, 116});
            zos.closeEntry();
        }

        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///test-image.zip', 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void shouldEmbedZipFromResource() throws IOException {
        Path zipFile = testDirectory.directory("inside").resolve("pelle.jpeg.zip");
        Files.copy(getClass().getResourceAsStream("/org/neo4j/genai/ai/image/embed/pelle.jpeg.zip"), zipFile);

        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///pelle.jpeg.zip', 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;

        wireMock.stubFor(post(urlEqualTo(
                        "/v1/projects/gem/locations/tasman/publishers/google/models/multimodalembedding:predict"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"predictions\": [{\"imageEmbedding\": [" + "0.1,".repeat(127) + "0.1]}]}")));

        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void shouldEmbedLocalGzipFile() throws IOException {
        Path gzFile = testDirectory.createFile("inside/test-image.bin.gz");
        try (var gos = new GZIPOutputStream(Files.newOutputStream(gzFile))) {
            gos.write(new byte[] {116, 101, 115, 116});
        }

        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///test-image.bin.gz', 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void shouldEmbedZipFileFromWebUrl() throws IOException {
        byte[] zipBytes = createZipBytes("test-image.bin", new byte[] {116, 101, 115, 116});
        this.wireMock.stubFor(get(urlEqualTo("/web-test.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/zip")
                        .withBody(zipBytes)));

        String webUrl = this.wireMock.baseUrl() + "/web-test.zip";
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed($url, 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of("url", webUrl), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void shouldEmbedGzipFileFromWebUrl() throws IOException {
        byte[] gzBytes = createGzipBytes(new byte[] {116, 101, 115, 116});
        this.wireMock.stubFor(get(urlEqualTo("/web-test.bin.gz"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/gzip")
                        .withBody(gzBytes)));

        String webUrl = this.wireMock.baseUrl() + "/web-test.bin.gz";
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed($url, 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of("url", webUrl), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void shouldFailForZipWithMultipleEntries() throws IOException {
        Path zipFile = testDirectory.createFile("inside/multiple.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("file1.bin"));
            zos.write(new byte[] {1, 2});
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("file2.bin"));
            zos.write(new byte[] {3, 4});
            zos.closeEntry();
        }

        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///multiple.zip', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("ZIP archive contains more than one file");
    }

    @Test
    void shouldFailForZipWithNoSuitableEntry() throws IOException {
        Path zipFile = testDirectory.createFile("inside/empty.zip");
        try (var zos = new ZipOutputStream(Files.newOutputStream(zipFile))) {
            zos.putNextEntry(new ZipEntry("emptydir/"));
            zos.closeEntry();
        }

        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.image.embed('file:///empty.zip', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("No suitable file found in ZIP archive");
    }

    private static byte[] createZipBytes(String entryName, byte[] content) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry(entryName));
            zos.write(content);
            zos.closeEntry();
        }
        return baos.toByteArray();
    }

    private static byte[] createGzipBytes(byte[] content) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var gos = new GZIPOutputStream(baos)) {
            gos.write(content);
        }
        return baos.toByteArray();
    }

    @Test
    void embeddingDocsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var expectedFunctionSignature =
                "ai.image.embed(resource :: STRING, provider :: STRING, configuration = {} :: MAP) :: VECTOR";
        final var showFuncQuery = """
                        SHOW FUNCTIONS YIELD name, argumentDescription, signature
                        WHERE name = 'ai.image.embed'
                        RETURN *
                        """;
        assertThat(db.executeTransactionally(showFuncQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .containsEntry("signature", expectedFunctionSignature)
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map m && "provider".equals(m.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);

        final var expectedProcedureSignature =
                "ai.image.embedBatch(resources :: LIST<STRING>, provider :: STRING, configuration = {} :: MAP) :: (index :: INTEGER, resource :: STRING, vector :: VECTOR)";
        final var showProcQuery = """
                        SHOW PROCEDURES YIELD name, argumentDescription, signature
                        WHERE name = 'ai.image.embedBatch'
                        RETURN *
                        """;
        assertThat(db.executeTransactionally(showProcQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .containsEntry("signature", expectedProcedureSignature)
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map m && "provider".equals(m.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);
    }
}

class ImageRequiredConfArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "vertexai:token",
                        "{ token: 'dummy-vertex-token', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' }"),
                new ProviderArgs(
                        "vertexai:apiKey",
                        "{ apiKey: 'dummy-api-key', model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' }"),
                new ProviderArgs(
                        "bedrock-titan",
                        "{ model: 'amazon.titan-embed-image-v1', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"));
    }
}

class ImageAllOptionsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(new ProviderArgs("vertexai:token", """
                        {
                          token: 'dummy-vertex-token',
                          model: 'multimodalembedding',
                          region: 'tasman',
                          project: 'gem',
                          publisher: 'google',
                          vendorOptions: { autoTruncate: true, task_type: 'QUESTION_ANSWERING' }
                        }"""), new ProviderArgs("vertexai:apiKey", """
                        {
                          apiKey: 'dummy-api-key',
                          model: 'multimodalembedding',
                          region: 'tasman',
                          project: 'gem',
                          publisher: 'google',
                          vendorOptions: { autoTruncate: true, task_type: 'QUESTION_ANSWERING' }
                        }"""));
    }
}

class ImageMissingSecretsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "vertexai",
                        "{ model: 'multimodalembedding', region: 'tasman', project: 'gem', publisher: 'google' }"),
                new ProviderArgs("bedrock-titan", "{ model: 'amazon.titan-embed-image-v1', region: 'eu-north-1' }"));
    }
}
