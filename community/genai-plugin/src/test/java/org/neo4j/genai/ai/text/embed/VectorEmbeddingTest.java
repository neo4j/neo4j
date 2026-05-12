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
package org.neo4j.genai.ai.text.embed;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.GenAiPluginExtension;
import org.neo4j.genai.ai.ProviderArgs;
import org.neo4j.genai.ai.ProviderArguments;
import org.neo4j.genai.ai.text.embed.provider.azure.AzureOpenAi;
import org.neo4j.genai.ai.text.embed.provider.bedrock.BedrockTitan;
import org.neo4j.genai.ai.text.embed.provider.openai.OpenAi;
import org.neo4j.genai.ai.text.embed.provider.vertexai.VertexAi;
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
public class VectorEmbeddingTest implements GenAITestExtension {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private TestDirectory testDirectory;

    private WireMockServer wireMock;

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        installPlugin(testDirectory);
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
    }

    @BeforeEach
    void setup() {
        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
    }

    @AfterAll
    void stopWireMock() {
        if (this.wireMock != null) this.wireMock.stop();
    }

    private final List<Map<String, Object>> EXPECTED_PROVIDERS = List.of(
            Map.of(
                    "name",
                    "Azure-OpenAI",
                    "requiredConfigType",
                    """
                    {
                      token :: STRING NOT NULL,
                      resource :: STRING NOT NULL,
                      model :: STRING NOT NULL
                    }""",
                    "optionalConfigType",
                    """
                    {
                      maxBatchSize :: INTEGER NOT NULL,
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("maxBatchSize", 8192L, "vendorOptions", Map.of())),
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
                    "OpenAI",
                    "requiredConfigType",
                    """
                    {
                      token :: STRING NOT NULL,
                      model :: STRING NOT NULL
                    }""",
                    "optionalConfigType",
                    """
                    {
                      maxBatchSize :: INTEGER NOT NULL,
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("maxBatchSize", 8192L, "vendorOptions", Map.of())),
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
        assertThat(db.executeTransactionally("CALL ai.text.embed.providers()", Map.of(), consume()))
                .containsExactlyElementsOf(EXPECTED_PROVIDERS);
    }

    @Test
    void openAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, "http://localhost/v1");
        final var query1 = """
                with { token: 'dummy-openai-token', model: 'text-embedding-3-small' } as conf
                return ai.text.embed('Hello!', 'openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.embed`");

        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/v1");
        final var query2 = """
                with { token: 'dummy-openai-token', model: 'text-embedding-3-small' } as conf
                return ai.text.embed('Hello!', 'openai', conf) IS :: VECTOR<FLOAT32> as result
                """;
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void azureOpenAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, "http://localhost/%s");
        final var query1 = """
                with { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'text-embedding-ada-002' } as conf
                return ai.text.embed('Hello!', 'azure-openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.embed`");

        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
        final var query2 = """
                with { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'text-embedding-3-small' } as conf
                return ai.text.embed('Hello!', 'azure-openai', conf) IS :: VECTOR<FLOAT32> as result
                """;
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @ParameterizedTest
    @ArgumentsSource(BatchWithMaxBatchSizeArguments.class)
    void batchEmbeddingWithMaxBatchSizeShouldProcessInMultipleBatches(ProviderArgs args) {
        // "Hello 1" is 2 tokens.
        // Setting maxBatchSize to 1 will force each "Hello 1" into its own batch because it exceeds the limit.
        final var query = """
                    CALL ai.text.embedBatch(
                      ['Hello 1', 'Hello 2'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(args.provider(), args.conf());

        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Map.of("vector", true, "index", 0L, "resource", "Hello 1"),
                        Map.of("vector", true, "index", 1L, "resource", "Hello 2"));
    }

    @ParameterizedTest
    @ArgumentsSource(BatchWithMaxBatchSizeArguments2.class)
    void batchEmbeddingWithMaxBatchSizeShouldProcessInMultipleBatches2(ProviderArgs args) {
        // "Hello 1" is 2 tokens.
        // Setting maxBatchSize to 10 will force each 5 groups of "Hello %d" into their own batch.
        final var query = """
                    CALL ai.text.embedBatch(
                      [
                      'Hello 1',
                      'Hello 2',
                      'Hello 3',
                      'Hello 4',
                      'Hello 5',
                      'Hello 6',
                      'Hello 7',
                      'Hello 8',
                      'Hello 9',
                      'Hello 10'
                      ],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(args.provider(), args.conf());

        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Map.of("vector", true, "index", 0L, "resource", "Hello 1"),
                        Map.of("vector", true, "index", 1L, "resource", "Hello 2"),
                        Map.of("vector", true, "index", 2L, "resource", "Hello 3"),
                        Map.of("vector", true, "index", 3L, "resource", "Hello 4"),
                        Map.of("vector", true, "index", 4L, "resource", "Hello 5"),
                        Map.of("vector", true, "index", 5L, "resource", "Hello 6"),
                        Map.of("vector", true, "index", 6L, "resource", "Hello 7"),
                        Map.of("vector", true, "index", 7L, "resource", "Hello 8"),
                        Map.of("vector", true, "index", 8L, "resource", "Hello 9"),
                        Map.of("vector", true, "index", 9L, "resource", "Hello 10"));
    }

    @ParameterizedTest
    @ArgumentsSource(BatchWithMaxBatchSizeArguments2.class)
    void batchEmbeddingWithMaxBatchSizeShouldProcessInMultipleBatchesWithNulls(ProviderArgs args) {
        // "Hello 1" is 2 tokens.
        // Setting maxBatchSize to 10 will force each 5 groups of "Hello %d" into their own batch.
        final var query = """
                    CALL ai.text.embedBatch(
                      [
                      null,
                      'Hello 1',
                      null,
                      null,
                      'Hello 2',
                      'Hello 3',
                      null,
                      null,
                      'Hello 4',
                      'Hello 5',
                      'Hello 6',
                      'Hello 7',
                      null,
                      'Hello 8',
                      'Hello 9',
                      null,
                      null,
                      null,
                      'Hello 10',
                      null
                      ],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> NOT NULL AS vector
                """.formatted(args.provider(), args.conf());

        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Maps.immutable
                                .of("vector", false, "index", (Object) 0L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 1L, "resource", "Hello 1"),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 2L, "resource", null)
                                .castToMap(),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 3L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 4L, "resource", "Hello 2"),
                        Map.of("vector", true, "index", 5L, "resource", "Hello 3"),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 6L, "resource", null)
                                .castToMap(),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 7L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 8L, "resource", "Hello 4"),
                        Map.of("vector", true, "index", 9L, "resource", "Hello 5"),
                        Map.of("vector", true, "index", 10L, "resource", "Hello 6"),
                        Map.of("vector", true, "index", 11L, "resource", "Hello 7"),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 12L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 13L, "resource", "Hello 8"),
                        Map.of("vector", true, "index", 14L, "resource", "Hello 9"),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 15L, "resource", null)
                                .castToMap(),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 16L, "resource", null)
                                .castToMap(),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 17L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 18L, "resource", "Hello 10"),
                        Maps.immutable
                                .of("vector", false, "index", (Object) 19L, "resource", null)
                                .castToMap());
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.embed('Hello!', '%s', conf) IS :: VECTOR<FLOAT32> AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void embeddingWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s as conf
                RETURN ai.text.embed('Hello!', '%s', conf) IS :: VECTOR<FLOAT32> AS result
                """.formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void batchEmbeddingWithRequiredArgs1(ProviderArgs args) {
        final var query = """
                    CALL ai.text.embedBatch(
                      ['Hello!'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", "Hello!"));
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void batchEmbeddingWithRequiredArgs2(ProviderArgs args) {
        final var query = """
                    CALL ai.text.embedBatch(
                      [null, 'Hello!'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    LET vectorResult = CASE WHEN vector IS NULL THEN null ELSE vector IS :: VECTOR<FLOAT32> END
                    RETURN index, resource, vectorResult AS vector
                """.formatted(args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Maps.immutable
                                .of("vector", null, "index", (Object) 0L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 1L, "resource", "Hello!"));
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void batchEmbeddingWithAllArgs1(ProviderArgs args) {
        final var query = """
                    CALL ai.text.embedBatch(
                      ['Hello!'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """.formatted(args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", "Hello!"));
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void batchEmbedWithAllArgs2(ProviderArgs args) {
        final var query = """
                    CALL ai.text.embedBatch(
                      [null, 'Hello!'],
                      '%s',
                      %s
                    )
                    YIELD index, vector, resource
                    LET vectorResult = CASE WHEN vector IS NULL THEN null ELSE vector IS :: VECTOR<FLOAT32> END
                    RETURN index, resource, vectorResult AS vector
                """.formatted(args.provider(), args.conf());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(
                        Maps.immutable
                                .of("vector", null, "index", (Object) 0L, "resource", null)
                                .castToMap(),
                        Map.of("vector", true, "index", 1L, "resource", "Hello!"));
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingWithNullResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.embed(null, '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingBatchWithNullResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.text.embedBatch(null, '%s', conf)
                YIELD index, vector, resource
                RETURN vector AS result""".formatted(args.conf(), args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'resources' must not be null");
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingWithEmptyStringResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.embed('', '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingBatchWithEmptyStringResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.text.embedBatch([''], '%s', conf)
                YIELD index, vector, resource
                RETURN vector AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void embeddingBatchWithEmptyResource(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.text.embedBatch([], '%s', conf)
                YIELD index, vector, resource
                RETURN count(index) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", 0L);
    }

    @Test
    void invalidProviderEmbed() {
        final var query1 = "RETURN ai.text.embed('Hello world', 'FakeProvider', {}) AS result";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Provider not supported: FakeProvider");

        final var query2 = "RETURN ai.text.embed('Hello world', null, {}) AS result";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'provider' must not be null");
    }

    @Test
    void invalidProviderEmbedBatch() {
        final var query1 = "CALL ai.text.embedBatch(['Hello world'], 'FakeProvider', {})";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Provider not supported: FakeProvider");

        final var query2 = "CALL ai.text.embedBatch(['Hello world'], null, {})";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'provider' must not be null");
    }

    @Test
    void invalidArgumentsEmbed() {
        final var query1 = "RETURN ai.text.embed('Hello world', 'OpenAI', 1) AS result";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Type mismatch: expected Map");

        final var query2 = "RETURN ai.text.embed('Hello world', 'OpenAI', null) AS result";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'configuration' must not be null");
    }

    @Test
    void invalidArgumentsEmbedBatch() {
        final var query1 = "CALL ai.text.embedBatch(['Hello world'], 'OpenAI', 1)";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("Type mismatch: expected Map");

        final var query2 = "CALL ai.text.embedBatch(['Hello world'], 'OpenAI', null)";
        assertThatThrownBy(() -> db.executeTransactionally(
                        query2, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContainingAll("'configuration' must not be null");
    }

    @ParameterizedTest
    @ArgumentsSource(MissingSecretsArguments.class)
    void missingSecretsEmbed(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.embed('Hello world', '%s', conf) AS result
                """.formatted(args.conf(), args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(
                        ".*'(token|accessKeyId|secretAccessKey|token or apiKey)' is expected to have been set");
    }

    @Test
    void incorrectVertexAiAuth() {
        final var query = """
                WITH { token: 'token', apiKey: 'key', model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.text.embed('Hello world', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*Only one of either 'token' or 'apiKey' is expected to have been set");
    }

    @ParameterizedTest
    @ArgumentsSource(MissingSecretsArguments.class)
    void missingSecretsEmbedBatch(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                CALL ai.text.embedBatch(['Hello world'], '%s', conf)
                YIELD index, vector, resource
                RETURN index, vector, resource""".formatted(args.conf(), args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(
                        ".*'(token|accessKeyId|secretAccessKey|token or apiKey)' is expected to have been set");
    }

    @Test
    void vertexAiMultimodalTextEmbedRetry() {
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'multimodalembedding@001', region: 'tasman', project: 'gem', publisher: 'google' } AS conf
                RETURN ai.text.embed('Hello!', 'vertexai', conf) IS :: VECTOR<FLOAT32> AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", true);
    }

    @Test
    void vertexAiMultimodalTextEmbedBatchRetry() {
        final var query = """
                CALL ai.text.embedBatch(
                  ['Hello!'],
                  'vertexai',
                  { token: 'dummy-vertex-token', model: 'multimodalembedding@001', region: 'tasman', project: 'gem', publisher: 'google' }
                )
                YIELD index, vector, resource
                RETURN index, resource, vector IS :: VECTOR<FLOAT32> AS vector
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .containsExactly(Map.of("vector", true, "index", 0L, "resource", "Hello!"));
    }

    @Test
    void embeddingDocsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var expectedFunctionSignature =
                "ai.text.embed(resource :: STRING, provider :: STRING, configuration = {} :: MAP) :: VECTOR";
        final var showFuncQuery = """
                        SHOW FUNCTIONS YIELD name, argumentDescription, signature
                        WHERE name = 'ai.text.embed'
                        RETURN *
                        """;
        assertThat(db.executeTransactionally(showFuncQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .containsEntry("signature", expectedFunctionSignature)
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map map && "provider".equals(map.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);

        final var expectedProcedureSignature =
                "ai.text.embedBatch(resources :: LIST<STRING>, provider :: STRING, configuration = {} :: MAP) :: (index :: INTEGER, resource :: STRING, vector :: VECTOR)";
        final var showProcQuery = """
                        SHOW PROCEDURES YIELD name, argumentDescription, signature
                        WHERE name = 'ai.text.embedBatch'
                        RETURN *
                        """;
        assertThat(db.executeTransactionally(showProcQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .containsEntry("signature", expectedProcedureSignature)
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map map && "provider".equals(map.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);
    }
}

class RequiredConfArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "azure-openai",
                        "{ token: 'dummy-azure-token', resource: 'dummy', model: 'text-embedding-3-small' }"),
                new ProviderArgs("openai", "{ token: 'dummy-openai-token', model: 'text-embedding-3-small' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ token: 'dummy-vertex-token', model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ apiKey: 'dummy-api-key', model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google', vendorOptions: {} }"),
                new ProviderArgs(
                        "bedrock-titan:model by name",
                        "{ model: 'amazon.titan-embed-text-v1', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-titan:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1', region: 'us-east-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"));
    }
}

class AllOptionsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("azure-openai", """
                        {
                          token: 'dummy-azure-token',
                          resource: 'dummy',
                          model: 'text-embedding-3-small',
                          vendorOptions: {
                            dimensions: 1536,
                            user: 'gem'
                          }
                        }"""),
                new ProviderArgs("openai", """
                        {
                          token: 'dummy-openai-token',
                          model: 'text-embedding-3-small',
                          vendorOptions: { dimensions: 1536, user: 'gem' }
                        }"""),
                new ProviderArgs("vertexai", """
                        {
                          token: 'dummy-vertex-token',
                          model: 'gemini-embedding-001',
                          region: 'tasman',
                          project: 'gem',
                          publisher: 'google',
                          vendorOptions: { autoTruncate: true, task_type: 'QUESTION_ANSWERING' }
                        }"""),
                new ProviderArgs("vertexai", """
                        {
                          apiKey: 'dummy-api-key',
                          model: 'gemini-embedding-001',
                          region: 'tasman',
                          project: 'gem',
                          publisher: 'google',
                          vendorOptions: { autoTruncate: true, task_type: 'QUESTION_ANSWERING' }
                        }"""),
                new ProviderArgs("bedrock-titan:model by name", """
                {
                  model: 'amazon.titan-embed-text-v1',
                  region: 'eu-north-1',
                  accessKeyId: 'bedrock-key',
                  secretAccessKey: 'secret',
                  vendorOptions: {
                    dimensions: 1024,
                    normalize: true,
                    embeddingTypes: ['float']
                  }
                }
                """),
                new ProviderArgs("bedrock-titan:foundation model by arn", """
                        {
                          model: 'arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0',
                          region: 'us-east-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            dimensions: 1024,
                            normalize: true,
                            embeddingTypes: ['float']
                          }
                        }
                        """));
    }
}

class MissingSecretsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("azure-openai", "{ resource: 'dummy', model: 'text-embedding-3-small' }"),
                new ProviderArgs("openai", "{ model: 'text-embedding-3-small' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google' }"),
                new ProviderArgs(
                        "bedrock-titan:model by name", "{ model: 'amazon.titan-embed-text-v1', region: 'eu-north-1'}"),
                new ProviderArgs(
                        "bedrock-titan:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1', region: 'us-east-1' }"));
    }
}

class BatchWithMaxBatchSizeArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "azure-openai",
                        "{ token: 'dummy-azure-token', resource: 'dummy', model: 'text-embedding-3-small', maxBatchSize: 1 }"),
                new ProviderArgs(
                        "openai", "{ token: 'dummy-openai-token', model: 'text-embedding-3-small', maxBatchSize: 1 }"),
                new ProviderArgs(
                        "vertexai",
                        "{ token: 'dummy-vertex-token', model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google', maxBatchSize: 1 }"));
    }
}

class BatchWithMaxBatchSizeArguments2 implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "azure-openai",
                        "{ token: 'dummy-azure-token', resource: 'dummy', model: 'text-embedding-3-small', maxBatchSize: 10 }"),
                new ProviderArgs(
                        "openai", "{ token: 'dummy-openai-token', model: 'text-embedding-3-small', maxBatchSize: 10 }"),
                new ProviderArgs(
                        "vertexai",
                        "{ token: 'dummy-vertex-token', model: 'gemini-embedding-001', region: 'tasman', project: 'gem', publisher: 'google', maxBatchSize: 10 }"));
    }
}
