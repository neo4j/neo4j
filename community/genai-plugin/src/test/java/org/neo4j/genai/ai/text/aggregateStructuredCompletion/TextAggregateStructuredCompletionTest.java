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
package org.neo4j.genai.ai.text.aggregateStructuredCompletion;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import org.neo4j.genai.ai.text.structuredCompletion.provider.azure.AzureOpenAi;
import org.neo4j.genai.ai.text.structuredCompletion.provider.bedrock.BedrockConverse;
import org.neo4j.genai.ai.text.structuredCompletion.provider.openai.OpenAi;
import org.neo4j.genai.ai.text.structuredCompletion.provider.vertex.VertexAi;
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
public class TextAggregateStructuredCompletionTest implements GenAITestExtension {
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
                .usingFilesUnderClasspath("wiremock/aggregateStructuredCompletion")
                // Uncomment for wiremock debug prints:
                // .notifier(new ConsoleNotifier(true))
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new OpenAi(baseUrl + "/v1"),
                new AzureOpenAi(),
                new VertexAi(p -> URI.create(baseUrl)),
                new BedrockConverse(p -> URI.create(baseUrl))));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25);
    }

    @BeforeEach
    void setup() {
        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
    }

    @BeforeAll
    void setUp() {
        db.executeTransactionally("""
                CREATE (:UserReview {review: "The food was okay, but the service was terrible!"})
                CREATE (:UserReview {review: "Love this place!!!"})
                CREATE (:UserReview {review: "Rude staff and meh food"})
                CREATE (:UserReview {review: "Best pancakes, a little slow, but I had time"})
                CREATE (:UserReview {review: "Alright"})
                CREATE (:UserReview {review: "Lovely wee place, recommend for a calm breakfast"})
                CREATE (:UserReview {review: "Had to wait ages for the food to come! Almost left"})
                CREATE (:UserReview {review: "mmmmm pancakes with syrup"})
                """);
    }

    @AfterAll
    void stopWireMock() {
        if (this.wireMock != null) this.wireMock.stop();
    }

    private final List<Map<String, Object>> EXPECTED_PROVIDERS = List.of(
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
                      vendorOptions :: MAP NOT NULL,
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of(), "chatHistory", List.of())),
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
                      vendorOptions :: MAP NOT NULL,
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of(), "chatHistory", List.of())),
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
                      vendorOptions :: MAP NOT NULL,
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("publisher", "google", "vendorOptions", Map.of(), "chatHistory", List.of())),
            Map.of(
                    "name",
                    "Bedrock",
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
                      vendorOptions :: MAP NOT NULL,
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of(), "chatHistory", List.of())),
            Map.of(
                    "name",
                    "Bedrock-Nova",
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
                    Map.of("vendorOptions", Map.of())));

    private String schemaLiteral() {
        return """
                {
                   type: 'object',
                   properties:
                       {
                            issues:
                                {
                                    type: 'array',
                                    items :
                                        {
                                            type: 'object',
                                            properties:
                                                {
                                                    priority: { type: "string" },
                                                    description: { type: "string" }
                                                },
                                            required: ["priority", "description"],
                                            additionalProperties: false
                                        }
                                }
                       },
                   required: ['issues'],
                   additionalProperties: false
                }
                """;
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, '%s', conf) AS result""".formatted(args.conf(), schemaLiteral(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsExactlyEntriesOf(Map.of(
                                    "issues",
                                    List.of(
                                            Map.of("priority", "High", "description", "Issue 1."),
                                            Map.of("priority", "Medium", "description", "Issue 2."))));
                });
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionNothingToAggregate(ProviderArgs args) {
        final var query = """
                WITH %s as conf, %s AS schema
                MATCH (u:Test)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, '%s', conf) AS result""".formatted(args.conf(), schemaLiteral(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void completionWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?',  schema, '%s', conf) AS result
                """.formatted(args.conf(), schemaLiteral(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsExactlyEntriesOf(Map.of(
                                    "issues",
                                    List.of(
                                            Map.of("priority", "High", "description", "Issue 1."),
                                            Map.of("priority", "Medium", "description", "Issue 2."))));
                });
    }

    @Test
    void docsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var showFuncQuery = """
                SHOW FUNCTIONS YIELD name, argumentDescription
                WHERE name = 'ai.text.aggregateStructuredCompletion'
                RETURN *
                """;
        assertThat(db.executeTransactionally(showFuncQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map map && "provider".equals(map.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);
    }

    @Test
    void openAiIncompleteResponseYieldsReason() {
        final var query = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } AS conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Fail!', schema, 'openai', conf) as result
                """.formatted(schemaLiteral());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Request failed due to: content_filter");
    }

    @Test
    void vertexAiErrorResponseIsSurfaced() {
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' } AS conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Fail Vertex!', schema, 'vertexai', conf) AS result
                """.formatted(schemaLiteral());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @ParameterizedTest
    @ArgumentsSource(MissingSecretsArguments.class)
    void missingSecretsEmbed(ProviderArgs args) {
        final var query = """
                WITH %s AS conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, '%s', conf) AS result
                """.formatted(args.conf(), schemaLiteral(), args.provider());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(
                        ".*'(token|accessKeyId|secretAccessKey|token or apiKey)' is expected to have been set");
    }

    @Test
    void incorrectVertexAiAuth() {
        final var query = """
                WITH { token: 'token', apiKey: 'dummy-api-key', model: 'gemini-3', region: 'tasman', project: 'astrid', publisher: 'google', vendorOptions: {}} AS conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, 'vertexai', conf) AS result
                """.formatted(schemaLiteral());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*Only one of either 'token' or 'apiKey' is expected to have been set");
    }

    @Test
    void openAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, "http://localhost/v1");
        final var query1 = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Fail OPENAI!', schema, 'openai', conf) as result
                """.formatted(schemaLiteral());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.aggregateStructuredCompletion`");

        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/v1");
        final var query2 = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, 'openai', conf) as result
                """.formatted(schemaLiteral());
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsExactlyEntriesOf(Map.of(
                                    "issues",
                                    List.of(
                                            Map.of("priority", "High", "description", "Issue 1."),
                                            Map.of("priority", "Medium", "description", "Issue 2."))));
                });
    }

    @Test
    void azureOpenAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, "http://localhost/%s");
        final var query1 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Fail AZURE!', schema, 'azure-openai', conf) as result
                """.formatted(schemaLiteral());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.aggregateStructuredCompletion`");

        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
        final var query2 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf, %s AS schema
                MATCH (u:UserReview)
                RETURN ai.text.aggregateStructuredCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', schema, 'azure-openai', conf) as result
                """.formatted(schemaLiteral());
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsExactlyEntriesOf(Map.of(
                                    "issues",
                                    List.of(
                                            Map.of("priority", "High", "description", "Issue 1."),
                                            Map.of("priority", "Medium", "description", "Issue 2."))));
                });
    }
}

class RequiredConfArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", "{ token: 'dummy-openai-token', model: 'gpt-5' }"),
                new ProviderArgs("azure-openai", "{ token: 'dummy-azure-token', resource: 'dummy', model: 'gpt-5' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ apiKey: 'dummy-api-key', model: 'gemini-3', region: 'tasman', project: 'astrid', publisher: 'google', vendorOptions: {}}"),
                new ProviderArgs(
                        "bedrock",
                        "{ accessKeyId: 'AKIA...', secretAccessKey: 'SECRET', region: 'us-east-1', model: 'some-model' }"));
    }
}

class AllOptionsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", """
                        {
                          token: 'dummy-openai-token',
                          model: 'gpt-5',
                          vendorOptions: {
                            max_output_tokens: 1024,
                            store: false
                          }
                        }"""),
                new ProviderArgs("azure-openai", """
                        {
                          token: 'dummy-azure-token',
                          resource: 'dummy',
                          model: 'gpt-5',
                          vendorOptions: {
                            max_output_tokens: 1024,
                            store: false
                          }
                        }"""),
                new ProviderArgs("vertexai", """
                        {
                          token: 'dummy-vertex-token',
                          model: 'gemini-3',
                          region: 'smaland',
                          project: 'astrid',
                          publisher: 'google',
                          vendorOptions: {
                            system_instruction: { parts: [ { text: 'You are Kommendoran'}]}
                          }
                        }"""),
                new ProviderArgs("vertexai", """
                        {
                          apiKey: 'dummy-api-key',
                          model: 'gemini-3',
                          region: 'tasman',
                          project: 'astrid',
                          publisher: 'google',
                          vendorOptions: {
                            system_instruction: { parts: [ { text: 'You are Kommendoran'}]}
                          }
                        }"""),
                new ProviderArgs("bedrock", ("""
                            {
                              accessKeyId: 'AKIA...',
                              secretAccessKey: 'SECRET',
                              region: 'us-east-1',
                              model: 'some-model',
                              vendorOptions: { temperature: 0.3 }
                            }
                            """)));
    }
}

class MissingSecretsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", "{ model: 'gpt-5' }"),
                new ProviderArgs("azure-openai", "{ resource: 'dummy', model: 'gpt-5' }"),
                new ProviderArgs(
                        "vertexai", "{ model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' }"),
                new ProviderArgs("bedrock", ("""
                            {
                              accessKeyId: 'AKIA...',
                              region: 'us-east-1',
                              model: 'some-model',
                              vendorOptions: { temperature: 0.3 },
                              chatHistory: [
                                { role: 'user', content: [ { text: 'What is the capital of France?' } ] },
                                { role: 'assistant', content: [ { text: 'The capital of France is Paris.' } ] }
                              ]
                            }
                            """)));
    }
}
