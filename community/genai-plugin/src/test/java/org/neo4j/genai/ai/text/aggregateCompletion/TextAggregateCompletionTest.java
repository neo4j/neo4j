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
package org.neo4j.genai.ai.text.aggregateCompletion;

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
import org.neo4j.genai.ai.text.completion.provider.azure.AzureOpenAi;
import org.neo4j.genai.ai.text.completion.provider.bedrock.BedrockConverse;
import org.neo4j.genai.ai.text.completion.provider.bedrock.BedrockNova;
import org.neo4j.genai.ai.text.completion.provider.bedrock.BedrockTitan;
import org.neo4j.genai.ai.text.completion.provider.openai.OpenAi;
import org.neo4j.genai.ai.text.completion.provider.vertex.VertexAi;
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
public class TextAggregateCompletionTest implements GenAITestExtension {
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
                .usingFilesUnderClasspath("wiremock/aggregateCompletion")
                // Uncomment for wiremock debug prints:
                // .notifier(new ConsoleNotifier(true))
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new OpenAi(baseUrl + "/v1"),
                new AzureOpenAi(),
                new VertexAi(p -> URI.create(baseUrl)),
                new BedrockConverse(p -> URI.create(baseUrl)),
                new BedrockNova(p -> URI.create(baseUrl)),
                new BedrockTitan(p -> URI.create(baseUrl))));
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

    @Test
    void listProviders() {
        assertThat(db.executeTransactionally("call ai.text.completion.providers()", Map.of(), consume()))
                .containsExactlyElementsOf(EXPECTED_PROVIDERS);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", "Bla bla bla... (%s)".formatted(args.provider()));
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionNothingToAggregate(ProviderArgs args) {
        final var query = """
                WITH %s as conf
                MATCH (u:Test)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void completionWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', '%s', conf) AS result
                """.formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", "Jag tog korven! (%s)".formatted(args.provider()));
    }

    @Test
    void docsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var showFuncQuery = """
                SHOW FUNCTIONS YIELD name, argumentDescription
                WHERE name = 'ai.text.aggregateCompletion'
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
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } AS conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Fail!', 'openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Request to OpenAI failed due to: content_filter");
    }

    @Test
    void vertexAiErrorResponseIsSurfaced() {
        final var query = """
                WITH { token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' } AS conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Fail Vertex!', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("INVALID_ARGUMENT");
    }

    @ParameterizedTest
    @ArgumentsSource(MissingSecretsArguments.class)
    void missingSecretsEmbed(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', '%s', conf) AS result
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
                WITH { token: 'token', apiKey: 'dummy-api-key', model: 'gemini-3', region: 'tasman', project: 'astrid', publisher: 'google', vendorOptions: {}} AS conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*Only one of either 'token' or 'apiKey' is expected to have been set");
    }

    @Test
    void openAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, "http://localhost/v1");
        final var query1 = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Fail OPENAI!', 'openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.aggregateCompletion`");

        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/v1");
        final var query2 = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', 'openai', conf) as result
                """;
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .containsEntry("result", "Bla bla bla... (openai)");
    }

    @Test
    void azureOpenAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, "http://localhost/%s");
        final var query1 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Fail AZURE!', 'azure-openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.aggregateCompletion`");

        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
        final var query2 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf
                MATCH (u:UserReview)
                RETURN ai.text.aggregateCompletion(u.review, 'Hello, can you tell me the issues with my restaurant based on these reviews?', 'azure-openai', conf) as result
                """;
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .containsEntry("result", "Bla bla bla... (azure-openai)");
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
                        "bedrock-nova:model by name",
                        "{ model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-nova:custom nova type model",
                        "{ model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova', modelType: 'amazon.nova', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-nova:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:eu-north-1::foundation-model/amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock:model by name",
                        "{ model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock:custom nova type model",
                        "{ model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova', modelType: 'amazon.nova', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:eu-north-1::foundation-model/amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-titan:model by name",
                        "{ model: 'amazon.titan-text-express-v1', region: 'eu-west-2', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-titan:custom titan type model",
                        "{ model: 'arn:aws:bedrock:xxx:001:custom-model/custom.titan', region: 'eu-west-2', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock-titan:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:eu-west-2::foundation-model/amazon.titan-text-lite-v1', region: 'eu-west-2', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"));
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
                new ProviderArgs("bedrock-nova:model by name", """
                        {
                          model: 'eu.amazon.nova-micro-v1:0',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock-nova:custom nova type model", """
                        {
                          model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock-nova:foundation model by arn", """
                        {
                          model: 'arn:aws:bedrock:eu-north-1::foundation-model/amazon.nova-micro-v1:0',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock:model by name", """
                        {
                          model: 'eu.amazon.nova-micro-v1:0',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock:custom nova type model", """
                        {
                          model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock:foundation model by arn", """
                        {
                          model: 'arn:aws:bedrock:eu-north-1::foundation-model/amazon.nova-micro-v1:0',
                          region: 'eu-north-1',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            system: [{ text: 'You are Kommendoran' }],
                            inferenceConfig: { maxTokens: 1024 }
                          }
                        }
                        """),
                new ProviderArgs("bedrock-titan:model by name", """
                        {
                          model: 'amazon.titan-text-lite-v2',
                          region: 'eu-west-2',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            textGenerationConfig: {
                              maxTokenCount: 1024
                            }
                          }
                        }
                        """),
                new ProviderArgs("bedrock-titan:custom titan type model", """
                        {
                          model: 'arn:aws:bedrock:xxx:002:custom-model/custom.titan',
                          region: 'eu-west-2',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            textGenerationConfig: {
                              maxTokenCount: 1024
                            }
                          }
                        }
                        """),
                new ProviderArgs("bedrock-titan:foundation model by arn", """
                        {
                          model: 'arn:aws:bedrock:eu-west-2::foundation-model/amazon.titan-text-lite-v2',
                          region: 'eu-west-2',
                          accessKeyId: 'bedrock-key',
                          secretAccessKey: 'secret',
                          vendorOptions: {
                            textGenerationConfig: {
                              maxTokenCount: 1024
                            }
                          }
                        }
                        """));
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
                new ProviderArgs(
                        "bedrock-nova:model by name",
                        "{ model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key' }"),
                new ProviderArgs(
                        "bedrock-nova:custom nova type model",
                        "{ model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova', modelType: 'amazon.nova', region: 'eu-north-1', secretAccessKey: 'secret' }"));
    }
}
