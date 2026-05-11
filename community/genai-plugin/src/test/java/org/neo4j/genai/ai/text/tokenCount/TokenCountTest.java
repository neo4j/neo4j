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
package org.neo4j.genai.ai.text.tokenCount;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.genai.GenAiPluginExtension;
import org.neo4j.genai.ai.ProviderArgs;
import org.neo4j.genai.ai.ProviderArguments;
import org.neo4j.genai.ai.text.tokenCount.provider.bedrock.Bedrock;
import org.neo4j.genai.ai.text.tokenCount.provider.openai.OpenAi;
import org.neo4j.genai.ai.text.tokenCount.provider.vertex.VertexAi;
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
public class TokenCountTest implements GenAITestExtension {
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
                .usingFilesUnderClasspath("wiremock/tokenCount")
                // Uncomment for wiremock debug prints:
                // .notifier(new ConsoleNotifier(true))
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new OpenAi(), new VertexAi(p -> URI.create(baseUrl)), new Bedrock(p -> URI.create(baseUrl))));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25);
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
                    "{\n  \n}",
                    "optionalConfigType",
                    """
                      {
                        model :: STRING NOT NULL
                      }""",
                    "defaultConfig",
                    Map.of("model", "")),
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
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("publisher", "google", "chatHistory", List.of())),
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
                      chatHistory :: LIST<ANY> NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("chatHistory", List.of())));

    @Test
    void providersProcedure() {
        final var query = "CALL ai.text.tokenCount.providers()";
        final var rows =
                db.executeTransactionally(query, Map.of(), r -> r.stream().toList());
        assertThat(rows).containsExactlyInAnyOrderElementsOf(EXPECTED_PROVIDERS);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.tokenCount('Hello!', '%s', conf) AS result""".formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", (long) 2);
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void completionWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.tokenCount('Hello!', '%s', conf) AS result
                """.formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", (long) 2);
    }

    @Test
    void docsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var showFuncQuery = """
                SHOW FUNCTIONS YIELD name, argumentDescription
                WHERE name = 'ai.text.tokenCount'
                RETURN *
                """;
        assertThat(db.executeTransactionally(showFuncQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map map && "provider".equals(map.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry(
                        "description",
                        "The identifier of the provider: " + expectedProviders + " The default is 'OpenAI'.");
    }

    @ParameterizedTest
    @ArgumentsSource(MissingSecretsArguments.class)
    void missingSecretsEmbed(ProviderArgs args) {
        final var query = """
                WITH %s AS conf
                RETURN ai.text.tokenCount('Hello world', '%s', conf) AS result
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
                RETURN ai.text.tokenCount('Hello world', 'vertexai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageMatching(".*Only one of either 'token' or 'apiKey' is expected to have been set");
    }

    @Test
    void openAiModelNotFoundFallback() {
        final var query = """
                RETURN ai.text.tokenCount('Hello!', 'openai', {model: 'non-existent-model'}) AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .singleElement(resultMap())
                .containsEntry("result", (long) 2);
    }

    @Test
    void nullInputReturnsNull() {
        final var query = """
                RETURN ai.text.tokenCount(null, 'openai', {model: 'gpt-4o', token: 'dummy'}) AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .singleElement(resultMap())
                .containsEntry("result", null);
    }

    @Test
    void defaultProviderIsOpenAi() {
        final var query = """
                RETURN ai.text.tokenCount('Hello! There is no wiremock mapping for this, so must be openAI') AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .singleElement(resultMap())
                .containsEntry("result", (long) 17);
    }

    @Test
    void openAiWithDefaultConfig() {
        final var query = """
                RETURN ai.text.tokenCount('Hello!', 'openai') AS result
                """;
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .singleElement(resultMap())
                .containsEntry("result", (long) 2);
    }

    @Test
    void bedrockMissingRegionThrows() {
        final var query = """
                RETURN ai.text.tokenCount('Hello!', 'bedrock', {accessKeyId: 'key', secretAccessKey: 'secret', model: 'model'}) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(query, Map.of(), consume()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContaining("'region' is expected to have been set");
    }

    @Test
    void nonOpenAiNullConfigThrows() {
        final var query = """
                RETURN ai.text.tokenCount('Hello!', 'vertexai', null) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(query, Map.of(), consume()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContaining("'configuration' must not be null");
    }

    @ParameterizedTest
    @ArgumentsSource(ChatHistoryArguments.class)
    void providerWithChatHistory(ProviderArgs args) {
        final var query = """
                %s
                RETURN ai.text.tokenCount('What country is Paris in?', '%s', conf) as result
                """.formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .containsEntry("result", (long) 42);
    }
}

class RequiredConfArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", "{ token: 'dummy-openai-token', model: 'gpt-5' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' }"),
                new ProviderArgs(
                        "vertexai",
                        "{ apiKey: 'dummy-api-key', model: 'gemini-3', region: 'tasman', project: 'astrid', publisher: 'google', vendorOptions: {}}"),
                new ProviderArgs(
                        "bedrock:model by name",
                        "{ model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock:custom nova type model",
                        "{ model: 'arn:aws:bedrock:xxx:001:custom-model/custom.nova', modelType: 'amazon.nova', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"),
                new ProviderArgs(
                        "bedrock:foundation model by arn",
                        "{ model: 'arn:aws:bedrock:eu-north-1::foundation-model/amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret' }"));
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
                        """));
    }
}

class MissingSecretsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs(
                        "vertexai", "{ model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' }"),
                new ProviderArgs(
                        "bedrock:model by name",
                        "{ model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key' }"));
    }
}

class ChatHistoryArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(new ProviderArgs("VertexAI", """
                        WITH [
                           {
                             role: "user",
                             parts: [
                               { text: "What is the capital of France?" }
                             ]
                           },
                           {
                             role: "model",
                             parts: [
                               { text: "The capital of France is Paris." }
                             ]
                           }
                         ] AS messageHistory
                        WITH { token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google', chatHistory: messageHistory } AS conf
                        """), new ProviderArgs("Bedrock", """
                        WITH [
                           {
                             role: "user",
                             content: [
                               { text: "What is the capital of France?" }
                             ]
                           },
                           {
                             role: "assistant",
                             content: [
                               { text: "The capital of France is Paris." }
                             ]
                           }
                         ] AS messageHistory
                        WITH { model: 'eu.amazon.nova-micro-v1:0', region: 'eu-north-1', accessKeyId: 'bedrock-key', secretAccessKey: 'secret', chatHistory: messageHistory } AS conf
                        """));
    }
}
