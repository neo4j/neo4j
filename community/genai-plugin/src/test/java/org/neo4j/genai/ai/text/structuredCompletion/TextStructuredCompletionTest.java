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
package org.neo4j.genai.ai.text.structuredCompletion;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
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
public class TextStructuredCompletionTest implements GenAITestExtension {

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
                .usingFilesUnderClasspath("wiremock/structuredCompletion")
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(
                new OpenAi(baseUrl + "/v1"),
                new AzureOpenAi(),
                new VertexAi(p -> java.net.URI.create(baseUrl)),
                new BedrockConverse(p -> java.net.URI.create(baseUrl))));
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
                    Map.of("vendorOptions", Map.of(), "chatHistory", List.of())));

    private String schemaLiteralSimple() {
        return """
                {
                  type: 'object',
                  properties: { answer: { type: 'string' } },
                  required: ['answer'],
                  additionalProperties: false
                }
                """;
    }

    private String schemaLiteral() {
        return """
                {
                   type: 'object',
                   properties:
                       {
                            recipes:
                                {
                                    type: 'array',
                                    items :
                                        {
                                            type: 'object',
                                            properties:
                                                {
                                                    name: { type: "string" },
                                                    description: { type: "string" }
                                                },
                                            required: ["name", "description"],
                                            additionalProperties: false
                                        }
                                }
                       },
                   required: ['recipes'],
                   additionalProperties: false
                }
                """;
    }

    @Test
    void providersProcedure() {
        final var query = "CALL ai.text.structuredCompletion.providers()";
        final var rows =
                db.executeTransactionally(query, Map.of(), r -> r.stream().toList());
        assertThat(rows).containsExactlyInAnyOrderElementsOf(EXPECTED_PROVIDERS);
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(RequiredArgsProviders.class)
    void completionWithRequiredArgsSimple(ProviderArgs args) {
        final var query = """
                WITH %s AS conf, %s AS schema
                RETURN ai.text.structuredCompletion('Hello!', schema, '%s', conf) AS result
                """.formatted(args.conf(), schemaLiteralSimple(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsEntry(
                                    "answer",
                                    "Hi there! How can I help today? I can explain concepts, answer questions, draft or edit emails, brainstorm ideas, translate text, summarize articles, help with coding, plan trips, make checklists, and more.");
                });
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(RequiredArgsProviders.class)
    void completionWithRequiredArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf, %s AS schema
                RETURN ai.text.structuredCompletion('Hello! Can you give me a list of 10 cookie recipes, just their name and description', schema, '%s', conf) AS result
                """.formatted(args.conf(), schemaLiteral(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsExactlyEntriesOf(
                                    Map.of(
                                            "recipes",
                                            List.of(
                                                    Map.of(
                                                            "name",
                                                            "Classic Chocolate Chip Cookies",
                                                            "description",
                                                            "Soft-centered, chewy cookies loaded with melted semisweet chocolate chips and vanilla."),
                                                    Map.of(
                                                            "name",
                                                            "Snickerdoodles",
                                                            "description",
                                                            "Chewy cookies rolled in cinnamon sugar, with a slight tang from cream of tartar."),
                                                    Map.of(
                                                            "name",
                                                            "Peanut Butter Cookies",
                                                            "description",
                                                            "Rich, fudgy peanut butter cookies with crisp fork-pressed ridges."),
                                                    Map.of(
                                                            "name",
                                                            "Oatmeal Raisin Cookies",
                                                            "description",
                                                            "Hearty, chewy cookies with oats, cinnamon, and sweet raisins."),
                                                    Map.of(
                                                            "name",
                                                            "White Chocolate Macadamia Nut Cookies",
                                                            "description",
                                                            "Crunchy edges and chewy centers studded with white chocolate chunks and macadamias."),
                                                    Map.of(
                                                            "name",
                                                            "Chocolate Thumbprint Cookies",
                                                            "description",
                                                            "Buttery cookies with a well of chocolate ganache or jam in the center."),
                                                    Map.of(
                                                            "name",
                                                            "Lemon Sugar Cookies",
                                                            "description",
                                                            "Soft, lemony cookies with bright zest and a delicate sugary crust."),
                                                    Map.of(
                                                            "name",
                                                            "Ginger Molasses Cookies",
                                                            "description",
                                                            "Thick, chewy cookies loaded with ginger, cinnamon, and rich molasses."),
                                                    Map.of(
                                                            "name",
                                                            "Coconut Macaroons",
                                                            "description",
                                                            "Sweet, chewy coconut cookies with toasty edges, often dipped in chocolate."),
                                                    Map.of(
                                                            "name",
                                                            "Almond Biscotti",
                                                            "description",
                                                            "Twice-baked, crisp almond cookies perfect for dipping into coffee or tea."))));
                });
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(RequiredArgsProviders.class)
    void completionWithRequiredArgsEmptySchema(ProviderArgs args) {
        final var query = """
                WITH %s AS conf, {} AS schema
                RETURN ai.text.structuredCompletion('Hello! Can you give me a list of 10 cookie recipes, just their name and description', schema, '%s', conf) AS result
                """.formatted(args.conf(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result).containsExactlyEntriesOf(Map.of());
                });
    }

    @ParameterizedTest(name = "{0}")
    @ArgumentsSource(AllArgsProviders.class)
    void completionWithAllArgs(ProviderArgs args) {
        final var query = """
                WITH %s AS conf, %s AS schema
                RETURN ai.text.structuredCompletion('What country is Paris in?', schema, '%s', conf) AS result
                """.formatted(args.conf(), schemaLiteralSimple(), args.provider());
        assertThat(db.executeTransactionally(query, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result).containsEntry("answer", "Paris is in France.");
                });
    }

    @Test
    void azureOpenAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, "http://localhost/%s");
        final var query1 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf, %s AS schema
                RETURN ai.text.structuredCompletion('Fail AZURE!', schema, 'azure-openai', conf) as result
                """.formatted(schemaLiteralSimple());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.structuredCompletion`");

        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
        final var query2 = """
                WITH { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf, %s AS schema
                RETURN ai.text.structuredCompletion('Hello!', schema, 'azure-openai', conf) as result
                """.formatted(schemaLiteralSimple());
        assertThat(db.executeTransactionally(query2, Map.of(), consume()))
                .as("Query:%n```%n%s%n```%n", query2)
                .singleElement(resultMap())
                .satisfies(row -> {
                    @SuppressWarnings("unchecked")
                    var result = (Map<String, Object>) row.get("result");
                    assertThat(result)
                            .containsEntry(
                                    "answer",
                                    "Hi there! How can I help today? I can explain concepts, answer questions, draft or edit emails, brainstorm ideas, translate text, summarize articles, help with coding, plan trips, make checklists, and more.");
                });
    }

    @Test
    void openAiIncompleteResponse() {
        final var query = """
                WITH { token: 'dummy-openai-token', model: 'gpt-5' } AS conf, %s AS schema
                RETURN ai.text.structuredCompletion('INCOMPLETE', schema, 'openai', conf) AS result
                """.formatted(schemaLiteralSimple());
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .isExactlyInstanceOf(QueryExecutionException.class)
                .hasMessageContaining("Failed to invoke function `ai.text.structuredCompletion`");
    }

    static class RequiredArgsProviders implements ProviderArguments {
        @Override
        public Stream<ProviderArgs> providers() {
            return Stream.of(
                    new ProviderArgs("openai", "{ token: 'dummy-openai-token', model: 'gpt-5' }"),
                    new ProviderArgs(
                            "azure-openai", "{ token: 'dummy-azure-token', resource: 'dummy', model: 'gpt-5' }"),
                    new ProviderArgs(
                            "vertexai",
                            "{ token: 'dummy-vertex-token', model: 'gemini-3', region: 'smaland', project: 'astrid', publisher: 'google' }"),
                    new ProviderArgs(
                            "vertexai",
                            "{ apiKey: 'dummy-api-key', model: 'gemini-3', region: 'tasman', project: 'astrid', publisher: 'google' }"),
                    new ProviderArgs(
                            "bedrock",
                            "{ accessKeyId: 'AKIA...', secretAccessKey: 'SECRET', region: 'us-east-1', model: 'some-model' }"));
        }
    }

    static class AllArgsProviders implements ProviderArguments {
        @Override
        public Stream<ProviderArgs> providers() {
            final var chatHistoryOpenAI = """
                    [
                      { role: 'user', content: 'What is the capital of France?' },
                      { role: 'assistant', content: 'The capital of France is Paris.' }
                    ]
                    """;
            return Stream.of(
                    new ProviderArgs("openai", ("""
                            {
                              token: 'dummy-openai-token',
                              model: 'gpt-5',
                              vendorOptions: { store: false },
                              chatHistory: %s
                            }
                            """.formatted(chatHistoryOpenAI)).trim()),
                    new ProviderArgs("azure-openai", ("""
                            {
                              token: 'dummy-azure-token',
                              resource: 'dummy',
                              model: 'gpt-5',
                              vendorOptions: { store: false },
                              chatHistory: %s
                            }
                            """.formatted(chatHistoryOpenAI)).trim()),
                    new ProviderArgs("vertexai", ("""
                            {
                              token: 'dummy-vertex-token',
                              model: 'gemini-3',
                              region: 'smaland',
                              project: 'astrid',
                              publisher: 'google',
                              vendorOptions: { maxOutputTokens: 2048 },
                              chatHistory: [
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
                              ]
                            }
                            """)),
                    new ProviderArgs("bedrock", ("""
                            {
                              accessKeyId: 'AKIA...',
                              secretAccessKey: 'SECRET',
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
}
