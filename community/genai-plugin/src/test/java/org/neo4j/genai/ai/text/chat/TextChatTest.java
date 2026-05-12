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
package org.neo4j.genai.ai.text.chat;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.neo4j.genai.ai.text.chat.provider.azure.AzureOpenAi;
import org.neo4j.genai.ai.text.chat.provider.openai.OpenAi;
import org.neo4j.genai.util.GenAITestExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImpermanentDbmsExtension(configurationCallback = "configure")
public class TextChatTest implements GenAITestExtension {
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
                .usingFilesUnderClasspath("wiremock/chat")
                // .notifier(new ConsoleNotifier(true))
                .http2PlainDisabled(true));
        this.wireMock.start();
        final var baseUrl = this.wireMock.baseUrl();
        builder.addExtension(new GenAiPluginExtension(new OpenAi(baseUrl + "/v1"), new AzureOpenAi()));
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
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of())),
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
                      vendorOptions :: MAP NOT NULL
                    }""",
                    "defaultConfig",
                    Map.of("vendorOptions", Map.of())));

    @Test
    void providers() {
        assertThat(db.executeTransactionally("CALL ai.text.chat.providers()", Map.of(), consume()))
                .containsExactlyInAnyOrderElementsOf(EXPECTED_PROVIDERS);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void completionWithRequiredArgs(ProviderArgs args) {
        final var query = """
                with %s as conf
                return ai.text.chat('Hello Chat!', null, '%s', conf) as result""".formatted(args.conf(), args.provider());
        final var res = db.executeTransactionally(
                query, Map.of("conf", args.conf()), l -> l.next().get("result"));
        assertThat(res)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.map(String.class, Object.class))
                .containsEntry("message", "Bla bla bla... (%s)".formatted(args.provider()))
                .containsEntry("chatId", "resp_xxx");
    }

    @ParameterizedTest
    @ArgumentsSource(AllOptionsArguments.class)
    void completionWithAllArgs(ProviderArgs args) {
        final var query = """
                with %s as conf
                return ai.text.chat('Hello Chat!', null, '%s', conf) as result
                """.formatted(args.conf(), args.provider());
        final var res = db.executeTransactionally(
                query, Map.of("conf", args.conf()), l -> l.next().get("result"));
        assertThat(res)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.map(String.class, Object.class))
                .containsEntry("message", "Bla bla bla... (%s)".formatted(args.provider()))
                .containsEntry("chatId", "resp_xxx");
    }

    @Test
    void docsAreUpToDate() {
        final var expectedProviders = EXPECTED_PROVIDERS.stream()
                .map(m -> m.get("name").toString())
                .sorted()
                .collect(Collectors.joining("', '", "'", "'."));
        final var showFuncQuery = """
                show functions yield name, argumentDescription
                where name = 'ai.text.chat'
                return *
                """;
        assertThat(db.executeTransactionally(showFuncQuery, Map.of(), consume()))
                .singleElement(map(String.class, Object.class))
                .extracting("argumentDescription", InstanceOfAssertFactories.LIST)
                .filteredOn(d -> d instanceof Map map && "provider".equals(map.get("name")))
                .singleElement(map(String.class, Object.class))
                .containsEntry("description", "The identifier of the provider: " + expectedProviders);
    }

    @ParameterizedTest
    @ArgumentsSource(RequiredConfArguments.class)
    void chatWithPreviousId() {
        final var conf = Map.of("token", "dummy-openai-token", "model", "gpt-5");
        final var query = """
                RETURN ai.text.chat('Hello again!', 'prev_123', 'OpenAI', $conf) AS result
                """;
        final var res = db.executeTransactionally(
                query, Map.of("conf", conf), l -> l.next().get("result"));
        assertThat(res)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.map(String.class, Object.class))
                .containsEntry("message", "Bla bla bla... (openai)")
                .containsEntry("chatId", "resp_xxx");
    }

    @Test
    void openAiIncompleteResponseYieldsReason() {
        final var query = """
                WITH { token: 'dummy-openai-token-fail', model: 'gpt-5' } AS conf
                RETURN ai.text.chat('Fail!',  'prev_1234', 'openai', conf) AS result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Request to OpenAI failed due to: content_filter");
    }

    @Test
    void openAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, "http://localhost/v1");
        final var query1 = """
                with { token: 'dummy-openai-token', model: 'gpt-5' } as conf
                return ai.text.chat('Fail OPENAI!', null, 'openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.chat`");

        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/v1");
        final var query2 = """
                with { token: 'dummy-openai-token', model: 'gpt-5' } as conf
                return ai.text.chat('Hello Chat!', null, 'openai', conf) as result
                """;
        final var res =
                db.executeTransactionally(query2, Map.of(), l -> l.next().get("result"));
        assertThat(res)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.map(String.class, Object.class))
                .containsEntry("message", "Bla bla bla... (openai)")
                .containsEntry("chatId", "resp_xxx");
    }

    @Test
    void azureOpenAIWithConfigSetBaseURL() {
        GenAIConfig.instance().setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, "http://localhost/%s");
        final var query1 = """
                with { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf
                return ai.text.chat('Fail AZURE!', null, 'azure-openai', conf) as result
                """;
        assertThatThrownBy(() -> db.executeTransactionally(
                        query1, Map.of(), r -> r.stream().toList()))
                .hasMessageContaining("Failed to invoke function `ai.text.chat`");

        GenAIConfig.instance()
                .setProperty(GenAIConfig.GENAI_AZURE_OPENAI_BASE_URL, this.wireMock.baseUrl() + "/openai/v1");
        final var query2 = """
                with { token: 'dummy-azure-token', resource: 'dummy-resource', model: 'gpt-5' } as conf
                return ai.text.chat('Hello Chat!', null, 'azure-openai', conf) as result
                """;
        final var res =
                db.executeTransactionally(query2, Map.of(), l -> l.next().get("result"));
        assertThat(res)
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.map(String.class, Object.class))
                .containsEntry("message", "Bla bla bla... (azure-openai)")
                .containsEntry("chatId", "resp_xxx");
    }
}

class RequiredConfArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", "{ token: 'dummy-openai-token', model: 'gpt-5' }"),
                new ProviderArgs("azure-openai", "{ token: 'dummy-azure-token', resource: 'dummy', model: 'gpt-5' }"));
    }
}

class AllOptionsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(new ProviderArgs("openai", """
                        {
                          token: 'dummy-openai-token',
                          model: 'gpt-5',
                          vendorOptions: {
                            max_output_tokens: 1024,
                            store: false
                          }
                        }"""), new ProviderArgs("azure-openai", """
                        {
                          token: 'dummy-azure-token',
                          resource: 'dummy',
                          model: 'gpt-5',
                          vendorOptions: {
                            max_output_tokens: 1024,
                            store: false
                          }
                        }"""));
    }
}

class MissingSecretsArguments implements ProviderArguments {
    @Override
    public Stream<ProviderArgs> providers() {
        return Stream.of(
                new ProviderArgs("openai", "{ model: 'gpt-5' }"),
                new ProviderArgs("azure-openai", "{ resource: 'dummy', model: 'gpt-5' }"));
    }
}
