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

import static java.util.Objects.requireNonNull;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.provider.NamedProvider;
import org.neo4j.genai.util.provider.ProviderRow;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Sensitive;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.virtual.MapValue;

public class TextChat {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.text.chat.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC = "The identifier of the provider: 'Azure-OpenAI', 'OpenAI'.";

    @Context
    public Providers providers;

    @Context
    public GenAIConfig genAIConfig;

    @UserFunction(name = "ai.text.chat")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Chat based on the specified prompt, optionally continuing a previous interaction.")
    public Map<String, String> chat(
            @Name(value = "prompt", description = "The user message to send.") String prompt,
            @Name(
                            value = "chatId",
                            description =
                                    "Previous chat ID to continue the conversation. If this is the first message in the conversation, set it to null.")
                    String previousResponseId,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration) {
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");
        final var provider = providers.configure(providerName, configuration, genAIConfig);
        if (prompt == null) return null;
        final var result = provider.chat(prompt, Optional.ofNullable(previousResponseId));
        return Map.of(
                "message", result.message,
                "chatId", result.chatId);
    }

    @Procedure(name = "ai.text.chat.providers")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Lists the available text chat providers.")
    public Stream<ProviderRow> listProviders() {
        return providers.providers().stream().map(ProviderRow::from).sorted(Comparator.comparing(ProviderRow::name));
    }

    public interface Provider extends NamedProvider {
        Implementation configure(HttpService httpService, MapValue configuration, GenAIConfig genAIConfig);

        interface Implementation extends NamedProvider.Implementation {
            ChatResult chat(String prompt, Optional<String> previousResponseId);
        }
    }

    public record ChatResult(
            @Description("The returned response from the given provider.")
            String message,

            @Description("The chat ID that can be used to continue this conversation.")
            String chatId) {}

    public interface Providers extends NamedProvider.Lookup<Provider> {
        Provider.Implementation configure(String name, MapValue configuration, GenAIConfig genAIConfig);

        record Impl(ImmutableList<Provider> providers, HttpService httpService) implements Providers {
            @Override
            public Provider.Implementation configure(String name, MapValue configuration, GenAIConfig genAIConfig) {
                return byName(name).configure(httpService, configuration, genAIConfig);
            }
        }
    }
}
