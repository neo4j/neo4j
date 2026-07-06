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

import static java.util.Objects.requireNonNull;

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
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Sensitive;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.virtual.MapValue;

public class TextStructuredCompletion {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.text.structuredCompletion.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC =
            "The identifier of the provider: 'Azure-OpenAI', 'Bedrock', 'OpenAI', 'VertexAI'.";

    @Context
    public Providers providers;

    @Context
    public GenAIConfig genAIConfig;

    @NotThreadSafe
    @UserFunction(name = "ai.text.structuredCompletion")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Generate structured output as a map based on the specified prompt and JSON schema.")
    public MapValue complete(
            @Name(value = "prompt", description = "The prompt to generate output from.") String prompt,
            @Name(value = "schema", description = "A map describing the JSON schema for the desired output.")
                    MapValue schema,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration) {
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");
        requireNonNull(schema, "'schema' must not be null");
        if (schema.isEmpty()) {
            // A result cannot be contained within the schema, so return an empty map
            return schema;
        }
        final var provider = providers.configure(providerName, configuration, genAIConfig);
        return prompt == null ? null : provider.complete(prompt, schema);
    }

    @Procedure(name = "ai.text.structuredCompletion.providers")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Lists the available structured text completion providers.")
    public Stream<ProviderRow> listProviders() {
        return providers.providers().stream().map(ProviderRow::from);
    }

    public interface Provider extends NamedProvider {
        Implementation configure(HttpService httpService, MapValue configuration, GenAIConfig genAIConfig);

        interface Implementation extends NamedProvider.Implementation {
            MapValue complete(String prompt, MapValue schema);
        }
    }

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
