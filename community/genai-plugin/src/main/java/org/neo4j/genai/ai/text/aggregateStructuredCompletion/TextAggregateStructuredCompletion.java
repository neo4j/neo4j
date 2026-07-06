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

import static java.util.Objects.requireNonNull;

import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.provider.NamedProvider;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Sensitive;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.values.virtual.MapValue;

public class TextAggregateStructuredCompletion {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.text.structuredCompletion.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC =
            "The identifier of the provider: 'Azure-OpenAI', 'Bedrock', 'Bedrock-Nova', 'Bedrock-Titan', 'OpenAI', 'VertexAI'.";

    @Context
    public TextStructuredCompletion.Providers providers;

    @Context
    public GenAIConfig genAIConfig;

    @NotThreadSafe
    @UserAggregationFunction(name = "ai.text.aggregateStructuredCompletion")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Generate text based on the specified prompt.")
    public CompleteFunction aggregateStructuredComplete() {
        return new CompleteFunction(providers, genAIConfig);
    }

    public static class CompleteFunction {

        public TextStructuredCompletion.Providers providers;
        public GenAIConfig genAIConfig;

        private final StringBuilder stringBuilder = new StringBuilder();
        private String providerName;
        private MapValue configuration;
        private MapValue schema;

        public CompleteFunction(TextStructuredCompletion.Providers providers, GenAIConfig genAIConfig) {
            this.providers = providers;
            this.genAIConfig = genAIConfig;
        }

        @UserAggregationUpdate
        public void update(
                @Name(value = "value", description = "The value to aggregate over.") String value,
                @Name(value = "prompt", description = "The prompt to generate text from.") String prompt,
                @Name(value = "schema", description = "A map describing the JSON schema for the desired output.")
                        MapValue schema,
                @Name(value = "provider", description = PROVIDER_DESC) String providerName,
                @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                        MapValue configuration) {
            if (stringBuilder.isEmpty()) {
                stringBuilder.append(prompt);
                stringBuilder.append("\n");
            }
            if (value != null) {
                stringBuilder.append(value);
                stringBuilder.append("\n");
            }
            this.providerName = providerName;
            this.schema = schema;
            this.configuration = configuration;
        }

        @UserAggregationResult
        public MapValue result() {
            // Nothing to aggregate over, return null
            if (stringBuilder.isEmpty()) {
                return null;
            }
            requireNonNull(providerName, "'provider' must not be null");
            requireNonNull(configuration, "'configuration' must not be null");
            final var provider = providers.configure(providerName, configuration, genAIConfig);
            if (schema.isEmpty()) {
                // A result cannot be contained within the schema, so return an empty map
                return schema;
            }
            var newPrompt = stringBuilder.toString();
            return provider.complete(newPrompt, schema);
        }
    }

    public interface Provider extends NamedProvider {
        Implementation configure(HttpService httpService, MapValue configuration, GenAIConfig genAIConfig);

        interface Implementation extends NamedProvider.Implementation {
            String complete(String prompt);
        }
    }
}
