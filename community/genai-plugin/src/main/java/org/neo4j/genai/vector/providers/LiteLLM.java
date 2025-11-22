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
package org.neo4j.genai.vector.providers;

import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.util.HttpClient;
import org.neo4j.genai.vector.VectorEncoding.Provider;
import org.neo4j.genai.vector.providers.openai.OpenAIBasedEncoder;

import java.net.URI;
import java.util.OptionalLong;

/**
 * for develop and testing with LiteLLM
 */
@ServiceProvider
public final class LiteLLM implements Provider<LiteLLM.Parameters> {
    public static final String NAME = "LiteLLM";
    public static final String DEFAULT_ENDPOINT = "http://localhost:4000/v1/embeddings";

    private final HttpClient client = new HttpClient();

    @Override
    public Class<Parameters> parameterDeclarations() {
        return Parameters.class;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Provider.Encoder configure(Parameters configuration) {
        return new Encoder(client, configuration);
    }

    public static class Parameters {
        public String model;
        public String endpoint = DEFAULT_ENDPOINT;
        public OptionalLong dimensions;
        //for some reason ,some modules does not support encoding_format(like Ollama)
        public boolean supportEncodingFormat = false;
    }

    static class Encoder extends OpenAIBasedEncoder {
        private final Parameters configuration;

        Encoder(HttpClient client, Parameters configuration) {
            super(NAME, client, URI.create(configuration.endpoint), configuration.dimensions);
            this.configuration = configuration;
        }

//        @Override
//        protected void extendHeaders(MutableMultimap<String, String> headers) {
//            headers.put("api-key", "dummy-lite-llm-key");
//        }

        @Override
        protected void extendPayload(MutableMap<String, Object> payload) {
            if (!configuration.supportEncodingFormat) {
                payload.remove("encoding_format");
            }
        }

    }
}
