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

import java.net.URI;
import java.util.OptionalLong;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.util.HttpClient;
import org.neo4j.genai.vector.VectorEncoding.Provider;
import org.neo4j.genai.vector.providers.openai.OpenAIBasedEncoder;

@ServiceProvider
public final class OpenAI implements Provider<OpenAI.Parameters> {
    public static final String NAME = "OpenAI";
    private static final URI ENDPOINT = URI.create("https://api.openai.com/v1/embeddings");
    static final String DEFAULT_MODEL = "text-embedding-ada-002";
    private final HttpClient client = new HttpClient();

    public static class Parameters {
        public String token;
        public String model = DEFAULT_MODEL;
        public OptionalLong dimensions;
    }

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
        return new Encoder(client, ENDPOINT, configuration);
    }

    static class Encoder extends OpenAIBasedEncoder {
        private final Parameters configuration;

        Encoder(HttpClient client, URI endpoint, Parameters configuration) {
            super(NAME, client, endpoint, configuration.dimensions);
            this.configuration = configuration;
        }

        @Override
        protected void extendHeaders(MutableMultimap<String, String> headers) {
            headers.put("Authorization", "Bearer " + configuration.token);
        }

        @Override
        protected void extendPayload(MutableMap<String, Object> payload) {
            payload.put("model", configuration.model);
        }
    }
}
