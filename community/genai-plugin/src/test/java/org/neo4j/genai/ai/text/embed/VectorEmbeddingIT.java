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
package org.neo4j.genai.ai.text.embed;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.genai.ai.Tokens;
import org.neo4j.genai.util.GenAITestExtension;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.VectorValue;

public class VectorEmbeddingIT {

    @Nested
    @EnabledIfEnvironmentVariable(named = Tokens.AzureOpenAi.TOKEN_ENV, matches = ".*")
    @EnabledIfEnvironmentVariable(named = Tokens.AzureOpenAi.RESOURCE_ENV, matches = ".*")
    class AzureOpenAi extends VectorEmbeddingITBase {
        @Override
        String provider() {
            return "azure-openai";
        }

        @Override
        Map<String, Object> params() {
            return Map.of(
                    "token",
                    System.getenv(Tokens.AzureOpenAi.TOKEN_ENV),
                    "resource",
                    System.getenv(Tokens.AzureOpenAi.RESOURCE_ENV));
        }

        @Override
        List<String> confRequired() {
            return List.of("{ token: $token, resource: $resource, model: 'text-embedding-3-small' }");
        }

        @Override
        List<String> confWithVendorOptions() {
            return List.of(
                    "{ token: $token, resource: $resource, model: 'text-embedding-3-small', vendorOptions: { dimensions: 1536, user: 'gem' }}");
        }
    }

    @Nested
    @EnabledIfEnvironmentVariable(named = Tokens.OpenAi.TOKEN_ENV, matches = ".*")
    class OpenAi extends VectorEmbeddingITBase {
        @Override
        Map<String, Object> params() {
            return Map.of("token", System.getenv(Tokens.OpenAi.TOKEN_ENV));
        }

        @Override
        List<String> confRequired() {
            return List.of("{ token: $token, model: 'text-embedding-3-small' }");
        }

        @Override
        List<String> confWithVendorOptions() {
            return List.of(
                    "{ token: $token, model: 'text-embedding-3-small', vendorOptions: { dimensions: 1536, user: 'gem' } }");
        }
    }

    @Nested
    @EnabledIfEnvironmentVariable(named = Tokens.Vertex.TOKEN_ENV, matches = ".*")
    @EnabledIfEnvironmentVariable(named = Tokens.Vertex.PROJECT_ENV, matches = ".*")
    class VertexAi extends VectorEmbeddingITBase {

        @Override
        Map<String, Object> params() {
            return Map.of(
                    "token",
                    System.getenv(Tokens.Vertex.TOKEN_ENV),
                    "project",
                    System.getenv(Tokens.Vertex.PROJECT_ENV),
                    "region",
                    System.getenv(Tokens.Vertex.REGION_ENV));
        }

        @Override
        List<String> confRequired() {
            var isApiKeyEnv = System.getenv(Tokens.Vertex.IS_API_KEY);
            var isApiKey = isApiKeyEnv != null && isApiKeyEnv.equalsIgnoreCase("true");
            var tokenOrKey = isApiKey ? "apiKey" : "token";
            return List.of("{ %s: $token, model: 'gemini-embedding-001', region: $region, project: $project}"
                    .formatted(tokenOrKey));
        }

        @Override
        List<String> confWithVendorOptions() {
            var isApiKeyEnv = System.getenv(Tokens.Vertex.IS_API_KEY);
            var isApiKey = isApiKeyEnv != null && isApiKeyEnv.equalsIgnoreCase("true");
            var tokenOrKey = isApiKey ? "apiKey" : "token";
            return List.of(
                    "{ %s: $token, model: 'gemini-embedding-001', region: $region, project: $project, vendorOptions: { autoTruncate: true, task_type: 'QUESTION_ANSWERING' }}"
                            .formatted(tokenOrKey));
        }
    }

    @Nested
    @EnabledIfEnvironmentVariable(named = Tokens.Bedrock.ACCESS_KEY_ENV, matches = ".*")
    @EnabledIfEnvironmentVariable(named = Tokens.Bedrock.SECRET_ACCESS_KEY_ENV, matches = ".*")
    class BedrockTitan extends VectorEmbeddingITBase {
        static final String KEY = "BEDROCK_KEY";
        static final String SECRET = "BEDROCK_SECRET_KEY";

        @Override
        String provider() {
            return "bedrock-titan";
        }

        @Override
        Map<String, Object> params() {
            return Map.of(
                    "key",
                    System.getenv(Tokens.Bedrock.ACCESS_KEY_ENV),
                    "secret",
                    System.getenv(Tokens.Bedrock.SECRET_ACCESS_KEY_ENV),
                    "region",
                    System.getenv(Tokens.Bedrock.REGION_ENV));
        }

        @Override
        List<String> confRequired() {
            return List.of(
                    "{ model: 'amazon.titan-embed-text-v1', region: 'us-east-1', accessKeyId: $key, secretAccessKey: $secret }",
                    "{ model: 'amazon.titan-embed-text-v2:0', region: 'eu-west-2', accessKeyId: $key, secretAccessKey: $secret }",
                    "{ model: 'arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1', region: 'us-east-1', accessKeyId: $key, secretAccessKey: $secret }");
        }

        @Override
        List<String> confWithVendorOptions() {
            return List.of(
                    "{ model: 'amazon.titan-embed-text-v2:0', region: 'eu-west-2', accessKeyId: $key, secretAccessKey: $secret, vendorOptions: { dimensions: 1024, normalize: true, embeddingTypes: ['float'] } }",
                    "{ model: 'arn:aws:bedrock:us-west-2::foundation-model/amazon.titan-embed-text-v2:0', region: 'us-west-2', accessKeyId: $key, secretAccessKey: $secret, vendorOptions: { dimensions: 1024, normalize: true, embeddingTypes: ['float'] } }");
        }
    }
}

@ImpermanentDbmsExtension(configurationCallback = "configure")
abstract class VectorEmbeddingITBase implements GenAITestExtension {

    @Inject
    GraphDatabaseAPI db;

    @Inject
    TestDirectory testDirectory;

    abstract Map<String, Object> params();

    abstract List<String> confRequired();

    abstract List<String> confWithVendorOptions();

    String provider() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        installPlugin(testDirectory);
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25);
        // Avoid logging the tokens
        builder.setConfig(GraphDatabaseSettings.log_queries_parameter_logging_enabled, false);
    }

    @Test
    void embedWithRequiredArgs() {
        for (final var conf : confRequired()) {
            assertNonNullVectorResult("""
                            WITH %s AS conf
                            WITH ai.text.embed('Hello!', '%s', conf) AS result
                            RETURN result""".formatted(conf, provider()));
        }
    }

    @Test
    void embedWithAllArgs() {
        for (final var conf : confWithVendorOptions()) {
            assertNonNullVectorResult("""
                            WITH %s AS conf
                            WITH ai.text.embed('Hello!', '%s', conf) AS result
                            RETURN result""".formatted(conf, provider()));
        }
    }

    @Test
    void batchEmbedWithRequiredArgs() {
        for (final var conf : confRequired()) {
            final var query = """
                            CALL ai.text.embedBatch(
                              ['Testing', null, 'Hello!'],
                              '%s',
                              %s
                            )
                            """.formatted(provider(), conf);
            assertThat(db.executeTransactionally(query, params(), consume()))
                    .as("Query:%n```%n%s%n```%n", query)
                    .satisfiesExactly(batchedNonBlankRow(0), batchedNullRow(1), batchedNonBlankRow(2));
        }
    }

    @Test
    void batchEmbedWithAllArgs() {
        for (final var conf : confWithVendorOptions()) {
            final var query = """
                            CALL ai.text.embedBatch(
                              ['Testing', null, 'Hello!'],
                              '%s',
                              %s
                            )
                            """.formatted(provider(), conf);
            assertThat(db.executeTransactionally(query, params(), consume()))
                    .as("Query:%n```%n%s%n```%n", query)
                    .satisfiesExactly(batchedNonBlankRow(0), batchedNullRow(1), batchedNonBlankRow(2));
        }
    }

    private void assertNonNullVectorResult(String query) {
        final var result = db.executeTransactionally(query, params(), consume());
        assertThat(result)
                .as("Query:%n```%n%s%n```%n", query)
                .singleElement(resultMap())
                .extracting("result")
                .asInstanceOf(InstanceOfAssertFactories.type(VectorValue.class))
                .isNotNull();
    }

    private Consumer<Map<String, Object>> batchedNullRow(long index) {
        return row -> assertThat(row).containsEntry("index", index).containsEntry("vector", null);
    }

    private Consumer<Map<String, Object>> batchedNonBlankRow(long index) {
        return row -> assertThat(row)
                .containsEntry("index", index)
                .hasEntrySatisfying(
                        "vector",
                        c -> assertThat(c)
                                .asInstanceOf(InstanceOfAssertFactories.type(VectorValue.class))
                                .isNotNull());
    }
}
