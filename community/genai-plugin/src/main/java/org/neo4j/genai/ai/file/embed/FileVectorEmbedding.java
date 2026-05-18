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
package org.neo4j.genai.ai.file.embed;

import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.ai.text.tokenChunking.RecursiveTokenSplitter;
import org.neo4j.genai.ai.text.tokenChunking.TextChunkConfig;
import org.neo4j.genai.util.GenAIProcedureException;
import org.neo4j.genai.util.ResourceLoader;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Sensitive;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class FileVectorEmbedding {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.text.embed.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC =
            "The identifier of the provider: 'Azure-OpenAI', 'Bedrock-Titan', 'OpenAI', 'VertexAI'.";

    @Context
    public URLAccessChecker urlAccessChecker;

    @Context
    public GenAIConfig genAIConfig;

    @Context
    public VectorEmbedding.Providers providers;

    @Context
    public SecurityContext securityContext;

    @Procedure(name = "ai.file.embedBatch")
    @Description("""
            Embed a given file in batches of resources as vectors using the named provider.
            The given file will be read, and if applicable, chunked into a list of resources which are then batch embedded.
            If no chunking, the embedding will be done as if a list of a single item was provider,
            if the file is chunked the the chunks will be provided in an ordered list and the procedure will return:
                * the corresponding 'index' within that LIST,
                * the original 'resource' element itself,
                * and the encoded 'vector'.
            """)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<VectorEmbedding.InternalBatchRow> encode(
            @Name(value = "file", description = "The path of the file") String file,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration,
            @Name(
                            value = "tokenCountLimit",
                            description =
                                    "The maximum token count limit for each chunk, if null, chunking will not be applied.",
                            defaultValue = "null")
                    Long limit) {
        requireNonNull(file, "'file' must not be null");
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");

        AnyValue modelValue = configuration.get("model");
        String model = (modelValue instanceof TextValue) ? ((TextValue) modelValue).stringValue() : "ada";

        TextChunkConfig textChunkConfig = new TextChunkConfig(limit, 0L, model);

        InputStream inputStream;
        try {
            inputStream = openFile(file);
        } catch (GenAIProcedureException e) {
            throw e;
        } catch (URLAccessValidationError | SecurityException | AuthorizationViolationException e) {
            throw new GenAIProcedureException(e.getMessage(), e);
        } catch (NoSuchFileException | FileNotFoundException e) {
            throw new GenAIProcedureException("File not found: " + file);
        } catch (Exception e) {
            throw new GenAIProcedureException("Failed to read file: " + file, e);
        }

        try {
            if (textChunkConfig.shouldUseChunking()) {
                return readFileWithChunking(inputStream, textChunkConfig, providerName, configuration);
            } else {
                return readFile(inputStream, providerName, configuration);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream openFile(String file) throws IOException, URLAccessValidationError {
        return ResourceLoader.openResource(file, urlAccessChecker, genAIConfig, securityContext);
    }

    private Stream<VectorEmbedding.InternalBatchRow> readFile(
            InputStream inputStream, String providerName, MapValue configuration) throws IOException {
        String content;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            content = reader.lines().collect(Collectors.joining("\n"));
        }

        var provider = providers.configure(providerName, configuration, genAIConfig);
        return VectorEmbedding.encodePartialBatch(List.of(content), provider, 0);
    }

    private Stream<VectorEmbedding.InternalBatchRow> readFileWithChunking(
            InputStream inputStream, TextChunkConfig textChunkConfig, String providerName, MapValue configuration)
            throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            final var provider = providers.configure(providerName, configuration, genAIConfig);

            var encoding = textChunkConfig.getEncoding();
            var splitter = new RecursiveTokenSplitter(encoding, textChunkConfig.getLimit(), 0);
            var currentOffset = 0;
            var currentBatchSize = 0;
            var newResources = new StringBuilder();
            var streams = new ArrayList<Stream<VectorEmbedding.InternalBatchRow>>();
            String line;

            while ((line = reader.readLine()) != null) {
                var resourceSize = encoding.countTokens(line);
                if (resourceSize > textChunkConfig.getLimit()) {
                    // The line alone exceeds the limit. Flush the current bucket (if any),
                    // then split the line into smaller chunks via RecursiveTokenSplitter.
                    if (currentBatchSize != 0) {
                        streams.add(VectorEmbedding.encodePartialBatch(
                                List.of(newResources.toString()), provider, currentOffset));
                        currentOffset += 1;
                        currentBatchSize = 0;
                        newResources = new StringBuilder();
                    }
                    for (String subChunk : splitter.splitText(line)) {
                        streams.add(VectorEmbedding.encodePartialBatch(List.of(subChunk), provider, currentOffset));
                        currentOffset += 1;
                    }
                } else if (currentBatchSize + resourceSize > textChunkConfig.getLimit() && currentBatchSize != 0) {
                    // Close current bucket, send away
                    streams.add(VectorEmbedding.encodePartialBatch(
                            List.of(newResources.toString()), provider, currentOffset));
                    currentOffset += 1;
                    newResources = new StringBuilder();
                    // Add current resource to the bucket
                    newResources.append(line);
                    currentBatchSize = resourceSize;
                } else {
                    // We have more room in the bucket
                    if (!newResources.isEmpty()) {
                        newResources.append("\n");
                    }
                    newResources.append(line);
                    currentBatchSize += resourceSize;
                }
            }
            if (!newResources.isEmpty()) {
                streams.add(
                        VectorEmbedding.encodePartialBatch(List.of(newResources.toString()), provider, currentOffset));
            } else if (streams.isEmpty()) {
                streams.add(VectorEmbedding.encodePartialBatch(List.of(""), provider, 0));
            }
            return streams.stream().flatMap(stream -> stream);
        }
    }
}
