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
package org.neo4j.genai.ai.image.embed;

import static java.util.Objects.requireNonNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.util.GenAIProcedureException;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.ResourceLoader;
import org.neo4j.genai.util.provider.NamedProvider;
import org.neo4j.genai.util.provider.ProviderRow;
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
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.MapValue;

public class ImageVectorEmbedding {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.image.embed.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC = "The identifier of the provider: 'Bedrock-Titan', 'VertexAI'.";

    @Context
    public Providers providers;

    @Context
    public HttpService httpService;

    @Context
    public GenAIConfig genAIConfig;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Context
    public SecurityContext securityContext;

    @Procedure(name = "ai.image.embed.providers")
    @Description("Lists the available image vector embedding providers.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<ProviderRow> listProviders() {
        return providers.providers().stream().map(ProviderRow::from).sorted(Comparator.comparing(ProviderRow::name));
    }

    @UserFunction(name = "ai.image.embed")
    @Description("Encode a given image as a vector using the named provider.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public VectorValue encode(
            @Name(
                            value = "resource",
                            description =
                                    "The image to transform into an embedding: either base64-encoded image bytes, a local file path (file:///path/to/image), or a remote URL (http/https/ftp).")
                    String resource,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration) {
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");
        final var provider = providers.configure(providerName, configuration, genAIConfig);
        if (resource == null || resource.isEmpty()) {
            return null;
        } else {
            return provider.encode(resolveToBase64(resource));
        }
    }

    @Procedure(name = "ai.image.embedBatch")
    @Description("""
            Encode a given batch of images as vectors using the named provider.
            For each element in the given resource LIST this returns:
                * the corresponding 'index' within that LIST,
                * the resolved 'resource' element (base64 bytes),
                * and the encoded 'vector'.
            """)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<VectorEmbedding.InternalBatchRow> encodeBatch(
            @Name(
                            value = "resources",
                            description =
                                    "The images to transform into embeddings: base64-encoded image bytes, local file paths (file:///path/to/image), or remote URLs (http/https/ftp).")
                    List<String> resources,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration) {
        requireNonNull(resources, "'resources' must not be null");
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");
        if (resources.isEmpty()) {
            return Stream.empty();
        }
        final var provider = providers.configure(providerName, configuration, genAIConfig);
        var resolved = resources.stream()
                .map(r -> (r == null || r.isEmpty()) ? r : resolveToBase64(r))
                .toList();
        return VectorEmbedding.encodePartialBatch(resolved, provider, 0);
    }

    private String resolveToBase64(String resource) {
        if (!ResourceLoader.isFileReference(resource)) {
            return resource;
        }
        try {
            try (InputStream is =
                    ResourceLoader.openResource(resource, urlAccessChecker, genAIConfig, securityContext)) {
                return Base64.getEncoder().encodeToString(is.readAllBytes());
            }
        } catch (GenAIProcedureException e) {
            throw e;
        } catch (URLAccessValidationError | SecurityException | AuthorizationViolationException e) {
            throw new GenAIProcedureException(e.getMessage(), e);
        } catch (NoSuchFileException | FileNotFoundException e) {
            throw new GenAIProcedureException("File not found: " + resource);
        } catch (IOException e) {
            throw new GenAIProcedureException("Failed to read resource: " + resource, e);
        }
    }

    public interface Provider extends NamedProvider {
        VectorEmbedding.Provider.Implementation configure(
                HttpService httpService, MapValue configuration, GenAIConfig genAIConfig);
    }

    public interface Providers extends NamedProvider.Lookup<Provider> {
        VectorEmbedding.Provider.Implementation configure(String name, MapValue configuration, GenAIConfig genAIConfig);

        record Impl(ImmutableList<Provider> providers, HttpService httpService) implements Providers {
            @Override
            public VectorEmbedding.Provider.Implementation configure(
                    String name, MapValue configuration, GenAIConfig genAIConfig) {
                return byName(name).configure(httpService, configuration, genAIConfig);
            }
        }
    }
}
