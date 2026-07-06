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

import static java.util.Objects.requireNonNull;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntLists;
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
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.MapValue;

public class VectorEmbedding {
    private static final String CONF_DESC =
            "Provider specific configuration, use `CALL ai.text.embed.providers()` to find the configuration needed for each provider. You can specify additional vendor options by adding `vendorOptions` with a map of values that will be passed along in the request.";
    private static final String PROVIDER_DESC =
            "The identifier of the provider: 'Azure-OpenAI', 'Bedrock-Titan', 'OpenAI', 'VertexAI'.";

    @Context
    public Providers providers;

    @Context
    public HttpService httpService;

    @Context
    public GenAIConfig genAIConfig;

    @Procedure(name = "ai.text.embed.providers")
    @Description("Lists the available vector embedding providers.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<ProviderRow> listProviders() {
        return providers.providers().stream().map(ProviderRow::from).sorted(Comparator.comparing(ProviderRow::name));
    }

    @UserFunction(name = "ai.text.embed")
    @Description("Encode a given resource as a vector using the named provider.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public VectorValue encode(
            @Name(value = "resource", description = "The object to transform into an embedding.") String resource,
            @Name(value = "provider", description = PROVIDER_DESC) String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}", description = CONF_DESC)
                    MapValue configuration) {
        requireNonNull(providerName, "'provider' must not be null");
        requireNonNull(configuration, "'configuration' must not be null");
        final var provider = providers.configure(providerName, configuration, genAIConfig);

        if (resource == null || resource.isEmpty()) {
            return null;
        } else {
            return provider.encode(resource);
        }
    }

    @Procedure(name = "ai.text.embedBatch")
    @Description("""
            Encode a given batch of resources as vectors using the named provider.
            For each element in the given resource LIST this returns:
                * the corresponding 'index' within that LIST,
                * the original 'resource' element itself,
                * and the encoded 'vector'.
            """)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<InternalBatchRow> encode(
            @Name(value = "resources", description = "The object to transform into an embedding.")
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

        // Not all providers use batching, so in those cases there is no limit, just return
        // the entire thing encoded
        var maxBatchSize = provider.maxBatchSize();
        if (maxBatchSize > 0) {
            var encoding = getEncoding(configuration.get("model").toString());
            var currentOffset = 0;
            var currentBatchSize = 0;
            var newResources = new ArrayList<String>();
            var streams = new ArrayList<Stream<InternalBatchRow>>();
            for (var resource : resources) {
                var resourceSize = resource == null ? 0 : encoding.countTokens(resource);
                if (currentBatchSize + resourceSize > maxBatchSize && currentBatchSize != 0) {
                    // Close current bucket, send away
                    streams.add(encodePartialBatch(newResources, provider, currentOffset));
                    currentBatchSize = 0;
                    currentOffset += newResources.size();
                    newResources = new ArrayList<>();
                    // Add current resource to the bucket
                    newResources.add(resource);
                    currentBatchSize += resourceSize;
                } else if (currentBatchSize + resourceSize > maxBatchSize && currentBatchSize == 0) {
                    // The resource is really large, we won't do any splitting, just send away, user error
                    newResources.add(resource);
                    streams.add(encodePartialBatch(newResources, provider, currentOffset));
                    currentOffset += newResources.size();
                    newResources = new ArrayList<>();
                } else {
                    // We have more room in the bucket
                    newResources.add(resource);
                    currentBatchSize += resourceSize;
                }
            }
            if (!newResources.isEmpty()) {
                streams.add(encodePartialBatch(newResources, provider, currentOffset));
            }
            return streams.stream().flatMap(stream -> stream);
        } else {
            return encodePartialBatch(resources, provider, 0);
        }
    }

    public static Stream<InternalBatchRow> encodePartialBatch(
            List<String> resources, Provider.Implementation provider, int currentOffset) {
        // Remember all the places where we had nulls and remove them from the requested resources
        final var removedIndexes = IntLists.mutable.empty();
        // We need to make a copy as the List interface doesn't guarantee mutability
        // We assume that most of the resources are not null, so we reserve space for all
        final var cleanedResources = Lists.mutable.<String>withInitialCapacity(resources.size());
        for (var it = resources.listIterator(); it.hasNext(); ) {
            final var index = it.nextIndex();
            final var resource = it.next();

            if (resource == null || resource.isEmpty()) {
                removedIndexes.add(index);
            } else {
                cleanedResources.add(resource);
            }
        }

        if (cleanedResources.isEmpty()) {
            return IntStream.range(0, removedIndexes.size()).mapToObj(index -> new InternalBatchRow(index, null, null));
        }

        return provider.encodeBatch(cleanedResources, removedIndexes.toArray(), currentOffset);
    }

    private Encoding getEncoding(String model) {
        final var registry = Encodings.newDefaultEncodingRegistry();
        return (model == null || model.isEmpty())
                ? registry.getEncoding(EncodingType.R50K_BASE)
                : registry.getEncodingForModel(model).orElseGet(() -> registry.getEncoding(EncodingType.R50K_BASE));
    }

    public interface Provider extends NamedProvider {
        Provider.Implementation configure(HttpService httpService, MapValue configuration, GenAIConfig genAIConfig);

        interface Implementation extends NamedProvider.Implementation {
            long maxBatchSize();

            VectorValue encode(String resource);

            Stream<VectorEmbedding.InternalBatchRow> encodeBatch(
                    List<String> resources, int[] nullIndexes, int batchOffset);
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

    public record InternalBatchRow(
            @Description("The index of the corresponding element in the input list.")
            long index,

            @Description("The name of the input resource.") String resource,

            @Description("The generated vector embedding for the resource.")
            VectorValue vector) {}
}
