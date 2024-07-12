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
package org.neo4j.genai.vector;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.collection.mutable.CollectionAdapter;
import org.neo4j.annotations.service.Service;
import org.neo4j.genai.util.Parameters;
import org.neo4j.genai.util.Parameters.Parameter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Sensitive;
import org.neo4j.procedure.UserFunction;
import org.neo4j.service.Services;
import org.neo4j.util.Preconditions;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public class VectorEncoding {
    public static String VERSION = "1.2.0";

    @SuppressWarnings("rawtypes")
    private static final ImmutableList<Provider> PROVIDERS = Lists.immutable.withAllSorted(
            Comparator.comparing(Provider::name, CASE_INSENSITIVE_ORDER),
            CollectionAdapter.adapt(Services.loadAll(Provider.class)));

    private static volatile VectorEncodingCallCountersMonitor MONITOR;

    @Context
    public GraphDatabaseService graphDatabaseService;

    @Procedure(name = "genai.vector.listEncodingProviders")
    @Description("Lists the available vector embedding providers.")
    public Stream<ProviderRow> listEncodingProviders() {
        return PROVIDERS.stream().map(ProviderRow::from);
    }

    public record ProviderRow(
            String name, String requiredConfigType, String optionalConfigType, Map<String, Object> defaultConfig) {
        public static ProviderRow from(Provider<?> provider) {
            final var parameters = Parameters.getParameters(provider.parameterDeclarations());

            return new ProviderRow(
                    provider.name(),
                    requiredConfigType(parameters),
                    optionalConfigType(parameters),
                    defaultConfig(parameters));
        }

        private static String requiredConfigType(List<Parameter> parameters) {
            return cypherMapType(parameters.stream().filter(Parameter::isRequired));
        }

        private static String optionalConfigType(List<Parameter> parameters) {
            return cypherMapType(parameters.stream().filter(Parameter::isOptional));
        }

        private static String cypherMapType(Stream<Parameter> parameters) {
            return parameters
                    .map(p -> "%s :: %s".formatted(p.name(), p.type().cypherName()))
                    .collect(Collectors.joining(", ", "{ ", " }"));
        }

        private static Map<String, Object> defaultConfig(List<Parameter> parameters) {
            // This matches the previous behaviour of not listing defaults of Optional.empty().
            final MutableMap<String, Object> defaults = Maps.mutable.empty();
            for (var parameter : parameters) {
                final var defaultValue = parameter.defaultValue();
                if (defaultValue == null) {
                    continue;
                }

                // Unwrap nullables if present; otherwise they are not included in the defaults map.
                if (defaultValue instanceof final Optional<?> optionalDefaultValue) {
                    optionalDefaultValue.ifPresent(o -> defaults.put(parameter.name(), o));
                } else if (defaultValue instanceof final OptionalLong optionalDefaultValue) {
                    optionalDefaultValue.ifPresent(o -> defaults.put(parameter.name(), o));
                } else if (defaultValue instanceof final OptionalDouble optionalDefaultValue) {
                    optionalDefaultValue.ifPresent(o -> defaults.put(parameter.name(), o));
                } else {
                    defaults.put(parameter.name(), defaultValue);
                }
            }
            return defaults.asUnmodifiable();
        }
    }

    @UserFunction(name = "genai.vector.encode")
    @Description("Encode a given resource as a vector using the named provider.")
    public Value encode(
            @Name("resource") String resource,
            @Name("provider") String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}") AnyValue configuration) {
        requireNonNull(providerName, "'provider' must not be null");
        final var configurationMap = requireNonNullMap(configuration);
        final var provider = getProvider(providerName);
        getMonitor().encodeFunctionCalled(provider.name());
        if (resource == null) {
            return NO_VALUE;
        } else {
            return Values.floatArray(provider.configure(configurationMap).encode(resource));
        }
    }

    @Procedure(name = "genai.vector.encodeBatch")
    @Description(
            """
            Encode a given batch of resources as vectors using the named provider.
            For each element in the given resource LIST this returns:
                * the corresponding 'index' within that LIST,
                * the original 'resource' element itself,
                * and the encoded 'vector'.
            """)
    public Stream<InternalBatchRow> encode(
            @Name("resources") List<String> resources,
            @Name("provider") String providerName,
            @Sensitive @Name(value = "configuration", defaultValue = "{}") AnyValue configuration) {
        requireNonNull(resources, "'resources' must not be null");
        Preconditions.checkArgument(!resources.isEmpty(), "'resources' must not be empty");
        requireNonNull(providerName, "'provider' must not be null");
        final var configurationMap = requireNonNullMap(configuration);
        final var provider = getProvider(providerName);
        getMonitor().encodeBatchProcedureCalled(provider.name());
        // Remember all the places where we had nulls and remove them from the requested resources
        final var removedIndexes = IntLists.mutable.empty();
        // We need to make a copy as the List interface doesn't guarantee mutability
        // We assume that most of the resources are not null, so we reserve space for all
        final var cleanedResources = Lists.mutable.<String>withInitialCapacity(resources.size());
        for (var it = resources.listIterator(); it.hasNext(); ) {
            final var index = it.nextIndex();
            final var resource = it.next();

            if (resource == null) {
                removedIndexes.add(index);
            } else {
                cleanedResources.add(resource);
            }
        }

        return provider.configure(configurationMap)
                .encode(cleanedResources, removedIndexes.toArray())
                .map(InternalBatchRow::new);
    }

    private static MapValue requireNonNullMap(AnyValue configuration) {
        if (configuration == NO_VALUE) {
            throw new IllegalArgumentException("'configuration' must not be null");
        }

        if (!(configuration instanceof final MapValue map)) {
            throw new IllegalArgumentException("'configuration' must be a map");
        }

        return map;
    }

    VectorEncodingCallCountersMonitor getMonitor() {
        var theMonitor = MONITOR;
        if (theMonitor == null) {
            synchronized (VectorEncoding.class) {
                theMonitor = MONITOR;
                if (theMonitor == null) {
                    MONITOR = getMonitor0();
                    theMonitor = MONITOR;
                }
            }
        }
        return theMonitor;
    }

    private VectorEncodingCallCountersMonitor getMonitor0() {
        Monitors monitors;
        // Using the dependency resolver or graph database api directly would sandbox the UDFs and we don't want that,
        // so that this plugin keeps usable without any additional configuration.
        if (this.graphDatabaseService != null
                && this.graphDatabaseService instanceof GraphDatabaseAPI graphDatabaseAPI) {
            monitors = graphDatabaseAPI.getDependencyResolver().resolveDependency(Monitors.class);
        } else {
            monitors = new Monitors();
        }
        return monitors.newMonitor(VectorEncodingCallCountersMonitor.class);
    }

    static Provider<?> getProvider(String name) {
        for (final var provider : PROVIDERS) {
            if (CASE_INSENSITIVE_ORDER.compare(provider.name(), name) == 0) {
                return provider;
            }
        }

        throw new RuntimeException("Vector encoding provider not supported: %s".formatted(name));
    }

    @Service
    public interface Provider<PARAMETERS> {
        Class<PARAMETERS> parameterDeclarations();

        String name();

        default Encoder configure(MapValue configuration) {
            return configure(Parameters.parse(parameterDeclarations(), configuration));
        }

        Encoder configure(PARAMETERS configuration);

        interface Encoder {
            float[] encode(String resource);

            default Stream<BatchRow> encode(List<String> resources, int[] nullIndexes) {
                final MutableInt offset = new MutableInt();
                return IntStream.range(0, resources.size() + nullIndexes.length).mapToObj(index -> {
                    // We need to reinsert nulls at the right place
                    if (Arrays.binarySearch(nullIndexes, index) >= 0) {
                        offset.increment();
                        return new BatchRow(index, null, null);
                    }
                    final int offsetIndex = index - offset.intValue();
                    final var resource = resources.get(offsetIndex);
                    return new BatchRow(index, resource, encode(resource));
                });
            }
        }
    }

    public record BatchRow(long index, String resource, float[] vector) {}

    public record InternalBatchRow(long index, String resource, Value vector) {
        InternalBatchRow(BatchRow row) {
            this(row.index(), row.resource(), Values.of(row.vector));
        }
    }
}
