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
package org.neo4j.kernel.impl.transaction.state;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class StaticIndexProviderMap extends LifecycleAdapter implements IndexProviderMap {
    private static final Comparator<IndexProvider> DESCENDING_MINIMUM_KERNEL_VERSION =
            Comparator.comparing(IndexProvider::getMinimumRequiredVersion).reversed();

    private final Map<IndexProviderDescriptor, IndexProvider> indexProvidersByDescriptor = new HashMap<>();
    private final Map<String, IndexProvider> indexProvidersByName = new HashMap<>();
    private final Map<IndexType, List<IndexProvider>> indexProvidersByType = new EnumMap<>(IndexType.class);
    private final DependencyResolver dependencies;

    public StaticIndexProviderMap(DependencyResolver dependencies, Iterable<IndexProvider> indexProviders) {
        this.dependencies = dependencies;
        for (IndexProvider provider : indexProviders) {
            add(provider);
        }
    }

    @Override
    public void init() throws Exception {
        // Add providers loaded by extensions
        dependencies.resolveTypeDependencies(IndexProvider.class).forEach(this::add);
    }

    @Override
    public IndexProvider getTokenIndexProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.LOOKUP, kernelVersion);
    }

    @Override
    public IndexProvider getDefaultProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.RANGE, kernelVersion);
    }

    @Override
    public IndexProvider getPointIndexProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.POINT, kernelVersion);
    }

    @Override
    public IndexProvider getTextIndexProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.TEXT, kernelVersion);
    }

    @Override
    public IndexProvider getFulltextProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.FULLTEXT, kernelVersion);
    }

    @Override
    public IndexProvider getVectorIndexProvider(KernelVersion kernelVersion) {
        return getIndexProvider(IndexType.VECTOR, kernelVersion);
    }

    @Override
    public IndexProvider lookup(IndexProviderDescriptor providerDescriptor) {
        IndexProvider provider = indexProvidersByDescriptor.get(providerDescriptor);
        assertProviderFound(provider, providerDescriptor.name());
        return provider;
    }

    @Override
    public IndexProvider lookup(String providerDescriptorName) {
        IndexProvider provider = indexProvidersByName.get(providerDescriptorName);
        assertProviderFound(provider, providerDescriptorName);
        return provider;
    }

    @Override
    public List<IndexProvider> lookup(IndexType indexType) {
        List<IndexProvider> indexProviders = indexProvidersByType.get(indexType);
        assertProviderFoundByType(indexProviders, indexType);
        return indexProviders;
    }

    @Override
    public void accept(Consumer<IndexProvider> visitor) {
        indexProvidersByDescriptor.values().forEach(visitor);
    }

    private IndexProvider getIndexProvider(IndexType indexType, KernelVersion kernelVersion) {
        List<IndexProvider> providers = indexProvidersByType.get(indexType);
        for (IndexProvider provider : providers) {
            if (kernelVersion.isAtLeast(provider.getMinimumRequiredVersion())) {
                return provider;
            }
        }
        throw new IndexProviderNotFoundException(
                "No provider for type " + indexType + " and version " + kernelVersion + " found.");
    }

    private void assertProviderFound(IndexProvider provider, String providerDescriptorName) {
        if (provider == null) {
            throw new IndexProviderNotFoundException(
                    "Tried to get index provider with name %s whereas available providers in this session being %s."
                            .formatted(providerDescriptorName, indexProvidersByName.keySet()));
        }
    }

    private void assertProviderFoundByType(List<IndexProvider> indexProviders, IndexType indexType) {
        if (indexProviders != null) {
            return;
        }

        List<String> providerNamesByType = new ArrayList<>();
        for (Entry<IndexType, List<IndexProvider>> entry : indexProvidersByType.entrySet()) {
            StringJoiner joiner = new StringJoiner(", ", "[", "]");
            for (IndexProvider provider : entry.getValue()) {
                joiner.add(provider.getProviderDescriptor().name());
            }
            String providerByType = entry.getKey() + "=" + joiner;
            providerNamesByType.add(providerByType);
        }
        throw new IndexProviderNotFoundException("Tried to get index providers for index type " + indexType
                + " but could not find any. Available index providers per type are " + providerNamesByType);
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        IndexProviderDescriptor providerDescriptor = index.getIndexProvider();
        IndexProvider provider = lookup(providerDescriptor);
        return provider.completeConfiguration(index, indexingBehaviour);
    }

    private void add(IndexProvider provider) {
        if (provider == null) {
            return;
        }

        IndexProviderDescriptor providerDescriptor = requireNonNull(provider.getProviderDescriptor());
        IndexProvider existing = indexProvidersByDescriptor.putIfAbsent(providerDescriptor, provider);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Tried to load multiple schema index providers with the same provider descriptor "
                            + providerDescriptor + ". First loaded " + existing + " then " + provider);
        }

        indexProvidersByName.putIfAbsent(providerDescriptor.name(), provider);
        List<IndexProvider> providersForType =
                indexProvidersByType.computeIfAbsent(provider.getIndexType(), it -> new ArrayList<>());
        providersForType.add(provider);
        providersForType.sort(DESCENDING_MINIMUM_KERNEL_VERSION);
    }
}
