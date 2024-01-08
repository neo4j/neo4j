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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProvider;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.impl.index.schema.PointIndexProvider;
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider;
import org.neo4j.kernel.impl.index.schema.TokenIndexProvider;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class StaticIndexProviderMap extends LifecycleAdapter implements IndexProviderMap {
    private final Map<IndexProviderDescriptor, IndexProvider> indexProvidersByDescriptor = new HashMap<>();
    private final Map<String, IndexProvider> indexProvidersByName = new HashMap<>();
    private final Map<IndexType, List<IndexProvider>> indexProvidersByType = new HashMap<>();
    private final IndexProvider tokenIndexProvider;
    private final IndexProvider rangeIndexProvider;
    private final IndexProvider pointIndexProvider;
    private final IndexProvider textIndexProvider;
    private final IndexProvider trigramIndexProvider;
    private final IndexProvider fulltextIndexProvider;
    private final IndexProvider vectorIndexProvider;
    private final DependencyResolver dependencies;

    public StaticIndexProviderMap(
            TokenIndexProvider tokenIndexProvider,
            RangeIndexProvider rangeIndexProvider,
            PointIndexProvider pointIndexProvider,
            TextIndexProvider textIndexProvider,
            TrigramIndexProvider trigramIndexProvider,
            FulltextIndexProvider fulltextIndexProvider,
            VectorIndexProvider vectorIndexProvider,
            DependencyResolver dependencies) {
        this.tokenIndexProvider = tokenIndexProvider;
        this.rangeIndexProvider = rangeIndexProvider;
        this.pointIndexProvider = pointIndexProvider;
        this.textIndexProvider = textIndexProvider;
        this.trigramIndexProvider = trigramIndexProvider;
        this.fulltextIndexProvider = fulltextIndexProvider;
        this.vectorIndexProvider = vectorIndexProvider;
        this.dependencies = dependencies;
    }

    @Override
    public void init() throws Exception {
        add(
                tokenIndexProvider,
                rangeIndexProvider,
                pointIndexProvider,
                textIndexProvider,
                trigramIndexProvider,
                fulltextIndexProvider,
                vectorIndexProvider);
        dependencies.resolveTypeDependencies(IndexProvider.class).forEach(this::add);
    }

    @Override
    public IndexProvider getTokenIndexProvider() {
        return tokenIndexProvider;
    }

    @Override
    public IndexProvider getDefaultProvider() {
        return rangeIndexProvider;
    }

    @Override
    public IndexProvider getPointIndexProvider() {
        return pointIndexProvider;
    }

    @Override
    public IndexProvider getTextIndexProvider() {
        return trigramIndexProvider;
    }

    @Override
    public IndexProvider getFulltextProvider() {
        return fulltextIndexProvider;
    }

    @Override
    public IndexProvider getVectorIndexProvider() {
        return vectorIndexProvider;
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
        var indexProviders = indexProvidersByType.get(indexType);
        assertProviderFoundByType(indexProviders, indexType);
        return indexProviders;
    }

    @Override
    public void accept(Consumer<IndexProvider> visitor) {
        indexProvidersByDescriptor.values().forEach(visitor);
    }

    private void assertProviderFound(IndexProvider provider, String providerDescriptorName) {
        if (provider == null) {
            throw new IndexProviderNotFoundException("Tried to get index provider with name " + providerDescriptorName
                    + " whereas available providers in this session being "
                    + indexProvidersByName.keySet() + ", and default being "
                    + rangeIndexProvider.getProviderDescriptor().name());
        }
    }

    private void assertProviderFoundByType(List<IndexProvider> indexProviders, IndexType indexType) {
        if (indexProviders == null) {
            var providerNamesByType = indexProvidersByType.entrySet().stream()
                    .map(entry -> {
                        var type = entry.getKey();
                        var providers = entry.getValue();
                        return "" + type + "="
                                + providers.stream()
                                        .map(provider ->
                                                provider.getProviderDescriptor().name())
                                        .toList();
                    })
                    .toList();
            throw new IndexProviderNotFoundException("Tried to get index providers for index type " + indexType
                    + " but could not find any. Available index providers per type are " + providerNamesByType);
        }
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

        var providerDescriptor = requireNonNull(provider.getProviderDescriptor());
        var existing = indexProvidersByDescriptor.putIfAbsent(providerDescriptor, provider);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Tried to load multiple schema index providers with the same provider descriptor "
                            + providerDescriptor + ". First loaded " + existing + " then " + provider);
        }
        indexProvidersByName.putIfAbsent(providerDescriptor.name(), provider);
        indexProvidersByType
                .computeIfAbsent(provider.getIndexType(), it -> new ArrayList<>())
                .add(provider);
    }

    private void add(IndexProvider... providers) {
        for (var provider : providers) {
            add(provider);
        }
    }
}
