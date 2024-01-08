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
package org.neo4j.kernel.impl.api.index;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class MockIndexProviderMap extends LifecycleAdapter implements IndexProviderMap {
    private final IndexProvider indexProvider;

    public MockIndexProviderMap(IndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    @Override
    public void init() {}

    @Override
    public IndexProvider getTokenIndexProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexProvider getDefaultProvider() {
        return indexProvider;
    }

    @Override
    public IndexProvider getPointIndexProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexProvider getTextIndexProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexProvider getFulltextProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexProvider getVectorIndexProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexProvider lookup(IndexProviderDescriptor providerDescriptor) {
        if (indexProvider.getProviderDescriptor().equals(providerDescriptor)) {
            return indexProvider;
        }
        throw new IndexProviderNotFoundException("lookup by descriptor failed");
    }

    @Override
    public IndexProvider lookup(String providerDescriptorName) {
        if (indexProvider.getProviderDescriptor().name().equals(providerDescriptorName)) {
            return indexProvider;
        }
        throw new IndexProviderNotFoundException("lookup by descriptor name failed");
    }

    @Override
    public List<IndexProvider> lookup(IndexType indexType) {
        if (indexProvider.getIndexType().equals(indexType)) {
            return Collections.singletonList(indexProvider);
        }
        throw new IndexProviderNotFoundException("lookup by type failed");
    }

    @Override
    public void accept(Consumer<IndexProvider> visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexDescriptor completeConfiguration(
            IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
        IndexProviderDescriptor providerDescriptor = index.getIndexProvider();
        IndexProvider provider = lookup(providerDescriptor);
        return provider.completeConfiguration(index, indexingBehaviour);
    }
}
