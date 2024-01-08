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
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.index.IndexProvider;

/**
 * Contains mapping from {@link IndexProviderDescriptor} or provider name to {@link IndexProvider}.
 */
public interface IndexProviderMap extends IndexConfigCompleter {
    /**
     * Looks up and returns the {@link IndexProvider} for the given {@link IndexProviderDescriptor}.
     *
     * @param providerDescriptor the descriptor identifying the {@link IndexProvider}.
     * @return the {@link IndexProvider} with the given {@link IndexProviderDescriptor}.
     * @throws IndexProviderNotFoundException if no such {@link IndexProvider} was found.
     */
    IndexProvider lookup(IndexProviderDescriptor providerDescriptor) throws IndexProviderNotFoundException;

    /**
     * Looks up and returns the {@link IndexProvider} for the given index provider name. The name is what
     * an {@link IndexProviderDescriptor#name()} call would return.
     *
     * @param providerDescriptorName the descriptor name identifying the {@link IndexProvider}.
     * @return the {@link IndexProvider} with the given name.
     * @throws IndexProviderNotFoundException if no such {@link IndexProvider} was found.
     */
    IndexProvider lookup(String providerDescriptorName) throws IndexProviderNotFoundException;

    /**
     * Looks up all {@link IndexProvider} with support for the given {@link IndexType}.
     *
     * @param indexType {@link IndexType} for which to find index providers.
     * @return A list of all {@link IndexProvider index providers} with support for the given type.
     */
    List<IndexProvider> lookup(IndexType indexType);

    /**
     * There's always a token index provider, this method returns it.
     *
     * @return token index provider for this instance
     */
    IndexProvider getTokenIndexProvider();

    /**
     * There's always a default {@link IndexProvider}, this method returns it.
     *
     * @return the default index provider for this instance.
     */
    IndexProvider getDefaultProvider();

    /**
     * The preferred {@link IndexProvider} for handling point indexes.
     */
    IndexProvider getPointIndexProvider();

    /**
     * The preferred {@link IndexProvider} for handling text indexes.
     *
     * @return the default or preferred index provider for full-text indexes.
     */
    IndexProvider getTextIndexProvider();

    /**
     * The preferred {@link IndexProvider} for handling full-text indexes.
     *
     * @return the default or preferred index provider for full-text indexes.
     */
    IndexProvider getFulltextProvider();

    /**
     * The preferred {@link IndexProvider} for handling vector indexes.
     */
    IndexProvider getVectorIndexProvider();

    /**
     * Visits all the {@link IndexProvider} with the visitor.
     *
     * @param visitor {@link Consumer} visiting all the {@link IndexProvider index providers} in this map.
     */
    void accept(Consumer<IndexProvider> visitor);

    IndexProviderMap EMPTY = new IndexProviderMap() {
        @Override
        public IndexDescriptor completeConfiguration(
                IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
            return index;
        }

        @Override
        public IndexProvider lookup(IndexProviderDescriptor descriptor) throws IndexProviderNotFoundException {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider lookup(String providerDescriptorName) throws IndexProviderNotFoundException {
            return IndexProvider.EMPTY;
        }

        @Override
        public List<IndexProvider> lookup(IndexType indexType) {
            return Collections.singletonList(IndexProvider.EMPTY);
        }

        @Override
        public IndexProvider getTokenIndexProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider getDefaultProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider getPointIndexProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider getTextIndexProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider getFulltextProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public IndexProvider getVectorIndexProvider() {
            return IndexProvider.EMPTY;
        }

        @Override
        public void accept(Consumer<IndexProvider> visitor) {
            // yey!
        }
    };
}
