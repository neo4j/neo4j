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

import java.util.Collection;
import java.util.List;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.Value;

public interface IndexingProvidersService {
    /**
     * Get the index provider descriptor for the index provider with the given name, or the
     * descriptor of the default index provider, if no name was given.
     *
     * @param providerName name of the wanted index provider
     */
    IndexProviderDescriptor indexProviderByName(String providerName);

    /**
     * Get the index type for the index provider with the given name.
     *
     * @param providerName name of the index provider to get matching index type for
     */
    IndexType indexTypeByProviderName(String providerName);

    /**
     * Get all the index providers that has support for the given {@link IndexType}.
     *
     * @param indexType {@link IndexType} to find index providers for
     * @return List of all {@link IndexProviderDescriptor} that describe index provider with support for the given
     * index type.
     */
    List<IndexProviderDescriptor> indexProvidersByType(IndexType indexType);

    /**
     * Validate that the given value tuple can be stored in the index associated with the given schema.
     * @param index the target index
     * @param tuple value tuple to validate
     * @param entityId the id of the entity being validated
     */
    void validateBeforeCommit(IndexDescriptor index, Value[] tuple, long entityId);

    /**
     * Validate the given index prototype, or throw an {@link IllegalArgumentException}.
     *
     * @param prototype The prototype to the validated.
     * @return
     */
    IndexPrototype validateIndexPrototype(IndexPrototype prototype);

    /**
     * See {@link IndexConfigCompleter#completeConfiguration(IndexDescriptor, StorageEngineIndexingBehaviour)}
     */
    IndexDescriptor completeConfiguration(IndexDescriptor index);

    /**
     * There's always a default {@link IndexProvider}, this method returns it.
     *
     * @return the default index provider for this instance.
     */
    IndexProviderDescriptor getDefaultProvider();

    /**
     * The preferred {@link IndexProvider} for handling full-text indexes.
     *
     * @return the default or preferred index provider for full-text indexes.
     */
    IndexProviderDescriptor getFulltextProvider();

    /**
     * @return the token index provider for this instance.
     */
    IndexProviderDescriptor getTokenIndexProvider();

    /**
     * @return the text index provider for this instance.
     */
    IndexProviderDescriptor getTextIndexProvider();

    /**
     * @return the point index provider for this instance.
     */
    IndexProviderDescriptor getPointIndexProvider();

    /**
     * @return the vector index provider instance.
     */
    IndexProviderDescriptor getVectorIndexProvider();

    /**
     * @return registered index provider with descriptor.
     */
    IndexProvider getIndexProvider(IndexProviderDescriptor descriptor);

    /**
     * @return all registered index providers
     */
    Collection<IndexProvider> getIndexProviders();
}
