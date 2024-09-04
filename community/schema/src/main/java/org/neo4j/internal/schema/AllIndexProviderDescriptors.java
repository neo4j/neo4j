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
package org.neo4j.internal.schema;

import java.util.Map;
import java.util.Optional;

public interface AllIndexProviderDescriptors {
    /**
     * Indicate that {@link IndexProviderDescriptor} has not yet been decided.
     * Specifically before transaction that create a new index has committed.
     */
    IndexProviderDescriptor UNDECIDED = new IndexProviderDescriptor("Undecided", "0");

    IndexProviderDescriptor TOKEN_DESCRIPTOR = new IndexProviderDescriptor("token-lookup", "1.0");

    IndexProviderDescriptor POINT_DESCRIPTOR = new IndexProviderDescriptor("point", "1.0");

    IndexProviderDescriptor RANGE_DESCRIPTOR = new IndexProviderDescriptor("range", "1.0");

    IndexProviderDescriptor FULLTEXT_DESCRIPTOR = new IndexProviderDescriptor("fulltext", "1.0");

    IndexProviderDescriptor TEXT_V1_DESCRIPTOR = new IndexProviderDescriptor("text", "1.0");
    IndexProviderDescriptor TEXT_V2_DESCRIPTOR = new IndexProviderDescriptor("text", "2.0");
    IndexProviderDescriptor DEFAULT_TEXT_DESCRIPTOR = TEXT_V2_DESCRIPTOR;

    IndexProviderDescriptor VECTOR_V1_DESCRIPTOR = new IndexProviderDescriptor("vector", "1.0");
    IndexProviderDescriptor VECTOR_V2_DESCRIPTOR = new IndexProviderDescriptor("vector", "2.0");
    IndexProviderDescriptor DEFAULT_VECTOR_DESCRIPTOR = VECTOR_V2_DESCRIPTOR;

    /**
     * Mapping of {@link IndexProviderDescriptor} to the {@link IndexType}s they describe.
     */
    Map<IndexProviderDescriptor, IndexType> INDEX_TYPES = Map.of(
            FULLTEXT_DESCRIPTOR, IndexType.FULLTEXT,
            TOKEN_DESCRIPTOR, IndexType.LOOKUP,
            TEXT_V1_DESCRIPTOR, IndexType.TEXT,
            TEXT_V2_DESCRIPTOR, IndexType.TEXT,
            RANGE_DESCRIPTOR, IndexType.RANGE,
            POINT_DESCRIPTOR, IndexType.POINT,
            VECTOR_V1_DESCRIPTOR, IndexType.VECTOR,
            VECTOR_V2_DESCRIPTOR, IndexType.VECTOR);

    /**
     * @param providerName the name of the provider to find
     * @return the pairing of {@link IndexProviderDescriptor} to {@link IndexType} for the provided name
     */
    static Optional<ProviderDescriptorDetails> providerDescriptorDetails(String providerName) {
        for (var entry : INDEX_TYPES.entrySet()) {
            if (entry.getKey().name().equals(providerName)) {
                return Optional.of(new ProviderDescriptorDetails(entry.getKey(), entry.getValue()));
            }
        }

        return Optional.empty();
    }

    /**
     * Record of the {@link IndexProviderDescriptor} to the {@link IndexType} they describe.
     * @param descriptor the index descriptor
     * @param type the index type
     */
    record ProviderDescriptorDetails(IndexProviderDescriptor descriptor, IndexType type) {}
}
