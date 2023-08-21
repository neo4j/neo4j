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
package org.neo4j.kernel.api.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

abstract class IndexConfigurationCompletionCompatibility extends IndexProviderCompatabilityTestBase {
    IndexConfigurationCompletionCompatibility(IndexProviderCompatibilityTestSuite testSuite) {
        super(testSuite, testSuite.indexPrototype());
    }

    @Test
    void configurationCompletionMustNotOverwriteExistingConfiguration() {
        IndexDescriptor index = descriptor;
        index = index.withIndexConfig(IndexConfig.with("Bob", Values.stringValue("Howard")));
        index = indexProvider.completeConfiguration(index, storageEngineIndexingBehaviour);
        assertEquals(index.getIndexConfig().get("Bob"), Values.stringValue("Howard"));
    }

    @Test
    void configurationCompletionMustBeIdempotent() {
        IndexDescriptor index = descriptor;
        IndexDescriptor onceCompleted = indexProvider.completeConfiguration(index, storageEngineIndexingBehaviour);
        IndexDescriptor twiceCompleted =
                indexProvider.completeConfiguration(onceCompleted, storageEngineIndexingBehaviour);
        assertEquals(onceCompleted.getIndexConfig(), twiceCompleted.getIndexConfig());
    }

    @Test
    void mustAssignCapabilitiesToDescriptorsThatHaveNone() {
        IndexDescriptor index = descriptor;
        IndexDescriptor completed = indexProvider.completeConfiguration(index, storageEngineIndexingBehaviour);
        assertNotEquals(completed.getCapability(), IndexCapability.NO_CAPABILITY);
        completed = completed.withIndexCapability(IndexCapability.NO_CAPABILITY);
        completed = indexProvider.completeConfiguration(completed, storageEngineIndexingBehaviour);
        assertNotEquals(completed.getCapability(), IndexCapability.NO_CAPABILITY);
    }

    @Test
    void mustNotOverwriteExistingCapabilities() {
        IndexCapability capability = new IndexCapability() {
            @Override
            public boolean supportsOrdering() {
                return false;
            }

            @Override
            public boolean supportsReturningValues() {
                return false;
            }

            @Override
            public boolean areValueCategoriesAccepted(ValueCategory... valueCategories) {
                Preconditions.requireNonEmpty(valueCategories);
                Preconditions.requireNoNullElements(valueCategories);
                return true;
            }

            @Override
            public boolean isQuerySupported(IndexQueryType queryType, ValueCategory valueCategory) {
                return true;
            }

            @Override
            public double getCostMultiplier(IndexQueryType... queryTypes) {
                return 1.0;
            }

            @Override
            public boolean supportPartitionedScan(IndexQuery... queries) {
                Preconditions.requireNonEmpty(queries);
                Preconditions.requireNoNullElements(queries);
                return false;
            }
        };
        IndexDescriptor index = descriptor.withIndexCapability(capability);
        IndexDescriptor completed = indexProvider.completeConfiguration(index, storageEngineIndexingBehaviour);
        assertSame(capability, completed.getCapability());
    }

    @Test
    void indexProviderMustReturnCorrectIndexType() {
        assertThat(indexProvider.getIndexType()).isEqualTo(testSuite.indexType());
    }
}
