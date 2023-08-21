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
package org.neo4j.kernel.api.schema.index;

import java.util.concurrent.ThreadLocalRandom;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

public class TestIndexDescriptorFactory {
    private TestIndexDescriptorFactory() {}

    public static IndexDescriptor forSchema(SchemaDescriptor schema) {
        return forSchema(randomId(), schema);
    }

    public static IndexDescriptor forSchema(long id, SchemaDescriptor schema) {
        return forSchema(id, IndexType.RANGE, schema);
    }

    public static IndexDescriptor forSchema(IndexType indexType, SchemaDescriptor schema) {
        return forSchema(randomId(), indexType, schema);
    }

    public static IndexDescriptor forSchema(long id, IndexType indexType, SchemaDescriptor schema) {
        final var index = IndexPrototype.forSchema(schema)
                .withIndexType(indexType)
                .withName("index_" + id)
                .materialise(id);
        return TestIndexConfigCompleter.INSTANCE.completeConfiguration(index, StorageEngineIndexingBehaviour.EMPTY);
    }

    public static IndexDescriptor uniqueForSchema(SchemaDescriptor schema) {
        return uniqueForSchema(IndexType.RANGE, schema);
    }

    public static IndexDescriptor uniqueForSchema(IndexType indexType, SchemaDescriptor schema) {
        final var id = randomId();
        final var index = IndexPrototype.uniqueForSchema(schema)
                .withIndexType(indexType)
                .withName("index_" + id)
                .materialise(id);
        return TestIndexConfigCompleter.INSTANCE.completeConfiguration(index, StorageEngineIndexingBehaviour.EMPTY);
    }

    public static IndexDescriptor forLabel(int labelId, int... propertyIds) {
        return forSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static IndexDescriptor forLabel(IndexType indexType, int labelId, int... propertyIds) {
        return forSchema(indexType, SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static IndexDescriptor forRelType(int relTypeId, int... propertyIds) {
        return forSchema(SchemaDescriptors.forRelType(relTypeId, propertyIds));
    }

    public static IndexDescriptor uniqueForLabel(int labelId, int... propertyIds) {
        return uniqueForSchema(SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    public static IndexDescriptor uniqueForLabel(IndexType indexType, int labelId, int... propertyIds) {
        return uniqueForSchema(indexType, SchemaDescriptors.forLabel(labelId, propertyIds));
    }

    private static int randomId() {
        return ThreadLocalRandom.current().nextInt(1, 1000);
    }

    private static class TestIndexConfigCompleter implements IndexConfigCompleter {
        public static final TestIndexConfigCompleter INSTANCE = new TestIndexConfigCompleter();

        private static final IndexCapability CAPABILITY = new IndexCapability() {
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
                return COST_MULTIPLIER_STANDARD;
            }

            @Override
            public boolean supportPartitionedScan(IndexQuery... queries) {
                Preconditions.requireNonEmpty(queries);
                Preconditions.requireNoNullElements(queries);
                return false;
            }
        };

        private TestIndexConfigCompleter() {}

        @Override
        public IndexDescriptor completeConfiguration(
                IndexDescriptor index, StorageEngineIndexingBehaviour indexingBehaviour) {
            return index.getCapability().equals(IndexCapability.NO_CAPABILITY)
                    ? index.withIndexCapability(CAPABILITY)
                    : index;
        }
    }
}
