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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;

import java.util.Arrays;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

abstract class CompositeIndexPopulatorCompatibility extends PropertyIndexProviderCompatibilityTestSuite.Compatibility {
    CompositeIndexPopulatorCompatibility(
            PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    abstract static class General extends CompositeIndexPopulatorCompatibility {
        General(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, IndexPrototype.forSchema(forLabel(1000, 100, 200)));
        }

        @Test
        void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
            withPopulator(
                    indexProvider.getPopulator(
                            descriptor,
                            indexSamplingConfig,
                            heapBufferFactory(1024),
                            INSTANCE,
                            tokenNameLookup,
                            Sets.immutable.empty(),
                            StorageEngineIndexingBehaviour.EMPTY),
                    p -> p.add(
                            Arrays.asList(
                                    add(1, descriptor, Values.of("v1"), Values.of("v2")),
                                    add(2, descriptor, Values.of("v1"), Values.of("v2"))),
                            CursorContext.NULL_CONTEXT));

            // then
            try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                    descriptor,
                    indexSamplingConfig,
                    tokenNameLookup,
                    Sets.immutable.empty(),
                    StorageEngineIndexingBehaviour.EMPTY)) {
                try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);
                        NodeValueIterator nodes = new NodeValueIterator()) {
                    reader.query(
                            nodes,
                            QueryContext.NULL_CONTEXT,
                            unconstrained(),
                            PropertyIndexQuery.exact(1, "v1"),
                            PropertyIndexQuery.exact(1, "v2"));
                    assertEquals(asSet(1L, 2L), PrimitiveLongCollections.toSet(nodes));
                }
            }
        }
    }

    abstract static class Unique extends CompositeIndexPopulatorCompatibility {
        Value value1 = Values.of("value1");
        Value value2 = Values.of("value2");
        Value value3 = Values.of("value3");
        int nodeId1 = 3;
        int nodeId2 = 4;

        Unique(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, IndexPrototype.uniqueForSchema(forLabel(1000, 100, 200)));
        }

        @Test
        void shouldEnforceUniqueConstraintsDirectly() throws Exception {
            // when
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
            withPopulator(
                    indexProvider.getPopulator(
                            descriptor,
                            indexSamplingConfig,
                            heapBufferFactory(1024),
                            INSTANCE,
                            tokenNameLookup,
                            Sets.immutable.empty(),
                            StorageEngineIndexingBehaviour.EMPTY),
                    p -> {
                        try {
                            p.add(
                                    Arrays.asList(
                                            add(nodeId1, descriptor, value1, value2),
                                            add(nodeId2, descriptor, value1, value2)),
                                    CursorContext.NULL_CONTEXT);
                            p.scanCompleted(
                                    PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);

                            fail("expected exception");
                        }
                        // then
                        catch (IndexEntryConflictException conflict) {
                            assertEquals(nodeId1, conflict.getExistingEntityId());
                            assertEquals(ValueTuple.of(value1, value2), conflict.getPropertyValues());
                            assertEquals(nodeId2, conflict.getAddedEntityId());
                        }
                    },
                    false);
        }

        @Test
        void shouldNotRestrictUpdatesDifferingOnSecondProperty() throws Exception {
            // given
            IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
            withPopulator(
                    indexProvider.getPopulator(
                            descriptor,
                            indexSamplingConfig,
                            heapBufferFactory(1024),
                            INSTANCE,
                            tokenNameLookup,
                            Sets.immutable.empty(),
                            StorageEngineIndexingBehaviour.EMPTY),
                    p -> {
                        p.add(
                                Arrays.asList(
                                        add(nodeId1, descriptor, value1, value2),
                                        add(nodeId2, descriptor, value1, value3)),
                                CursorContext.NULL_CONTEXT);
                        p.scanCompleted(PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);
                    });
        }
    }
}
