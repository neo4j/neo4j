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

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

abstract class SimpleIndexPopulatorCompatibility extends PropertyIndexProviderCompatibilityTestSuite.Compatibility {
    SimpleIndexPopulatorCompatibility(PropertyIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
        super(testSuite, prototype);
    }

    final IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(Config.defaults());

    @Test
    void shouldStorePopulationFailedForRetrievalFromProviderLater() throws Exception {
        // GIVEN
        String failure = "The contrived failure";
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        // WHEN (this will attempt to call close)
        withPopulator(
                indexProvider.getPopulator(
                        descriptor,
                        indexSamplingConfig,
                        heapBufferFactory(1024),
                        INSTANCE,
                        tokenNameLookup,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY),
                p -> p.markAsFailed(failure),
                false);
        // THEN
        assertThat(indexProvider.getPopulationFailure(descriptor, CursorContext.NULL_CONTEXT, Sets.immutable.empty()))
                .contains(failure);
    }

    @Test
    void shouldReportInitialStateAsFailedIfPopulationFailed() throws Exception {
        // GIVEN
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
                    String failure = "The contrived failure";

                    // WHEN
                    p.markAsFailed(failure);
                    p.close(false, CursorContext.NULL_CONTEXT);

                    // THEN
                    assertEquals(
                            FAILED,
                            indexProvider.getInitialState(
                                    descriptor, CursorContext.NULL_CONTEXT, Sets.immutable.empty()));
                },
                false);
    }

    @Test
    void shouldBeAbleToDropAClosedIndexPopulator() {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        final IndexPopulator p = indexProvider.getPopulator(
                descriptor,
                indexSamplingConfig,
                heapBufferFactory(1024),
                INSTANCE,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY);
        p.close(false, CursorContext.NULL_CONTEXT);

        // WHEN
        p.drop();

        // THEN - no exception should be thrown (it's been known to!)
    }

    @Test
    void shouldApplyUpdatesIdempotently() throws Exception {
        // GIVEN
        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig(config);
        final Value propertyValue = Values.of("value1");
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
                    long nodeId = 1;

                    // update using populator...
                    var update = add(nodeId, descriptor, propertyValue);
                    p.add(singletonList(update), CursorContext.NULL_CONTEXT);
                    // ...is the same as update using updater
                    try (IndexUpdater updater = p.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                        updater.process(update);
                    }
                });

        // THEN
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                indexSamplingConfig,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING);
                    NodeValueIterator nodes = new NodeValueIterator()) {
                int propertyKeyId = descriptor.schema().getPropertyId();
                reader.query(
                        nodes,
                        QueryContext.NULL_CONTEXT,
                        unconstrained(),
                        PropertyIndexQuery.exact(propertyKeyId, propertyValue));
                assertEquals(asSet(1L), PrimitiveLongCollections.toSet(nodes));
            }
        }
    }

    @Test
    void shouldPopulateWithAllValues() throws Exception {
        // GIVEN
        withPopulator(
                indexProvider.getPopulator(
                        descriptor,
                        indexSamplingConfig,
                        heapBufferFactory(1024),
                        INSTANCE,
                        tokenNameLookup,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY),
                p -> p.add(updates(valueSet1), CursorContext.NULL_CONTEXT));

        // THEN
        assertHasAllValues(valueSet1);
    }

    @Test
    void shouldUpdateWithAllValuesDuringPopulation() throws Exception {
        // GIVEN
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
                    try (IndexUpdater updater = p.newPopulatingUpdater(CursorContext.NULL_CONTEXT)) {
                        for (NodeAndValue entry : valueSet1) {
                            updater.process(add(entry.nodeId, descriptor, entry.value));
                        }
                    }
                });

        // THEN
        assertHasAllValues(valueSet1);
    }

    @Test
    void shouldPopulateAndUpdate() throws Exception {
        // GIVEN
        withPopulator(
                indexProvider.getPopulator(
                        descriptor,
                        indexSamplingConfig,
                        heapBufferFactory(1024),
                        INSTANCE,
                        tokenNameLookup,
                        Sets.immutable.empty(),
                        StorageEngineIndexingBehaviour.EMPTY),
                p -> p.add(updates(valueSet1), CursorContext.NULL_CONTEXT));

        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                indexSamplingConfig,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // WHEN
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                List<ValueIndexEntryUpdate<?>> updates = updates(valueSet2);
                for (ValueIndexEntryUpdate<?> update : updates) {
                    updater.process(update);
                }
            }

            // THEN
            try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
                int propertyKeyId = descriptor.schema().getPropertyId();
                for (NodeAndValue entry : Iterables.concat(valueSet1, valueSet2)) {
                    try (NodeValueIterator nodes = new NodeValueIterator()) {
                        reader.query(
                                nodes,
                                QueryContext.NULL_CONTEXT,
                                unconstrained(),
                                PropertyIndexQuery.exact(propertyKeyId, entry.value));
                        assertEquals(entry.nodeId, nodes.next());
                        assertFalse(nodes.hasNext());
                    }
                }
            }
        }
    }

    /**
     * This test target a bug around minimal splitter in gbpTree and unique index populator. It goes like this:
     * Given a set of updates (value,entityId):
     * - ("A01",1), ("A90",3), ("A9",2)
     * If ("A01",1) and ("A90",3) would cause a split to occur they would produce a minimal splitter ("A9",3).
     * Note that the value in this minimal splitter is equal to our last update ("A9",2).
     * When making insertions with the unique populator we don't compare entityId which would means ("A9",2)
     * ends up to the right of ("A9",3), even though it belongs to the left because of entityId being smaller.
     * At this point the tree is in an inconsistent (key on wrong side of splitter).
     *
     * To work around this problem the entityId is only kept in minimal splitter if strictly necessary to divide
     * left from right. This means the minimal splitter between ("A01",1) and ("A90",3) is ("A9",-1) and ("A9",2)
     * will correctly be placed on the right side of this splitter.
     *
     * To trigger this scenario this test first insert a bunch of values that are all unique and that will cause a
     * split to happen. This is the firstBatch.
     * The second batch are constructed so that at least one of them will have a value equal to the splitter key
     * constructed during the firstBatch.
     * It's important that the secondBatch has ids that are lower than the first batch to align with example described above.
     */
    @Test
    void shouldPopulateAndRemoveEntriesWithSimilarMinimalSplitter() throws Exception {
        String prefix = "Work out your own salvation. Do not depend on others. ";
        int nbrOfNodes = 200;
        long nodeId = 0;

        // Second batch has lower ids
        List<NodeAndValue> secondBatch = new ArrayList<>();
        for (int i = 0; i < nbrOfNodes; i++) {
            secondBatch.add(new NodeAndValue(nodeId++, stringValue(prefix + i)));
        }

        // First batch has higher ids and minimal splitter among values in first batch will be found among second batch
        List<NodeAndValue> firstBatch = new ArrayList<>();
        for (int i = 0; i < nbrOfNodes; i++) {
            firstBatch.add(new NodeAndValue(nodeId++, stringValue(prefix + i + " " + i)));
        }

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
                    p.add(updates(firstBatch), CursorContext.NULL_CONTEXT);
                    p.add(updates(secondBatch), CursorContext.NULL_CONTEXT);

                    // Index should be consistent
                });

        List<NodeAndValue> toRemove = new ArrayList<>();
        toRemove.addAll(firstBatch);
        toRemove.addAll(secondBatch);
        Collections.shuffle(toRemove);

        // And we should be able to remove the entries in any order
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                indexSamplingConfig,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            // WHEN
            try (IndexUpdater updater =
                    accessor.newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                for (NodeAndValue nodeAndValue : toRemove) {
                    updater.process(IndexEntryUpdate.remove(nodeAndValue.nodeId, descriptor, nodeAndValue.value));
                }
            }

            // THEN
            try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
                int propertyKeyId = descriptor.schema().getPropertyId();
                for (NodeAndValue nodeAndValue : toRemove) {
                    NodeValueIterator nodes = new NodeValueIterator();
                    reader.query(
                            nodes,
                            QueryContext.NULL_CONTEXT,
                            unconstrained(),
                            PropertyIndexQuery.exact(propertyKeyId, nodeAndValue.value));
                    boolean anyHits = false;

                    StringJoiner nodesStillLeft = new StringJoiner(", ", "[", "]");
                    while (nodes.hasNext()) {
                        anyHits = true;
                        nodesStillLeft.add(Long.toString(nodes.next()));
                    }
                    assertFalse(anyHits, "Expected this query to have zero hits but found " + nodesStillLeft);
                }
            }
        }
    }

    private void assertHasAllValues(List<NodeAndValue> values) throws IOException, IndexNotApplicableKernelException {
        try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                descriptor,
                indexSamplingConfig,
                tokenNameLookup,
                Sets.immutable.empty(),
                StorageEngineIndexingBehaviour.EMPTY)) {
            try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
                int propertyKeyId = descriptor.schema().getPropertyId();
                for (NodeAndValue entry : values) {
                    try (NodeValueIterator nodes = new NodeValueIterator()) {
                        reader.query(
                                nodes,
                                QueryContext.NULL_CONTEXT,
                                unconstrained(),
                                PropertyIndexQuery.exact(propertyKeyId, entry.value));
                        assertEquals(entry.nodeId, nodes.next());
                        assertFalse(nodes.hasNext());
                    }
                }
            }
        }
    }

    abstract static class General extends SimpleIndexPopulatorCompatibility {
        General(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, testSuite.indexPrototype());
        }

        @Test
        void shouldProvidePopulatorThatAcceptsDuplicateEntries() throws Exception {
            // when
            long offset = valueSet1.size();
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
                        p.add(updates(valueSet1, 0), CursorContext.NULL_CONTEXT);
                        p.add(updates(valueSet1, offset), CursorContext.NULL_CONTEXT);
                    });

            // then
            try (IndexAccessor accessor = indexProvider.getOnlineAccessor(
                    descriptor,
                    indexSamplingConfig,
                    tokenNameLookup,
                    Sets.immutable.empty(),
                    StorageEngineIndexingBehaviour.EMPTY)) {
                try (ValueIndexReader reader = accessor.newValueReader(NO_USAGE_TRACKING)) {
                    int propertyKeyId = descriptor.schema().getPropertyId();
                    for (NodeAndValue entry : valueSet1) {
                        try (NodeValueIterator nodes = new NodeValueIterator()) {
                            reader.query(
                                    nodes,
                                    QueryContext.NULL_CONTEXT,
                                    unconstrained(),
                                    PropertyIndexQuery.exact(propertyKeyId, entry.value));
                            assertEquals(
                                    asSet(entry.nodeId, entry.nodeId + offset),
                                    PrimitiveLongCollections.toSet(nodes),
                                    entry.value.toString());
                        }
                    }
                }
            }
        }
    }

    abstract static class Unique extends SimpleIndexPopulatorCompatibility {
        Unique(PropertyIndexProviderCompatibilityTestSuite testSuite) {
            super(testSuite, testSuite.uniqueIndexPrototype());
        }

        /**
         * This is also checked by the UniqueConstraintCompatibility test, only not on this abstraction level.
         */
        @Test
        void shouldProvidePopulatorThatEnforcesUniqueConstraints() throws Exception {
            // when
            Value value = Values.of("value1");
            int nodeId1 = 1;
            int nodeId2 = 2;

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
                                    Arrays.asList(add(nodeId1, descriptor, value), add(nodeId2, descriptor, value)),
                                    CursorContext.NULL_CONTEXT);
                            p.scanCompleted(
                                    PhaseTracker.nullInstance, populationWorkScheduler, CursorContext.NULL_CONTEXT);

                            fail("expected exception");
                        }
                        // then
                        catch (Exception e) {
                            Throwable root = getRootCause(e);
                            if (root instanceof IndexEntryConflictException conflict) {
                                assertEquals(nodeId1, conflict.getExistingEntityId());
                                assertEquals(ValueTuple.of(value), conflict.getPropertyValues());
                                assertEquals(nodeId2, conflict.getAddedEntityId());
                            } else {
                                throw e;
                            }
                        }
                    },
                    false);
        }
    }
}
