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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.filter;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.MinimalIndexAccessor;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configure")
public class IndexPopulationMissConcurrentUpdateIT {
    private static final String NAME_PROPERTY = "name";
    private static final long INITIAL_CREATION_NODE_ID_THRESHOLD = 30;
    private static final long SCAN_BARRIER_NODE_ID_THRESHOLD = 10;
    private final ControlledSchemaIndexProvider index = new ControlledSchemaIndexProvider();

    @Inject
    private GraphDatabaseService db;

    @Inject
    private IdController idController;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.noOpSystemGraphInitializer().addExtension(index);
        builder.setConfig(GraphDatabaseInternalSettings.index_population_queue_threshold, 1);
    }

    /**
     * Tests an issue where the {@link MultipleIndexPopulator} had a condition when applying external concurrent updates that any given
     * update would only be applied if the entity id was lower than the highest entity id the scan had seen (i.e. where the scan was currently at).
     * This would be a problem because of how the {@link TokenIndexReader} works internally, which is that it reads one bit-set of node ids
     * at the time, effectively caching a small range of ids. If a concurrent creation would happen right in front of where the scan was
     * after it had read and cached that bit-set it would not apply the update and miss that entity in the scan and would end up with an index
     * that was inconsistent with the store.
     */
    @Test
    public void shouldNoticeConcurrentUpdatesWithinCurrentLabelIndexEntryRange() throws Exception {
        // given nodes [0...30]. Why 30, because this test ties into a bug regarding "caching" of bit-sets in label
        // index reader,
        // where each bit-set is of size 64.
        List<Node> nodes = new ArrayList<>();
        int nextId = 0;
        try (Transaction tx = db.beginTx()) {
            Node node;
            do {
                node = tx.createNode(LABEL_ONE);
                node.setProperty(NAME_PROPERTY, "Node " + nextId++);
                nodes.add(node);
            } while (node.getId() < INITIAL_CREATION_NODE_ID_THRESHOLD);
            tx.commit();
        }
        assertThat(count(filter(n -> n.getId() <= SCAN_BARRIER_NODE_ID_THRESHOLD, nodes)))
                .as(
                        "At least one node below the scan barrier threshold must have been created, otherwise test assumptions are invalid or outdated")
                .isGreaterThan(0L);
        assertThat(count(filter(n -> n.getId() > SCAN_BARRIER_NODE_ID_THRESHOLD, nodes)))
                .as(
                        "At least two nodes above the scan barrier threshold and below initial creation threshold must have been created, "
                                + "otherwise test assumptions are invalid or outdated")
                .isGreaterThan(1L);
        idController.maintenance();

        // when
        try (TransactionImpl tx = (TransactionImpl) db.beginTx()) {
            IndexingTestUtil.createNodePropIndexWithSpecifiedProvider(
                    tx, ControlledSchemaIndexProvider.INDEX_PROVIDER, LABEL_ONE, NAME_PROPERTY);
            tx.commit();
        }

        index.barrier.await();
        // Now the index population has come some way into the first bit-set entry of the label index
        try (Transaction tx = db.beginTx()) {
            Node node;
            do {
                node = tx.createNode(LABEL_ONE);
                node.setProperty(NAME_PROPERTY, nextId++);
                nodes.add(node);
            } while (node.getId() < index.populationAtId);
            // here we know that we have created a node in front of the index populator and also inside the cached
            // bit-set of the label index reader
            tx.commit();
        }
        index.barrier.release();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, MINUTES);
            tx.commit();
        }

        // then all nodes must be in the index
        assertEquals(nodes.size(), index.entitiesByScan.size() + index.entitiesByUpdater.size());
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (Node node : allNodes) {
                assertTrue(
                        index.entitiesByScan.contains(node.getId()) || index.entitiesByUpdater.contains(node.getId()));
            }
            tx.commit();
        }
    }

    /**
     * A very specific {@link IndexProvider} which can be paused and continued at juuust the right places.
     */
    private static class ControlledSchemaIndexProvider extends ExtensionFactory<Supplier> {
        private final Barrier.Control barrier = new Barrier.Control();
        private final Set<Long> entitiesByScan = new ConcurrentSkipListSet<>();
        private final Set<Long> entitiesByUpdater = new ConcurrentSkipListSet<>();
        private volatile long populationAtId;
        static final IndexProviderDescriptor INDEX_PROVIDER = new IndexProviderDescriptor("controlled", "1");

        ControlledSchemaIndexProvider() {
            super(ExtensionType.DATABASE, "controlled");
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Supplier noDependencies) {
            return new BaseTestingIndexProvider(INDEX_PROVIDER, directoriesByProvider(Path.of("not-even-persistent"))) {
                @Override
                public MinimalIndexAccessor getMinimalIndexAccessor(
                        IndexDescriptor descriptor, boolean forRebuildDuringRecovery) {
                    return null;
                }

                @Override
                public IndexPopulator getPopulator(
                        IndexDescriptor descriptor,
                        IndexSamplingConfig samplingConfig,
                        ByteBufferFactory bufferFactory,
                        MemoryTracker memoryTracker,
                        TokenNameLookup tokenNameLookup,
                        ImmutableSet<OpenOption> openOptions,
                        StorageEngineIndexingBehaviour indexingBehaviour) {
                    return new IndexPopulator.Adapter() {
                        @Override
                        public void add(
                                Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext) {
                            for (IndexEntryUpdate<?> update : updates) {
                                boolean added = entitiesByScan.add(update.getEntityId());
                                assertTrue(added); // scans should never see multiple updates from the same entityId
                                if (update.getEntityId() > SCAN_BARRIER_NODE_ID_THRESHOLD) {
                                    populationAtId = update.getEntityId();
                                    barrier.reached();
                                }
                            }
                        }

                        @Override
                        public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
                            return new IndexUpdater() {
                                @Override
                                public void process(IndexEntryUpdate<?> update) {
                                    boolean added = entitiesByUpdater.add(update.getEntityId());
                                    assertTrue(
                                            added); // we know that in this test we won't apply multiple updates for an
                                    // entityId
                                }

                                @Override
                                public void close() {}
                            };
                        }

                        @Override
                        public void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
                            assertTrue(populationCompletedSuccessfully);
                        }

                        @Override
                        public void markAsFailed(String failure) {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public IndexAccessor getOnlineAccessor(
                        IndexDescriptor descriptor,
                        IndexSamplingConfig samplingConfig,
                        TokenNameLookup tokenNameLookup,
                        ImmutableSet<OpenOption> openOptions,
                        boolean readOnly,
                        StorageEngineIndexingBehaviour indexingBehaviour) {
                    return mock(IndexAccessor.class);
                }

                @Override
                public InternalIndexState getInitialState(
                        IndexDescriptor descriptor, CursorContext cursorContext, ImmutableSet<OpenOption> openOptions) {
                    return POPULATING;
                }
            };
        }
    }
}
