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
package org.neo4j.storageengine.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.intValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.storageengine.api.EagerValueIndexEntryUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
class IndexUpdatesWorkSyncTest {
    @Inject
    private RandomSupport random;

    private final CursorContextFactory contextFactory = NULL_CONTEXT_FACTORY;

    @RepeatedTest(10)
    void shouldApplyIndexUpdatesSingleThreadedIfToldTo() {
        // ensure context factory creates non-equal contexts
        assertThat(NULL_CONTEXT_FACTORY.create("0")).isNotEqualTo(contextFactory.create("1"));
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("index")
                .materialise(1L);
        int threads = 10;
        Set<UpdateAndContext> appliedUpdates = ConcurrentHashMap.newKeySet();
        AtomicInteger concurrentlyApplyingThreads = new AtomicInteger();
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterator<IndexEntryUpdate> updates, CursorContext cursorContext, boolean parallel) {
                assertThat(concurrentlyApplyingThreads.incrementAndGet()).isOne();
                assertThat(parallel).isFalse();
                while (updates.hasNext()) {
                    IndexEntryUpdate u = updates.next();
                    appliedUpdates.add(new UpdateAndContext(u, cursorContext));
                }
                concurrentlyApplyingThreads.decrementAndGet();
            }
        };

        // when
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(updateListener, false);
        var sentUpdates = queueUpdatesInParallel(index, threads, workSync, contextFactory);

        // then
        assertThat(appliedUpdates).hasSameElementsAs(sentUpdates);
        assertThat(concurrentlyApplyingThreads.get()).isZero();
    }

    @Test
    void shouldApplyIndexUpdatesInParallelIfToldTo() {
        // given
        IndexDescriptor index = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("index")
                .materialise(1L);
        int threads = 4;
        CountDownLatch latch = new CountDownLatch(threads);
        Set<UpdateAndContext> appliedUpdates = ConcurrentHashMap.newKeySet();
        Set<Thread> applyingThreads = ConcurrentHashMap.newKeySet();
        IndexUpdateListener updateListener = new IndexUpdateListener.Adapter() {
            @Override
            public void applyUpdates(
                    Iterator<IndexEntryUpdate> updates, CursorContext cursorContext, boolean parallel) {
                assertThat(parallel).isTrue();
                applyingThreads.add(Thread.currentThread());
                while (updates.hasNext()) {
                    IndexEntryUpdate u = updates.next();
                    appliedUpdates.add(new UpdateAndContext(u, cursorContext));
                }
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        // when
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(updateListener, true);
        var sentUpdates = queueUpdatesInParallel(index, threads, workSync, contextFactory);

        // then
        assertThat(appliedUpdates).hasSameElementsAs(sentUpdates);
        assertThat(applyingThreads).hasSize(threads);
    }

    /**
     * There was this race where a entity creation committing concurrently with index population could result
     * in index population missing the newly created entity entirely.
     * <p>
     * Example of events to trigger this:
     * <ol>
     *     <li>have a node store e.g. [nnnnnnnnnn_n], i.e. 10 used nodes, one deleted and then another used node</li>
     *     <li>let index population (ReadEntityIdsStep) read the first batch of e.g. batchSize=10 nodes</li>
     *     <li>fire a tx that creates a new node (that gets the ID of the deleted node, the hole created for it)</li>
     *     <li>precisely after the index updates for this node has been applied, but before the index updaters
     *     are closed the index population thread comes in and reads the second batch. If the external updates queue
     *     triggers external updates to be applied then the value index update for the new node will be discarded
     *     since it's ahead of the scan point - at the same time the token index update that marks that node as used
     *     still sits in the "pending updates" buffer in the TokenIndexUpdater. Therefor the "refreshed"
     *     entity ID reader will not see the new node</li>
     *     <li>=> The new node will not end up in the index</li>
     * </ol>
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldApplyTokenIndexUpdatesBeforeValueIndexUpdates(boolean parallelApply) throws IOException {
        // given
        long[] indexIds = new long[] {1L, 2L};
        ArrayUtils.shuffle(indexIds, random.random());
        IndexDescriptor tokenIndex = IndexPrototype.forSchema(SchemaDescriptors.forAnyEntityTokens(EntityType.NODE))
                .withName("token")
                .materialise(indexIds[0]);
        IndexDescriptor valueIndex = IndexPrototype.forSchema(SchemaDescriptors.forLabel(1, 2))
                .withName("value")
                .materialise(indexIds[1]);
        Set<IndexEntryUpdate> observedUpdates = new HashSet<>();
        IndexUpdatesWorkSync workSync = new IndexUpdatesWorkSync(
                new IndexUpdateListener.Adapter() {
                    private boolean seenTokenIndexUpdates;
                    private boolean seenValueIndexUpdates;

                    @Override
                    public void applyUpdates(
                            Iterator<IndexEntryUpdate> updates, CursorContext cursorContext, boolean parallel) {
                        while (updates.hasNext()) {
                            var update = updates.next();
                            if (update instanceof TokenIndexEntryUpdate) {
                                assertThat(seenValueIndexUpdates).isFalse();
                                seenTokenIndexUpdates = true;
                            } else {
                                assertThat(seenTokenIndexUpdates).isTrue();
                                seenValueIndexUpdates = true;
                            }
                            observedUpdates.add(update);
                        }
                    }
                },
                parallelApply);

        // when
        List<IndexEntryUpdate> updates = new ArrayList<>();
        updates.add(EagerValueIndexEntryUpdate.add(0, valueIndex, Values.intValue(123)));
        updates.add(EagerValueIndexEntryUpdate.remove(1, valueIndex, Values.intValue(123)));
        updates.add(EagerValueIndexEntryUpdate.change(
                2, valueIndex, new Value[] {Values.intValue(123)}, new Value[] {Values.intValue(124)}));
        updates.add(TokenIndexEntryUpdate.tokenChange(3, tokenIndex, new int[] {1, 2}, new int[] {3, 4}));
        Collections.shuffle(updates, random.random());
        try (var batch = workSync.newBatch(CursorContext.NULL_CONTEXT)) {
            updates.forEach(batch::indexUpdate);
        }

        // then
        assertThat(observedUpdates).isEqualTo(new HashSet<>(updates));
    }

    private Set<UpdateAndContext> queueUpdatesInParallel(
            IndexDescriptor index, int threads, IndexUpdatesWorkSync workSync, CursorContextFactory contextFactory) {
        Set<UpdateAndContext> sentUpdates = ConcurrentHashMap.newKeySet();
        Race race = new Race();
        race.addContestants(
                threads,
                i -> throwing(() -> {
                    var cursorContext = contextFactory.create(Integer.toString(i));
                    try (IndexUpdatesWorkSync.Batch batch = workSync.newBatch(cursorContext)) {
                        EagerValueIndexEntryUpdate update = EagerValueIndexEntryUpdate.add(i, index, intValue(10 + i));
                        sentUpdates.add(new UpdateAndContext(update, cursorContext));
                        batch.indexUpdate(update);
                    }
                }),
                1);
        race.goUnchecked();
        return sentUpdates;
    }

    record UpdateAndContext(IndexEntryUpdate update, CursorContext context) {}
}
